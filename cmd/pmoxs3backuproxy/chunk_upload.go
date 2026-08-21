package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"sync"

	"github.com/minio/minio-go/v7"
)

type chunkObjectStore interface {
	StatObject(context.Context, string, string, minio.StatObjectOptions) (minio.ObjectInfo, error)
	PutObject(context.Context, string, string, io.Reader, int64, minio.PutObjectOptions) (minio.UploadInfo, error)
}

type chunkRequest struct {
	Digest      string
	EncodedSize int64
	Size        uint64
	WriterID    int32
	ObjectName  string
}

func chunkObjectName(digest string) (string, error) {
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != sha256.Size {
		return "", fmt.Errorf("invalid SHA-256 chunk digest %q", digest)
	}
	return fmt.Sprintf("chunks/%s/%s/%s", digest[0:2], digest[2:4], digest[4:]), nil
}

func parseChunkRequest(r *http.Request) (chunkRequest, error) {
	q := r.URL.Query()
	digest := q.Get("digest")
	objectName, err := chunkObjectName(digest)
	if err != nil {
		return chunkRequest{}, err
	}

	encodedSize, err := strconv.ParseInt(q.Get("encoded-size"), 10, 64)
	if err != nil || encodedSize <= 0 {
		return chunkRequest{}, fmt.Errorf("invalid encoded-size %q", q.Get("encoded-size"))
	}
	if r.ContentLength >= 0 && r.ContentLength != encodedSize {
		return chunkRequest{}, fmt.Errorf(
			"encoded-size %d does not match Content-Length %d",
			encodedSize,
			r.ContentLength,
		)
	}

	size, err := strconv.ParseUint(q.Get("size"), 10, 64)
	if err != nil || size == 0 || size > math.MaxInt64 {
		return chunkRequest{}, fmt.Errorf("invalid chunk size %q", q.Get("size"))
	}
	wid, err := strconv.ParseInt(q.Get("wid"), 10, 32)
	if err != nil || wid < 0 {
		return chunkRequest{}, fmt.Errorf("invalid writer id %q", q.Get("wid"))
	}

	return chunkRequest{
		Digest:      digest,
		EncodedSize: encodedSize,
		Size:        size,
		WriterID:    int32(wid),
		ObjectName:  objectName,
	}, nil
}

type chunkFlight struct {
	done chan struct{}
	err  error
}

// chunkFlightGroup coalesces concurrent writes of the same content-addressed
// chunk. Unlike a mutex, followers drain their HTTP/2 request bodies before
// waiting for the leader. This is essential: waiting with an unread body can
// exhaust the connection-level receive window and deadlock every stream.
type chunkFlightGroup struct {
	mu    sync.Mutex
	calls map[string]*chunkFlight
}

func (g *chunkFlightGroup) Do(
	ctx context.Context,
	key string,
	leader func() (bool, error),
	drainFollower func() error,
) (known bool, err error) {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[string]*chunkFlight)
	}
	if call, ok := g.calls[key]; ok {
		g.mu.Unlock()
		if err := drainFollower(); err != nil {
			return false, fmt.Errorf("drain duplicate chunk body: %w", err)
		}
		select {
		case <-call.done:
			if call.err != nil {
				return false, call.err
			}
			// Whether the leader uploaded or reused the object, it is known by
			// the time a duplicate request receives its response.
			return true, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}

	call := &chunkFlight{done: make(chan struct{})}
	g.calls[key] = call
	g.mu.Unlock()

	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("chunk upload leader panicked: %v", recovered)
			g.finish(key, call, false, err)
			panic(recovered)
		}
		g.finish(key, call, known, err)
	}()

	return leader()
}

func (g *chunkFlightGroup) finish(key string, call *chunkFlight, _ bool, err error) {
	g.mu.Lock()
	call.err = err
	close(call.done)
	if g.calls[key] == call {
		delete(g.calls, key)
	}
	g.mu.Unlock()
}

func drainChunkBody(body io.Reader, expected int64) error {
	n, err := io.Copy(io.Discard, body)
	if err != nil {
		return err
	}
	if n != expected {
		return fmt.Errorf("read %d encoded bytes, expected %d", n, expected)
	}
	return nil
}

func isObjectNotFound(err error) bool {
	resp := minio.ToErrorResponse(err)
	return resp.StatusCode == http.StatusNotFound ||
		resp.Code == "NoSuchKey" ||
		resp.Code == "NoSuchObject" ||
		resp.Code == "NotFound"
}

type countingReader struct {
	r io.Reader
	n int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

func storeChunk(
	ctx context.Context,
	store chunkObjectStore,
	bucket string,
	request chunkRequest,
	body io.Reader,
	storageClass string,
) (bool, error) {
	_, err := store.StatObject(ctx, bucket, request.ObjectName, minio.StatObjectOptions{})
	if err == nil {
		if err := drainChunkBody(body, request.EncodedSize); err != nil {
			return false, fmt.Errorf("drain known chunk %s: %w", request.Digest, err)
		}
		return true, nil
	}
	if !isObjectNotFound(err) {
		return false, fmt.Errorf("stat chunk %s: %w", request.Digest, err)
	}

	counted := &countingReader{r: body}
	if _, err := store.PutObject(
		ctx,
		bucket,
		request.ObjectName,
		counted,
		request.EncodedSize,
		putOptions(storageClass, nil),
	); err != nil {
		return false, fmt.Errorf("put chunk %s: %w", request.Digest, err)
	}
	if counted.n != request.EncodedSize {
		return false, fmt.Errorf(
			"put chunk %s consumed %d encoded bytes, expected %d",
			request.Digest,
			counted.n,
			request.EncodedSize,
		)
	}
	if trailing, err := io.Copy(io.Discard, body); err != nil {
		return false, fmt.Errorf("drain chunk %s after upload: %w", request.Digest, err)
	} else if trailing != 0 {
		return false, fmt.Errorf(
			"chunk %s contains %d trailing encoded bytes",
			request.Digest,
			trailing,
		)
	}
	return false, nil
}
