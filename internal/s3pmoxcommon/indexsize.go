package s3pmoxcommon

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/minio/minio-go/v7"
)

// Layout of a Proxmox index file, as written by this proxy and documented in
// the Proxmox Backup Server documentation:
//
//	fixed index (.fidx), header of 4096 bytes
//	  0:8    magic
//	  8:24   uuid
//	  24:32  ctime
//	  32:64  index checksum
//	  64:72  size of the archive, little endian
//	  72:80  chunk size, little endian
//	  4096:  one 32 bytes digest per chunk
//
//	dynamic index (.didx), header of 4096 bytes
//	  0:8    magic
//	  8:24   uuid
//	  24:32  ctime
//	  32:64  index checksum
//	  4096:  entries of 40 bytes, an end offset (little endian uint64)
//	         followed by a 32 bytes digest
//
// A fixed index therefore states the archive size in its header, and a dynamic
// index states it in the end offset of its last entry. Both are readable with
// a range request of a few dozen bytes, which is what makes it possible to
// report backup sizes without ever downloading an index, let alone a chunk.
const (
	IndexHeaderSize     = 4096
	fixedIndexSizeAt    = 64
	dynamicIndexEntry   = 40
	fixedIndexHeaderMin = 80
)

var ErrNotAnIndex = errors.New("object is not a fixed or dynamic index")

// IsIndex reports whether an object name is a backup index.
func IsIndex(name string) bool {
	return strings.HasSuffix(name, ".fidx") || strings.HasSuffix(name, ".didx")
}

// ArchiveSize returns the logical size of the archive described by an index,
// which is the size of the guest disk or of the pxar stream, not the space the
// backup occupies once deduplicated.
//
// objectSize is the size of the index object itself, as already reported by a
// bucket listing; passing it avoids a StatObject round trip.
//
// The cost of this call is one ranged GET of at most 80 bytes.
func ArchiveSize(ctx context.Context, c *minio.Client, bucket string, key string, objectSize int64) (uint64, error) {
	switch {
	case strings.HasSuffix(key, ".fidx"):
		return fixedArchiveSize(ctx, c, bucket, key)
	case strings.HasSuffix(key, ".didx"):
		return dynamicArchiveSize(ctx, c, bucket, key, objectSize)
	default:
		return 0, ErrNotAnIndex
	}
}

func fixedArchiveSize(ctx context.Context, c *minio.Client, bucket string, key string) (uint64, error) {
	head, err := rangeRead(ctx, c, bucket, key, 0, fixedIndexHeaderMin-1)
	if err != nil {
		return 0, err
	}
	if len(head) < fixedIndexHeaderMin {
		return 0, fmt.Errorf("%s: index header is %d bytes, expected at least %d", key, len(head), fixedIndexHeaderMin)
	}
	if !bytes.Equal(head[0:8], PROXMOX_INDEX_MAGIC_FIXED[:]) {
		return 0, fmt.Errorf("%s: wrong magic for a fixed index", key)
	}
	return binary.LittleEndian.Uint64(head[fixedIndexSizeAt : fixedIndexSizeAt+8]), nil
}

func dynamicArchiveSize(ctx context.Context, c *minio.Client, bucket string, key string, objectSize int64) (uint64, error) {
	// A dynamic index does not carry the archive size in its header: the end
	// offset of the last chunk is the size, so the tail is what has to be read.
	if objectSize <= IndexHeaderSize {
		// Header only, no chunk: an empty archive.
		return 0, nil
	}
	payload := objectSize - IndexHeaderSize
	if payload%dynamicIndexEntry != 0 {
		return 0, fmt.Errorf(
			"%s: %d bytes after the header is not a multiple of the %d bytes entry size",
			key, payload, dynamicIndexEntry,
		)
	}
	tail, err := rangeRead(ctx, c, bucket, key, objectSize-dynamicIndexEntry, objectSize-1)
	if err != nil {
		return 0, err
	}
	if len(tail) < 8 {
		return 0, fmt.Errorf("%s: short read on the last index entry", key)
	}
	return binary.LittleEndian.Uint64(tail[0:8]), nil
}

func rangeRead(ctx context.Context, c *minio.Client, bucket string, key string, start int64, end int64) ([]byte, error) {
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(start, end); err != nil {
		return nil, err
	}
	obj, err := c.GetObject(ctx, bucket, key, opts)
	if err != nil {
		return nil, err
	}
	defer obj.Close()

	buf := make([]byte, end-start+1)
	n, err := io.ReadFull(obj, buf)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	return buf[:n], nil
}
