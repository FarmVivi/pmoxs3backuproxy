package s3pmoxcommon

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"
)

var (
	uncompressedBlobMagic = [8]byte{66, 171, 56, 7, 190, 131, 112, 161}
	compressedBlobMagic   = [8]byte{49, 185, 88, 66, 111, 182, 163, 127}
)

const maxManifestSize = 128 * 1024 * 1024

type backupManifest struct {
	Files []manifestFile `json:"files"`
}

type manifestFile struct {
	Filename  string `json:"filename"`
	CryptMode string `json:"crypt-mode"`
}

// decodeManifestBlob implements the two unencrypted Proxmox DataBlob formats
// used for index.json.blob. The manifest itself stays readable by the server;
// its file entries describe whether the referenced archives/chunks are plain,
// signed, or client-side encrypted.
func decodeManifestBlob(raw []byte) ([]byte, error) {
	if len(raw) < 12 {
		return nil, fmt.Errorf("manifest blob too small: %d bytes", len(raw))
	}
	if got, want := crc32.ChecksumIEEE(raw[12:]), binary.LittleEndian.Uint32(raw[8:12]); got != want {
		return nil, fmt.Errorf("manifest blob CRC mismatch: got %08x, want %08x", got, want)
	}

	magic := [8]byte(raw[:8])
	switch magic {
	case uncompressedBlobMagic:
		return append([]byte(nil), raw[12:]...), nil
	case compressedBlobMagic:
		decoder, err := zstd.NewReader(bytes.NewReader(raw[12:]), zstd.WithDecoderMaxMemory(maxManifestSize))
		if err != nil {
			return nil, fmt.Errorf("open compressed manifest: %w", err)
		}
		defer decoder.Close()
		decoded, err := io.ReadAll(io.LimitReader(decoder, maxManifestSize+1))
		if err != nil {
			return nil, fmt.Errorf("decompress manifest: %w", err)
		}
		if len(decoded) > maxManifestSize {
			return nil, fmt.Errorf("decoded manifest exceeds %d bytes", maxManifestSize)
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("unsupported manifest blob magic %x", magic)
	}
}

func applyManifestCryptModes(snapshot *Snapshot, raw []byte) error {
	modes, err := ParseManifestCryptModes(raw)
	if err != nil {
		return err
	}
	ApplySnapshotCryptModes(snapshot, modes)
	return nil
}

// ParseManifestCryptModes validates and decodes a PBS manifest into the
// archive encryption modes advertised by the backup client.
func ParseManifestCryptModes(raw []byte) (map[string]string, error) {
	data, err := decodeManifestBlob(raw)
	if err != nil {
		return nil, err
	}
	var manifest backupManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("decode manifest JSON: %w", err)
	}
	modes := make(map[string]string, len(manifest.Files))
	for _, file := range manifest.Files {
		mode := file.CryptMode
		if mode == "" { // manifests predating PBS 0.8 implied no encryption
			mode = "none"
		}
		switch mode {
		case "none", "sign-only", "encrypt":
		default:
			return nil, fmt.Errorf("file %q has unsupported crypt mode %q", file.Filename, mode)
		}
		modes[file.Filename] = mode
	}
	return modes, nil
}

// ApplySnapshotCryptModes decorates a snapshot built from the object listing.
func ApplySnapshotCryptModes(snapshot *Snapshot, modes map[string]string) {
	for i := range snapshot.Files {
		if mode, ok := modes[snapshot.Files[i].Filename]; ok {
			snapshot.Files[i].CryptMode = mode
		}
	}
}

// ReadSnapshotCryptModes fetches and decodes one immutable manifest. Callers
// listing snapshots repeatedly should cache the returned map by object key.
func ReadSnapshotCryptModes(ctx context.Context, c *minio.Client, snapshot Snapshot) (map[string]string, error) {
	key := snapshot.S3Prefix() + "/index.json.blob"
	obj, err := c.GetObject(ctx, snapshot.Datastore, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	raw, readErr := io.ReadAll(io.LimitReader(obj, maxManifestSize+13))
	closeErr := obj.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read %s: %w", key, readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close %s: %w", key, closeErr)
	}
	if len(raw) > maxManifestSize+12 {
		return nil, fmt.Errorf("%s exceeds the maximum manifest size", key)
	}
	modes, err := ParseManifestCryptModes(raw)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", key, err)
	}
	return modes, nil
}
