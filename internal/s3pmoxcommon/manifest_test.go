package s3pmoxcommon

import (
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func manifestBlob(t *testing.T, data []byte, compressed bool) []byte {
	t.Helper()
	payload := data
	magic := uncompressedBlobMagic
	if compressed {
		encoder, err := zstd.NewWriter(nil)
		if err != nil {
			t.Fatal(err)
		}
		payload = encoder.EncodeAll(data, nil)
		encoder.Close()
		magic = compressedBlobMagic
	}
	raw := make([]byte, 12, 12+len(payload))
	copy(raw, magic[:])
	raw = append(raw, payload...)
	binary.LittleEndian.PutUint32(raw[8:12], crc32.ChecksumIEEE(payload))
	return raw
}

func TestApplyManifestCryptModes(t *testing.T) {
	manifest := []byte(`{"files":[{"filename":"disk.img.fidx","crypt-mode":"encrypt"},{"filename":"qemu-server.conf.blob","crypt-mode":"sign-only"}]}`)
	for _, compressed := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "compressed"}[compressed], func(t *testing.T) {
			snapshot := Snapshot{Files: []SnapshotFile{
				{Filename: "disk.img.fidx", CryptMode: "none"},
				{Filename: "qemu-server.conf.blob", CryptMode: "none"},
				{Filename: "client.log.blob", CryptMode: "none"},
			}}
			if err := applyManifestCryptModes(&snapshot, manifestBlob(t, manifest, compressed)); err != nil {
				t.Fatal(err)
			}
			if snapshot.Files[0].CryptMode != "encrypt" || snapshot.Files[1].CryptMode != "sign-only" {
				t.Fatalf("manifest modes not applied: %+v", snapshot.Files)
			}
			if snapshot.Files[2].CryptMode != "none" {
				t.Fatalf("unlisted auxiliary file changed: %+v", snapshot.Files[2])
			}
		})
	}
}

func TestDecodeManifestBlobRejectsCorruption(t *testing.T) {
	raw := manifestBlob(t, []byte(`{"files":[]}`), false)
	raw[len(raw)-1] ^= 1
	if _, err := decodeManifestBlob(raw); err == nil {
		t.Fatal("corrupt CRC accepted")
	}
}

func TestApplyManifestCryptModesRejectsUnknownMode(t *testing.T) {
	raw := manifestBlob(t, []byte(`{"files":[{"filename":"disk.img.fidx","crypt-mode":"mystery"}]}`), false)
	if err := applyManifestCryptModes(&Snapshot{}, raw); err == nil {
		t.Fatal("unknown crypt mode accepted")
	}
}
