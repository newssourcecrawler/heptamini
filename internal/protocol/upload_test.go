package protocol

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"testing"
)

func shaHex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func b64(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

func TestUploadManifestValidation(t *testing.T) {
	u := NewUploadState("J_1")

	// nil manifest
	if err := u.ApplyManifest(nil); err == nil {
		t.Fatalf("expected error for nil manifest")
	}

	// missing payload_id
	err := u.ApplyManifest(&PayloadManifest{
		PayloadID:       "",
		ChunkBytes:      8,
		Chunks:          1,
		ContentEncoding: "identity",
		Cipher:          "none",
	})
	if err == nil {
		t.Fatalf("expected error for missing payload_id")
	}

	// chunk_bytes=0
	err = u.ApplyManifest(&PayloadManifest{
		PayloadID:       "P_1",
		ChunkBytes:      0,
		Chunks:          1,
		ContentEncoding: "identity",
		Cipher:          "none",
	})
	if err == nil {
		t.Fatalf("expected error for chunk_bytes=0")
	}

	// chunks=0
	err = u.ApplyManifest(&PayloadManifest{
		PayloadID:       "P_1",
		ChunkBytes:      8,
		Chunks:          0,
		ContentEncoding: "identity",
		Cipher:          "none",
	})
	if err == nil {
		t.Fatalf("expected error for chunks=0")
	}

	// ok
	err = u.ApplyManifest(&PayloadManifest{
		PayloadID:        "P_1",
		ChunkBytes:       8,
		Chunks:           1,
		ContentEncoding:  "identity",
		Cipher:           "none",
		Sha256HexEncoded: "",
	})
	if err != nil {
		t.Fatalf("expected ok manifest, got %v", err)
	}
	if u.Manifest == nil || u.Manifest.PayloadID != "P_1" {
		t.Fatalf("manifest not stored")
	}
	if u.NextIndex != 0 || u.BytesSeen != 0 || len(u.Buf) != 0 {
		t.Fatalf("state not reset on ApplyManifest")
	}
}

func TestUploadChunkNeedsManifestFirst(t *testing.T) {
	u := NewUploadState("J_1")

	need, err := u.ApplyChunk(&PayloadChunk{
		PayloadID: "P_1",
		Index:     0,
		Offset:    0,
		Sha256Hex: shaHex([]byte("x")),
		BytesB64:  b64([]byte("x")),
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if need == nil || need.Code != "manifest_missing" {
		t.Fatalf("expected manifest_missing need, got %#v", need)
	}
}

func TestUploadChunkOrderEnforced(t *testing.T) {
	u := NewUploadState("J_1")
	if err := u.ApplyManifest(&PayloadManifest{
		PayloadID:       "P_1",
		ChunkBytes:      8,
		Chunks:          2,
		ContentEncoding: "identity",
		Cipher:          "none",
	}); err != nil {
		t.Fatalf("ApplyManifest: %v", err)
	}

	// send index 1 first -> need expected_index=0
	b := []byte("a")
	need, err := u.ApplyChunk(&PayloadChunk{
		PayloadID: "P_1",
		Index:     1,
		Offset:    0,
		Sha256Hex: shaHex(b),
		BytesB64:  b64(b),
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if need == nil || need.Code != "chunk_expected_index" || need.Want != 0 {
		t.Fatalf("expected chunk_expected_index want=0, got %#v", need)
	}
}

func TestUploadChunkHashMismatch(t *testing.T) {
	u := NewUploadState("J_1")
	if err := u.ApplyManifest(&PayloadManifest{
		PayloadID:       "P_1",
		ChunkBytes:      8,
		Chunks:          1,
		ContentEncoding: "identity",
		Cipher:          "none",
	}); err != nil {
		t.Fatalf("ApplyManifest: %v", err)
	}

	b := []byte("hello")
	need, err := u.ApplyChunk(&PayloadChunk{
		PayloadID: "P_1",
		Index:     0,
		Offset:    0,
		// wrong hash
		Sha256Hex: shaHex([]byte("nope")),
		BytesB64:  b64(b),
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if need == nil || need.Code != "chunk_hash_mismatch" {
		t.Fatalf("expected chunk_hash_mismatch, got %#v", need)
	}
}

func TestUploadPayloadHashMismatch(t *testing.T) {
	u := NewUploadState("J_1")

	// Two chunks, but declare a wrong overall sha256.
	if err := u.ApplyManifest(&PayloadManifest{
		PayloadID:        "P_1",
		ChunkBytes:       3,
		Chunks:           2,
		ContentEncoding:  "identity",
		Cipher:           "none",
		Sha256HexEncoded: shaHex([]byte("WRONG")),
	}); err != nil {
		t.Fatalf("ApplyManifest: %v", err)
	}

	b0 := []byte("abc")
	need, err := u.ApplyChunk(&PayloadChunk{
		PayloadID: "P_1",
		Index:     0,
		Offset:    0,
		Sha256Hex: shaHex(b0),
		BytesB64:  b64(b0),
	})
	if err != nil || need != nil {
		t.Fatalf("expected first chunk ok, got need=%#v err=%v", need, err)
	}

	b1 := []byte("def")
	need, err = u.ApplyChunk(&PayloadChunk{
		PayloadID: "P_1",
		Index:     1,
		Offset:    3,
		Sha256Hex: shaHex(b1),
		BytesB64:  b64(b1),
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if need == nil || need.Code != "payload_hash_mismatch" {
		t.Fatalf("expected payload_hash_mismatch, got %#v", need)
	}
}

func TestUploadHappyPathComplete(t *testing.T) {
	u := NewUploadState("J_1")

	// Payload "hi" in one chunk with matching overall hash.
	payload := []byte("hi")
	if err := u.ApplyManifest(&PayloadManifest{
		PayloadID:        "P_1",
		ChunkBytes:       8,
		Chunks:           1,
		ContentEncoding:  "identity",
		Cipher:           "none",
		Sha256HexEncoded: shaHex(payload),
	}); err != nil {
		t.Fatalf("ApplyManifest: %v", err)
	}

	need, err := u.ApplyChunk(&PayloadChunk{
		PayloadID: "P_1",
		Index:     0,
		Offset:    0,
		Sha256Hex: shaHex(payload),
		BytesB64:  b64(payload),
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if need != nil {
		t.Fatalf("expected no need, got %#v", need)
	}
	if !u.IsComplete() {
		t.Fatalf("expected complete")
	}
	if string(u.Buf) != "hi" {
		t.Fatalf("expected buf 'hi', got %q", string(u.Buf))
	}
}
