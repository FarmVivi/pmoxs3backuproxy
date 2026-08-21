package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
)

func TestChunkObjectName(t *testing.T) {
	digest := strings.Repeat("ab", 32)
	name, err := chunkObjectName(digest)
	if err != nil {
		t.Fatal(err)
	}
	if want := "chunks/ab/ab/" + digest[4:]; name != want {
		t.Fatalf("got %q, want %q", name, want)
	}
	for _, invalid := range []string{"", "abcd", strings.Repeat("z", 64), strings.Repeat("ab", 33)} {
		if _, err := chunkObjectName(invalid); err == nil {
			t.Fatalf("invalid digest %q accepted", invalid)
		}
	}
}

func TestRestoreChunkRejectsMalformedDigestWithoutPanic(t *testing.T) {
	datastore := "test"
	s := &Server{H2Ticket: &TicketEntry{Client: &minio.Client{}}, SelectedDataStore: &datastore}
	req := httptest.NewRequest(http.MethodGet, "/chunk?digest=x", nil)
	recorder := httptest.NewRecorder()
	s.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("got HTTP %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}
