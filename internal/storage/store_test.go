package storage

import (
	"context"
	"os"
	"testing"

	collectorprofiles "go.opentelemetry.io/proto/otlp/collector/profiles/v1development"
	"google.golang.org/protobuf/proto"
)

func TestSaveWritesReadableAtomicFile(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path, err := store.Save(context.Background(), &collectorprofiles.ExportProfilesServiceRequest{})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var request collectorprofiles.ExportProfilesServiceRequest
	if err := proto.Unmarshal(data, &request); err != nil {
		t.Fatal(err)
	}
}
