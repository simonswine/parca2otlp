package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	collectorprofiles "go.opentelemetry.io/proto/otlp/collector/profiles/v1development"
	profiles "go.opentelemetry.io/proto/otlp/profiles/v1development"
	"google.golang.org/protobuf/proto"
)

func TestSaveWritesReadableAtomicFile(t *testing.T) {
	store, err := New(t.TempDir(), 0)
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

func TestNewPrunesOldestManagedFiles(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "old.otlp.pb", 10, time.Now().Add(-2*time.Hour))
	writeFile(t, dir, "new.otlp.pb", 10, time.Now().Add(-time.Hour))
	writeFile(t, dir, "notes.txt", 100, time.Now())
	writeFile(t, dir, ".tmp-interrupted.otlp.pb", 100, time.Now())
	if _, err := New(dir, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "old.otlp.pb")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old file still exists: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "new.otlp.pb")); err != nil {
		t.Fatalf("new file was removed: %v", err)
	}
	for _, name := range []string{"notes.txt", ".tmp-interrupted.otlp.pb"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("ignored file %q was removed: %v", name, err)
		}
	}
}

func TestSaveEvictsOldestFile(t *testing.T) {
	dir := t.TempDir()
	request := profileRequest(1)
	unbounded, err := New(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	first, err := unbounded.Save(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(first)
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(dir, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Save(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(first); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old file still exists: %v", err)
	}
	if _, err := os.Stat(second); err != nil {
		t.Fatalf("new file was not persisted: %v", err)
	}
}

func TestSaveRejectsOversizedBatchWithoutEviction(t *testing.T) {
	dir := t.TempDir()
	unbounded, err := New(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	path, err := unbounded.Save(context.Background(), profileRequest(1))
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(dir, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Save(context.Background(), profileRequest(1024)); !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("Save error = %v, want ErrBatchTooLarge", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("existing file was removed: %v", err)
	}
}

func TestParseSize(t *testing.T) {
	for _, test := range []struct {
		input string
		want  int64
	}{{"0", 0}, {"256MiB", 256 << 20}, {"1GiB", 1 << 30}, {"1024", 1024}} {
		got, err := ParseSize(test.input)
		if err != nil || got != test.want {
			t.Fatalf("ParseSize(%q) = %d, %v; want %d, nil", test.input, got, err, test.want)
		}
	}
	if _, err := ParseSize("256MB"); err == nil {
		t.Fatal("ParseSize accepted unsupported unit")
	}
}

func writeFile(t *testing.T, dir, name string, size int, modified time.Time) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, make([]byte, size), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, modified, modified); err != nil {
		t.Fatal(err)
	}
}

func profileRequest(size int) *collectorprofiles.ExportProfilesServiceRequest {
	return &collectorprofiles.ExportProfilesServiceRequest{ResourceProfiles: []*profiles.ResourceProfiles{{ScopeProfiles: []*profiles.ScopeProfiles{{Profiles: []*profiles.Profile{{OriginalPayloadFormat: "pprof", OriginalPayload: make([]byte, size)}}}}}}}
}
