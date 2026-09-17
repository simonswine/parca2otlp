package debuginfo

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	debuginfopb "buf.build/gen/go/parca-dev/parca/protocolbuffers/go/parca/debuginfo/v1alpha1"
)

func TestUploadLifecycle(t *testing.T) {
	store, err := New(t.TempDir(), 1024, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.Initiate("build/a", debuginfopb.BuildIDType_BUILD_ID_TYPE_GNU, debuginfopb.DebuginfoType_DEBUGINFO_TYPE_DEBUGINFO_UNSPECIFIED, "opaque-hash", 4, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Upload(session.ID, "build/a", session.Type, func(writer io.Writer) error { _, err := writer.Write([]byte("data")); return err }); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkFinished("build/a", session.ID, session.Type); err != nil {
		t.Fatal(err)
	}
	if !store.hasArtifact("build/a", session.Type) {
		t.Fatal("artifact was not published")
	}
	data, err := os.ReadFile(filepath.Join(store.root, "artifacts", "debuginfo", artifactKey("build/a", session.Type)+".data"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, []byte("data")) {
		t.Fatalf("artifact = %q, want data", data)
	}
	should, _, err := store.ShouldUpload("build/a", session.Type, false)
	if err != nil || should {
		t.Fatalf("ShouldUpload = %t, %v; want false, nil", should, err)
	}
}

func TestUploadRejectsMetadataAndSizeMismatch(t *testing.T) {
	store, err := New(t.TempDir(), 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.Initiate("build", debuginfopb.BuildIDType_BUILD_ID_TYPE_HASH, debuginfopb.DebuginfoType_DEBUGINFO_TYPE_EXECUTABLE, "hash", 2, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Upload(session.ID, "other", session.Type, func(writer io.Writer) error { return nil }); err == nil {
		t.Fatal("Upload accepted mismatched build ID")
	}
	if _, err := store.Upload(session.ID, "build", session.Type, func(writer io.Writer) error { _, err := writer.Write([]byte("too large")); return err }); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("Upload error = %v, want ErrTooLarge", err)
	}
}

func TestFailedUploadCanBeRetriedImmediately(t *testing.T) {
	store, err := New(t.TempDir(), 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.Initiate("build", debuginfopb.BuildIDType_BUILD_ID_TYPE_GNU, debuginfopb.DebuginfoType_DEBUGINFO_TYPE_DEBUGINFO_UNSPECIFIED, "hash", 4, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Upload(session.ID, "build", session.Type, func(io.Writer) error { return context.DeadlineExceeded }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Upload error = %v, want context deadline exceeded", err)
	}
	should, _, err := store.ShouldUpload("build", session.Type, false)
	if err != nil || !should {
		t.Fatalf("ShouldUpload = %t, %v; want true, nil", should, err)
	}
}

func TestRetentionEvictsOldestArtifact(t *testing.T) {
	store, err := New(t.TempDir(), 4, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	first := upload(t, store, "first", "one1")
	second := upload(t, store, "second", "two2")
	if store.hasArtifact("first", first.Type) {
		t.Fatal("oldest artifact was not evicted")
	}
	if !store.hasArtifact("second", second.Type) {
		t.Fatal("new artifact was not retained")
	}
}

func upload(t *testing.T, store *Store, buildID, data string) session {
	t.Helper()
	session, err := store.Initiate(buildID, debuginfopb.BuildIDType_BUILD_ID_TYPE_GNU, debuginfopb.DebuginfoType_DEBUGINFO_TYPE_DEBUGINFO_UNSPECIFIED, "hash-"+buildID, int64(len(data)), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Upload(session.ID, buildID, session.Type, func(writer io.Writer) error { _, err := writer.Write([]byte(data)); return err }); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkFinished(buildID, session.ID, session.Type); err != nil {
		t.Fatal(err)
	}
	return session
}
