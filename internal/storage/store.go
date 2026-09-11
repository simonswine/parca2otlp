package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	collectorprofiles "go.opentelemetry.io/proto/otlp/collector/profiles/v1development"
	"google.golang.org/protobuf/proto"
)

// Store persists complete OTLP export requests.
type Store struct {
	dir string
}

func New(dir string) (*Store, error) {
	if dir == "" {
		return nil, fmt.Errorf("storage directory is required")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create storage directory: %w", err)
	}
	return &Store{dir: dir}, nil
}

func (s *Store) Save(ctx context.Context, request *collectorprofiles.ExportProfilesServiceRequest) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	data, err := proto.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("marshal OTLP profiles: %w", err)
	}

	name := time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + uuid.NewString() + ".otlp.pb"
	finalPath := filepath.Join(s.dir, name)
	temporary, err := os.CreateTemp(s.dir, ".tmp-*.otlp.pb")
	if err != nil {
		return "", fmt.Errorf("create temporary profile file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)

	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return "", fmt.Errorf("write profile file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return "", fmt.Errorf("sync profile file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return "", fmt.Errorf("close profile file: %w", err)
	}
	finalPath = filepath.Join(s.dir, name)
	if err := os.Rename(temporaryPath, finalPath); err != nil {
		return "", fmt.Errorf("publish profile file: %w", err)
	}
	return finalPath, nil
}
