package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	collectorprofiles "go.opentelemetry.io/proto/otlp/collector/profiles/v1development"
	"google.golang.org/protobuf/proto"
)

var ErrBatchTooLarge = errors.New("profile batch exceeds storage limit")

// Store persists complete OTLP export requests within a bounded directory.
type Store struct {
	dir     string
	maxSize int64

	mu    sync.Mutex
	files []storedFile
	size  int64
}

type storedFile struct {
	path     string
	size     int64
	modified time.Time
}

// New loads existing profile files and removes the oldest files needed to meet
// maxSize. A zero maxSize disables retention.
func New(dir string, maxSize int64) (*Store, error) {
	if dir == "" {
		return nil, fmt.Errorf("storage directory is required")
	}
	if maxSize < 0 {
		return nil, fmt.Errorf("storage limit cannot be negative")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create storage directory: %w", err)
	}
	store := &Store{dir: dir, maxSize: maxSize}
	if err := store.load(); err != nil {
		return nil, err
	}
	if err := store.prune(0); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *Store) Save(ctx context.Context, request *collectorprofiles.ExportProfilesServiceRequest) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	data, err := proto.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("marshal OTLP profiles: %w", err)
	}
	if s.maxSize > 0 && int64(len(data)) > s.maxSize {
		return "", fmt.Errorf("%w: batch is %d bytes, limit is %d bytes", ErrBatchTooLarge, len(data), s.maxSize)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.prune(int64(len(data))); err != nil {
		return "", err
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
	if err := os.Rename(temporaryPath, finalPath); err != nil {
		return "", fmt.Errorf("publish profile file: %w", err)
	}
	info, err := os.Stat(finalPath)
	if err != nil {
		return "", fmt.Errorf("stat published profile file: %w", err)
	}
	s.files = append(s.files, storedFile{path: finalPath, size: info.Size(), modified: info.ModTime()})
	s.size += info.Size()
	s.sortFiles()
	return finalPath, nil
}

func (s *Store) load() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("read storage directory: %w", err)
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || strings.HasPrefix(entry.Name(), ".tmp-") || !strings.HasSuffix(entry.Name(), ".otlp.pb") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("stat stored profile %q: %w", entry.Name(), err)
		}
		file := storedFile{path: filepath.Join(s.dir, entry.Name()), size: info.Size(), modified: info.ModTime()}
		s.files = append(s.files, file)
		s.size += file.size
	}
	s.sortFiles()
	return nil
}

func (s *Store) sortFiles() {
	sort.Slice(s.files, func(i, j int) bool {
		if s.files[i].modified.Equal(s.files[j].modified) {
			return s.files[i].path < s.files[j].path
		}
		return s.files[i].modified.Before(s.files[j].modified)
	})
}

func (s *Store) prune(incomingSize int64) error {
	if s.maxSize == 0 {
		return nil
	}
	for s.size+incomingSize > s.maxSize && len(s.files) > 0 {
		oldest := s.files[0]
		if err := os.Remove(oldest.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove expired profile file %q: %w", oldest.path, err)
		}
		s.files = s.files[1:]
		s.size -= oldest.size
	}
	if s.size+incomingSize > s.maxSize {
		return fmt.Errorf("storage limit is too small for incoming profile batch")
	}
	return nil
}
