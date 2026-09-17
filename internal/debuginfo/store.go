package debuginfo

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	debuginfopb "buf.build/gen/go/parca-dev/parca/protocolbuffers/go/parca/debuginfo/v1alpha1"
	"github.com/google/uuid"
)

var (
	ErrNotFound      = errors.New("debug upload not found")
	ErrTooLarge      = errors.New("debug artifact exceeds storage limit")
	ErrSizeMismatch  = errors.New("debug upload size does not match initiation")
	ErrNotUploaded   = errors.New("debug upload has not completed")
	ErrAlreadyExists = errors.New("debug artifact already exists")
)

const uploadInProgressReason = "A previous upload is still in-progress and not stale yet (only stale uploads can be retried)."

type Store struct {
	root       string
	maxSize    int64
	staleAfter time.Duration

	mu       sync.Mutex
	sessions map[string]session
	files    []artifact
	size     int64
}

type session struct {
	ID          string                    `json:"id"`
	BuildID     string                    `json:"build_id"`
	BuildIDType debuginfopb.BuildIDType   `json:"build_id_type"`
	Type        debuginfopb.DebuginfoType `json:"type"`
	Hash        string                    `json:"hash"`
	Size        int64                     `json:"size"`
	CreatedAt   time.Time                 `json:"created_at"`
	Uploaded    bool                      `json:"uploaded"`
}

type artifact struct {
	path     string
	size     int64
	modified time.Time
}

func New(root string, maxSize int64, staleAfter time.Duration) (*Store, error) {
	if root == "" {
		return nil, fmt.Errorf("debug storage directory is required")
	}
	if maxSize < 0 {
		return nil, fmt.Errorf("debug storage limit cannot be negative")
	}
	if staleAfter <= 0 {
		return nil, fmt.Errorf("debug upload stale period must be positive")
	}
	for _, dir := range []string{root, filepath.Join(root, "artifacts"), filepath.Join(root, "uploads")} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("create debug storage directory: %w", err)
		}
	}
	store := &Store{root: root, maxSize: maxSize, staleAfter: staleAfter, sessions: map[string]session{}}
	if err := store.load(); err != nil {
		return nil, err
	}
	if err := store.prune(0); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *Store) ShouldUpload(buildID string, typ debuginfopb.DebuginfoType, force bool) (bool, string, error) {
	if err := validate(buildID, typ); err != nil {
		return false, "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hasArtifact(buildID, typ) && !force {
		return false, "debug artifact already exists", nil
	}
	if active, ok := s.activeSession(buildID, typ); ok && !force {
		if time.Since(active.CreatedAt) < s.staleAfter {
			return false, uploadInProgressReason, nil
		}
		if err := s.removeSession(active); err != nil {
			return false, "", err
		}
	}
	return true, "debug artifact upload accepted", nil
}

func (s *Store) Initiate(buildID string, buildIDType debuginfopb.BuildIDType, typ debuginfopb.DebuginfoType, hash string, size int64, force bool) (session, error) {
	if err := validate(buildID, typ); err != nil {
		return session{}, err
	}
	if !validBuildIDType(buildIDType) {
		return session{}, fmt.Errorf("unsupported build ID type %q", buildIDType)
	}
	if hash == "" {
		return session{}, fmt.Errorf("debug artifact hash is required")
	}
	if size <= 0 {
		return session{}, fmt.Errorf("debug artifact size must be positive")
	}
	if s.maxSize > 0 && size > s.maxSize {
		return session{}, fmt.Errorf("%w: artifact is %d bytes, limit is %d bytes", ErrTooLarge, size, s.maxSize)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hasArtifact(buildID, typ) && !force {
		return session{}, ErrAlreadyExists
	}
	if active, ok := s.activeSession(buildID, typ); ok {
		if !force && time.Since(active.CreatedAt) < s.staleAfter {
			return session{}, fmt.Errorf("debug upload is in progress")
		}
		if err := s.removeSession(active); err != nil {
			return session{}, err
		}
	}
	result := session{ID: uuid.NewString(), BuildID: buildID, BuildIDType: buildIDType, Type: typ, Hash: hash, Size: size, CreatedAt: time.Now().UTC()}
	if err := s.saveSession(result); err != nil {
		return session{}, err
	}
	s.sessions[result.ID] = result
	return result, nil
}

func (s *Store) Upload(id, buildID string, typ debuginfopb.DebuginfoType, write func(io.Writer) error) (session, error) {
	s.mu.Lock()
	current, ok := s.sessions[id]
	if !ok {
		s.mu.Unlock()
		return session{}, ErrNotFound
	}
	if current.BuildID != buildID || current.Type != typ {
		s.mu.Unlock()
		return session{}, fmt.Errorf("debug upload metadata does not match session")
	}
	if current.Uploaded {
		s.mu.Unlock()
		return session{}, fmt.Errorf("debug upload already received")
	}
	path := s.tempPath(current.ID)
	s.mu.Unlock()
	completed := false
	defer func() {
		if completed {
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if active, ok := s.sessions[current.ID]; ok {
			_ = s.removeSession(active)
		}
	}()
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return session{}, fmt.Errorf("open debug upload: %w", err)
	}
	if err := write(&sizeLimitedWriter{writer: file, remaining: current.Size}); err != nil {
		file.Close()
		os.Remove(path)
		return session{}, err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(path)
		return session{}, fmt.Errorf("sync debug upload: %w", err)
	}
	if err := file.Close(); err != nil {
		os.Remove(path)
		return session{}, fmt.Errorf("close debug upload: %w", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return session{}, fmt.Errorf("stat debug upload: %w", err)
	}
	if info.Size() != current.Size {
		os.Remove(path)
		return session{}, fmt.Errorf("%w: got %d bytes, want %d", ErrSizeMismatch, info.Size(), current.Size)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok = s.sessions[id]
	if !ok {
		os.Remove(path)
		return session{}, ErrNotFound
	}
	current.Uploaded = true
	if err := s.saveSession(current); err != nil {
		return session{}, err
	}
	s.sessions[id] = current
	completed = true
	return current, nil
}

type sizeLimitedWriter struct {
	writer    io.Writer
	remaining int64
}

func (w *sizeLimitedWriter) Write(data []byte) (int, error) {
	if int64(len(data)) > w.remaining {
		return 0, ErrTooLarge
	}
	n, err := w.writer.Write(data)
	w.remaining -= int64(n)
	return n, err
}

func (s *Store) MarkFinished(buildID, id string, typ debuginfopb.DebuginfoType) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.sessions[id]
	if !ok {
		return ErrNotFound
	}
	if current.BuildID != buildID || current.Type != typ {
		return fmt.Errorf("debug upload metadata does not match session")
	}
	if !current.Uploaded {
		return ErrNotUploaded
	}
	artifactDir := filepath.Join(s.root, "artifacts", typeName(typ))
	if err := os.MkdirAll(artifactDir, 0o750); err != nil {
		return fmt.Errorf("create artifact directory: %w", err)
	}
	dataPath := filepath.Join(artifactDir, artifactKey(buildID, typ)+".data")
	previous, hasPrevious := s.artifact(dataPath)
	incomingSize := current.Size
	if hasPrevious && previous.size < incomingSize {
		incomingSize -= previous.size
	} else if hasPrevious {
		incomingSize = 0
	}
	if err := s.prune(incomingSize); err != nil {
		return err
	}
	if err := os.Rename(s.tempPath(id), dataPath); err != nil {
		return fmt.Errorf("publish debug artifact: %w", err)
	}
	metadata, err := json.Marshal(current)
	if err != nil {
		return fmt.Errorf("marshal debug artifact metadata: %w", err)
	}
	if err := os.WriteFile(filepath.Join(artifactDir, artifactKey(buildID, typ)+".json"), metadata, 0o600); err != nil {
		return fmt.Errorf("write debug artifact metadata: %w", err)
	}
	info, err := os.Stat(dataPath)
	if err != nil {
		return fmt.Errorf("stat debug artifact: %w", err)
	}
	if hasPrevious {
		s.removeArtifact(previous.path)
	}
	s.files = append(s.files, artifact{path: dataPath, size: info.Size(), modified: info.ModTime()})
	s.size += info.Size()
	s.sortFiles()
	if err := s.removeSession(current); err != nil {
		return err
	}
	return nil
}

func (s *Store) load() error {
	entries, err := os.ReadDir(filepath.Join(s.root, "uploads"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.root, "uploads", entry.Name()))
		if err != nil {
			return err
		}
		var current session
		if err := json.Unmarshal(data, &current); err != nil {
			return fmt.Errorf("read debug upload session %q: %w", entry.Name(), err)
		}
		if time.Since(current.CreatedAt) >= s.staleAfter {
			if err := s.removeSession(current); err != nil {
				return err
			}
			continue
		}
		s.sessions[current.ID] = current
	}
	artifactRoot := filepath.Join(s.root, "artifacts")
	types, err := os.ReadDir(artifactRoot)
	if err != nil {
		return err
	}
	for _, typ := range types {
		if !typ.IsDir() {
			continue
		}
		files, err := os.ReadDir(filepath.Join(artifactRoot, typ.Name()))
		if err != nil {
			return err
		}
		for _, file := range files {
			if !file.Type().IsRegular() || filepath.Ext(file.Name()) != ".data" {
				continue
			}
			info, err := file.Info()
			if err != nil {
				return err
			}
			s.files = append(s.files, artifact{path: filepath.Join(artifactRoot, typ.Name(), file.Name()), size: info.Size(), modified: info.ModTime()})
			s.size += info.Size()
		}
	}
	s.sortFiles()
	return nil
}

func (s *Store) prune(incoming int64) error {
	if s.maxSize == 0 {
		return nil
	}
	for s.size+incoming > s.maxSize && len(s.files) > 0 {
		oldest := s.files[0]
		if err := os.Remove(oldest.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove old debug artifact: %w", err)
		}
		os.Remove(stringsTrimSuffix(oldest.path, ".data") + ".json")
		s.files = s.files[1:]
		s.size -= oldest.size
	}
	if s.size+incoming > s.maxSize {
		return fmt.Errorf("debug storage limit is too small for artifact")
	}
	return nil
}

func (s *Store) activeSession(buildID string, typ debuginfopb.DebuginfoType) (session, bool) {
	for _, current := range s.sessions {
		if current.BuildID == buildID && current.Type == typ {
			return current, true
		}
	}
	return session{}, false
}
func (s *Store) hasArtifact(buildID string, typ debuginfopb.DebuginfoType) bool {
	_, err := os.Stat(filepath.Join(s.root, "artifacts", typeName(typ), artifactKey(buildID, typ)+".data"))
	return err == nil
}
func (s *Store) saveSession(current session) error {
	data, err := json.Marshal(current)
	if err != nil {
		return err
	}
	return os.WriteFile(s.sessionPath(current.ID), data, 0o600)
}
func (s *Store) removeSession(current session) error {
	delete(s.sessions, current.ID)
	os.Remove(s.tempPath(current.ID))
	if err := os.Remove(s.sessionPath(current.ID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
func (s *Store) sessionPath(id string) string { return filepath.Join(s.root, "uploads", id+".json") }
func (s *Store) tempPath(id string) string    { return filepath.Join(s.root, "uploads", id+".part") }
func (s *Store) sortFiles() {
	sort.Slice(s.files, func(i, j int) bool {
		if s.files[i].modified.Equal(s.files[j].modified) {
			return s.files[i].path < s.files[j].path
		}
		return s.files[i].modified.Before(s.files[j].modified)
	})
}
func (s *Store) artifact(path string) (artifact, bool) {
	for _, current := range s.files {
		if current.path == path {
			return current, true
		}
	}
	return artifact{}, false
}
func (s *Store) removeArtifact(path string) {
	for i, current := range s.files {
		if current.path == path {
			s.files = append(s.files[:i], s.files[i+1:]...)
			s.size -= current.size
			return
		}
	}
}
func artifactKey(buildID string, typ debuginfopb.DebuginfoType) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d\x00%s", typ, buildID)))
	return hex.EncodeToString(sum[:])
}
func typeName(typ debuginfopb.DebuginfoType) string {
	return map[debuginfopb.DebuginfoType]string{debuginfopb.DebuginfoType_DEBUGINFO_TYPE_DEBUGINFO_UNSPECIFIED: "debuginfo", debuginfopb.DebuginfoType_DEBUGINFO_TYPE_EXECUTABLE: "executable", debuginfopb.DebuginfoType_DEBUGINFO_TYPE_SOURCES: "sources", debuginfopb.DebuginfoType(3): "source-map"}[typ]
}
func stringsTrimSuffix(value, suffix string) string { return value[:len(value)-len(suffix)] }
func validate(buildID string, typ debuginfopb.DebuginfoType) error {
	if buildID == "" {
		return fmt.Errorf("build ID is required")
	}
	if typeName(typ) == "" {
		return fmt.Errorf("unsupported debug artifact type %q", typ)
	}
	return nil
}
func validBuildIDType(typ debuginfopb.BuildIDType) bool {
	if typ == debuginfopb.BuildIDType(4) {
		return true // SOURCE_MAP_DEBUG_ID was added after the pinned generated API.
	}
	_, ok := debuginfopb.BuildIDType_name[int32(typ)]
	return ok
}
