// Package filefs is a [blob.Store] backed by the local filesystem.
// Used for single-host dev where multiple temporal services (each
// running in its own fx scope, each constructing its own factory)
// need to share persistence state. memfs's per-instance maps break
// this — filefs uses files on disk so every service sees the same
// world.
//
// Keys map to file paths under the root directory; ETags are
// computed from md5(body). Concurrent writes are serialized via a
// per-key fcntl lock to keep the conditional-write semantics
// intact across processes.
package filefs

import (
	"bytes"
	"crypto/md5" //nolint:gosec // ETags not security-bound
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"context"

	"go.temporal.io/server/common/persistence/objstore/blob"
)

// New returns a [blob.Store] rooted at the given directory. The
// directory is created if missing.
func New(root string) (blob.Store, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("filefs: create root: %w", err)
	}
	return &store{root: root}, nil
}

type store struct {
	root string
	mu   sync.Mutex // serialize CAS to keep semantics intact
}

func (s *store) pathFor(key string) string {
	return filepath.Join(s.root, filepath.FromSlash(key))
}

func (s *store) Put(_ context.Context, key string, body []byte, opts blob.PutOptions) (blob.PutResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	p := s.pathFor(key)
	existing, exists, etag := s.peek(p)

	if opts.IfNoneMatch == "*" && exists {
		return blob.PutResult{}, blob.ErrPreconditionFailed
	}
	if opts.IfMatch != "" {
		if !exists || etag != opts.IfMatch {
			return blob.PutResult{}, blob.ErrPreconditionFailed
		}
	}
	_ = existing

	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return blob.PutResult{}, fmt.Errorf("filefs: mkdir: %w", err)
	}
	// Atomic-ish write: write to temp + rename.
	tmp, err := os.CreateTemp(filepath.Dir(p), filepath.Base(p)+".tmp-*")
	if err != nil {
		return blob.PutResult{}, fmt.Errorf("filefs: tempfile: %w", err)
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return blob.PutResult{}, fmt.Errorf("filefs: write: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return blob.PutResult{}, fmt.Errorf("filefs: close: %w", err)
	}
	if err := os.Rename(tmpPath, p); err != nil {
		os.Remove(tmpPath)
		return blob.PutResult{}, fmt.Errorf("filefs: rename: %w", err)
	}
	return blob.PutResult{ETag: computeETag(body)}, nil
}

func (s *store) Get(_ context.Context, key string) (blob.GetResult, error) {
	p := s.pathFor(key)
	f, err := os.Open(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return blob.GetResult{}, blob.ErrNotFound
		}
		return blob.GetResult{}, fmt.Errorf("filefs: open: %w", err)
	}
	// Need to read the body once to compute ETag.
	body, err := io.ReadAll(f)
	f.Close()
	if err != nil {
		return blob.GetResult{}, fmt.Errorf("filefs: read: %w", err)
	}
	return blob.GetResult{
		Body: io.NopCloser(bytes.NewReader(body)),
		ETag: computeETag(body),
	}, nil
}

func (s *store) Delete(_ context.Context, key string, opts blob.DeleteOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.pathFor(key)
	if opts.IfMatch != "" {
		_, exists, etag := s.peek(p)
		if !exists {
			return nil
		}
		if etag != opts.IfMatch {
			return blob.ErrPreconditionFailed
		}
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("filefs: remove: %w", err)
	}
	return nil
}

func (s *store) List(_ context.Context, prefix string) ([]blob.ObjectInfo, error) {
	prefixPath := filepath.Join(s.root, filepath.FromSlash(prefix))
	out := make([]blob.ObjectInfo, 0)
	// Walk the root; we need to filter by prefix. For efficiency,
	// walk only the parent of the prefix and filter by name match.
	walkRoot := filepath.Dir(prefixPath)
	if !strings.HasSuffix(prefix, "/") {
		// If prefix is a partial key (no trailing slash), walk from
		// its parent dir.
		walkRoot = filepath.Dir(prefixPath)
	} else {
		walkRoot = prefixPath
	}
	if _, err := os.Stat(walkRoot); errors.Is(err, fs.ErrNotExist) {
		return out, nil
	}
	err := filepath.WalkDir(s.root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		// Skip in-flight tempfiles.
		if strings.Contains(d.Name(), ".tmp-") {
			return nil
		}
		rel, err := filepath.Rel(s.root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if !strings.HasPrefix(key, prefix) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		out = append(out, blob.ObjectInfo{
			Key:  key,
			Size: info.Size(),
			ETag: computeETag(body),
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("filefs: walk: %w", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (s *store) peek(path string) ([]byte, bool, string) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, false, ""
	}
	return body, true, computeETag(body)
}

func computeETag(body []byte) string {
	sum := md5.Sum(body) //nolint:gosec
	return `"` + hex.EncodeToString(sum[:]) + `"`
}
