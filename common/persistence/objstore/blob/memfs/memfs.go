// Package memfs is an in-process [blob.Store] backed by a map.
// Used by tests + the local dev path so the persistence layer can
// be exercised without a real object-storage backend running.
//
// Concurrency: safe for use from multiple goroutines — operations
// take the same mutex. Implementations of higher-level stores
// assume that.
//
// Ports the design from driftwood's internal/storage/memfs.
package memfs

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // ETags are not security boundaries
	"encoding/hex"
	"io"
	"sort"
	"strings"
	"sync"

	"go.temporal.io/server/common/persistence/objstore/blob"
)

// New returns an empty in-memory blob.Store.
func New() blob.Store {
	return &memfs{objects: map[string]object{}}
}

type object struct {
	body []byte
	etag string
}

type memfs struct {
	mu      sync.Mutex
	objects map[string]object
}

// Put — see blob.Store. ETag is the md5 of the body, hex-encoded
// and quoted (matching S3's ETag convention) so callers can hand
// the value back unchanged.
func (m *memfs) Put(_ context.Context, key string, body []byte, opts blob.PutOptions) (blob.PutResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, exists := m.objects[key]
	if opts.IfNoneMatch == "*" && exists {
		return blob.PutResult{}, blob.ErrPreconditionFailed
	}
	if opts.IfMatch != "" {
		if !exists || existing.etag != opts.IfMatch {
			return blob.PutResult{}, blob.ErrPreconditionFailed
		}
	}

	etag := computeETag(body)
	m.objects[key] = object{body: append([]byte(nil), body...), etag: etag}
	return blob.PutResult{ETag: etag}, nil
}

// Get — see blob.Store.
func (m *memfs) Get(_ context.Context, key string) (blob.GetResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	obj, ok := m.objects[key]
	if !ok {
		return blob.GetResult{}, blob.ErrNotFound
	}
	return blob.GetResult{
		Body: io.NopCloser(bytes.NewReader(obj.body)),
		ETag: obj.etag,
	}, nil
}

// Delete — see blob.Store. Idempotent on missing keys.
func (m *memfs) Delete(_ context.Context, key string, opts blob.DeleteOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if opts.IfMatch != "" {
		existing, exists := m.objects[key]
		if !exists {
			return nil // already gone — idempotent
		}
		if existing.etag != opts.IfMatch {
			return blob.ErrPreconditionFailed
		}
	}
	delete(m.objects, key)
	return nil
}

// List — see blob.Store. Returns keys in sorted order; no
// pagination (the in-memory map fits in a single response).
func (m *memfs) List(_ context.Context, prefix string) ([]blob.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]blob.ObjectInfo, 0)
	for key, obj := range m.objects {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		out = append(out, blob.ObjectInfo{
			Key:  key,
			Size: int64(len(obj.body)),
			ETag: obj.etag,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func computeETag(body []byte) string {
	sum := md5.Sum(body) //nolint:gosec
	return `"` + hex.EncodeToString(sum[:]) + `"`
}
