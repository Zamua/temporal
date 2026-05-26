// Package sharedmem is a process-global in-memory [blob.Store] keyed
// by a name. All callers passing the same name share the same
// underlying map, so multiple fx scopes (one per Temporal service)
// in the same process see consistent state without going through the
// filesystem.
//
// Use cases:
//   - Local dev where every service is in one process and you want
//     conformance-speed writes (no APFS rename latency).
//   - Unit tests that span multiple factories.
//
// For production, use the s3 backend. For per-test isolation, use
// memfs (each instance is its own map).
package sharedmem

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // ETags not security-bound
	"encoding/hex"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"

	"go.temporal.io/server/common/persistence/objstore/blob"
)

var (
	registry   = map[string]*store{}
	registryMu sync.Mutex
)

// Named returns a [blob.Store] backed by a process-global map keyed
// by `name`. Multiple calls with the same name return the SAME
// underlying state; calls with different names get separate stores.
func Named(name string) blob.Store {
	registryMu.Lock()
	defer registryMu.Unlock()
	s, ok := registry[name]
	if !ok {
		s = &store{objects: map[string]object{}}
		registry[name] = s
	}
	return s
}

// Reset clears the named store's state (test helper).
func Reset(name string) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if s, ok := registry[name]; ok {
		s.mu.Lock()
		s.objects = map[string]object{}
		s.mu.Unlock()
	}
}

type object struct {
	body []byte
	etag string
}

type store struct {
	mu      sync.Mutex
	objects map[string]object
}

func (s *store) Put(_ context.Context, key string, body []byte, opts blob.PutOptions) (blob.PutResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.objects[key]
	if opts.IfNoneMatch == "*" && exists {
		return blob.PutResult{}, blob.ErrPreconditionFailed
	}
	if opts.IfMatch != "" {
		if !exists || existing.etag != opts.IfMatch {
			return blob.PutResult{}, blob.ErrPreconditionFailed
		}
	}
	etag := computeETag(body)
	s.objects[key] = object{body: append([]byte(nil), body...), etag: etag}
	return blob.PutResult{ETag: etag}, nil
}

func (s *store) Get(_ context.Context, key string) (blob.GetResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[key]
	if !ok {
		return blob.GetResult{}, blob.ErrNotFound
	}
	return blob.GetResult{
		Body: io.NopCloser(bytes.NewReader(obj.body)),
		ETag: obj.etag,
	}, nil
}

func (s *store) Delete(_ context.Context, key string, opts blob.DeleteOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if opts.IfMatch != "" {
		existing, exists := s.objects[key]
		if !exists {
			return nil
		}
		if existing.etag != opts.IfMatch {
			return blob.ErrPreconditionFailed
		}
	}
	delete(s.objects, key)
	return nil
}

func (s *store) List(_ context.Context, prefix string) ([]blob.ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]blob.ObjectInfo, 0)
	for key, obj := range s.objects {
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

// _ ensures errors are reachable from this package for type assertions.
var _ = errors.Is
