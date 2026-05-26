// Package blob defines the object-storage abstraction every objstore
// persistence implementation runs on top of. The interface is small
// enough to fit any object-storage backend (AWS S3, MinIO, R2, B2,
// Google Cloud Storage, Azure Blob Storage, an in-memory fake for
// tests) and rich enough to express the optimistic-concurrency
// primitives the higher-level stores need: conditional PUT (ETag
// CAS), idempotent DELETE, and lexicographic LIST.
//
// Why this layer exists: Temporal's existing persistence backends
// (SQL, Cassandra) couple the storage primitives to the query model
// — you can't separate "how do I persist a workflow execution" from
// "how do I read it back" because both go through the same SQL
// engine. Object storage is different. The primitives are tiny
// (read/write/list/conditional-write) and provider-agnostic. By
// making them an explicit port we get:
//
//   - One Store interface, many providers (S3, GCS, MinIO, …)
//   - Trivial test backend (memfs) — same code path as production
//   - The higher-level stores (Shard, Execution, Task, …) never know
//     which provider they're talking to
//
// Naming convention: this package is "blob" rather than "objstore"
// (which is the wrapper package) so a caller writes `blob.Store`
// in their type signatures — short, evocative, no stuttering.
package blob

import (
	"context"
	"errors"
	"io"
)

// ErrNotFound is returned by Get/Head when the key does not exist.
var ErrNotFound = errors.New("blob: object not found")

// ErrPreconditionFailed is returned by Put when an If-Match or
// If-None-Match precondition is not satisfied. Higher-level stores
// use this signal for CAS retry loops.
var ErrPreconditionFailed = errors.New("blob: precondition failed")

// Store is the object-storage port. Backends implement this in
// terms of their native API (S3 PutObject/GetObject/ListObjectsV2,
// GCS Object.NewWriter/NewReader, etc.).
//
// All methods are safe to call concurrently. Implementations should
// honor ctx for cancellation + deadlines.
type Store interface {
	// Put writes an object at key. The body's content-type and
	// optional precondition headers come through PutOptions. Returns
	// the object's ETag on success — callers use it for subsequent
	// CAS writes via PutOptions.IfMatch.
	Put(ctx context.Context, key string, body []byte, opts PutOptions) (PutResult, error)

	// Get returns the object's body as a ReadCloser. Caller must
	// Close it. Returns ErrNotFound when the key does not exist.
	Get(ctx context.Context, key string) (GetResult, error)

	// Delete removes the object at key. Idempotent — deleting a
	// missing key returns nil. Implementations may surface
	// ErrPreconditionFailed when opts.IfMatch is set and doesn't
	// match the current ETag.
	Delete(ctx context.Context, key string, opts DeleteOptions) error

	// List returns objects whose keys start with prefix, sorted
	// lexicographically. The result is bounded by the implementation
	// (typically the provider's per-page limit, e.g. 1000 for S3).
	// Callers needing pagination should narrow the prefix or use
	// the provider's continuation token directly via a backend-
	// specific extension.
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
}

// PutOptions carries the per-write metadata + CAS preconditions.
type PutOptions struct {
	// ContentType is the MIME type recorded with the object.
	// Defaults to "application/octet-stream" if empty. Recorded but
	// not required for correctness — the higher-level stores use
	// their own typed codecs.
	ContentType string

	// IfMatch makes the write conditional on the existing object
	// having this ETag. Returns ErrPreconditionFailed otherwise.
	// Used for compare-and-swap updates (e.g. workflow manifest
	// pointer-swap).
	IfMatch string

	// IfNoneMatch makes the write conditional on no object existing
	// at the key. Set to "*" for first-writer-wins semantics.
	// Returns ErrPreconditionFailed if the object already exists.
	IfNoneMatch string
}

// PutResult is the success payload from Put.
type PutResult struct {
	// ETag uniquely identifies this version of the object.
	// Pass it back in PutOptions.IfMatch to perform CAS updates.
	ETag string
}

// GetResult is the success payload from Get.
type GetResult struct {
	Body io.ReadCloser
	ETag string
}

// DeleteOptions carries CAS preconditions for Delete.
type DeleteOptions struct {
	// IfMatch makes the delete conditional on the existing object
	// having this ETag. Returns ErrPreconditionFailed otherwise.
	IfMatch string
}

// ObjectInfo is the per-object record returned by List.
type ObjectInfo struct {
	Key  string
	Size int64
	ETag string
}
