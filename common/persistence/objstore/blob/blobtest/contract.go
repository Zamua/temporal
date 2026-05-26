// Package blobtest is the conformance suite every [blob.Store]
// implementation runs against. Backends pass a factory to
// RunContract; the suite exercises Put/Get/Delete/List + the
// conditional-write semantics every higher-level store relies on.
//
// memfs uses it as its primary test; s3 will reuse it against
// real MinIO + an offline mock.
package blobtest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"go.temporal.io/server/common/persistence/objstore/blob"
)

// Factory builds a fresh, empty [blob.Store] plus a cleanup hook.
// Run inside each subtest so cases stay isolated.
type Factory func(t *testing.T) (store blob.Store, cleanup func())

// RunContract drives the full conformance suite against factory.
func RunContract(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("PutGetRoundTrip", func(t *testing.T) { testPutGetRoundTrip(t, factory) })
	t.Run("GetMissingReturnsNotFound", func(t *testing.T) { testGetMissingReturnsNotFound(t, factory) })
	t.Run("PutOverwrites", func(t *testing.T) { testPutOverwrites(t, factory) })
	t.Run("DeleteIsIdempotent", func(t *testing.T) { testDeleteIsIdempotent(t, factory) })
	t.Run("IfNoneMatchStarRejectsExisting", func(t *testing.T) { testIfNoneMatchStar(t, factory) })
	t.Run("IfMatchRejectsStaleETag", func(t *testing.T) { testIfMatchRejectsStale(t, factory) })
	t.Run("IfMatchAllowsCurrentETag", func(t *testing.T) { testIfMatchAllowsCurrent(t, factory) })
	t.Run("DeleteIfMatchRejectsStale", func(t *testing.T) { testDeleteIfMatchRejectsStale(t, factory) })
	t.Run("ListReturnsKeysWithPrefixSorted", func(t *testing.T) { testListPrefixSorted(t, factory) })
	t.Run("ListPrefixIsExclusive", func(t *testing.T) { testListPrefixExclusive(t, factory) })
}

func testPutGetRoundTrip(t *testing.T, factory Factory) {
	store, cleanup := factory(t)
	defer cleanup()
	ctx := context.Background()

	want := []byte("hello world")
	put, err := store.Put(ctx, "k/1", want, blob.PutOptions{})
	mustNoErr(t, err)
	if put.ETag == "" {
		t.Fatal("expected non-empty ETag on Put")
	}

	got, err := store.Get(ctx, "k/1")
	mustNoErr(t, err)
	defer got.Body.Close()
	if got.ETag != put.ETag {
		t.Fatalf("etag mismatch: got %q want %q", got.ETag, put.ETag)
	}
	body, err := io.ReadAll(got.Body)
	mustNoErr(t, err)
	if !bytes.Equal(body, want) {
		t.Fatalf("body mismatch: got %q want %q", body, want)
	}
}

func testGetMissingReturnsNotFound(t *testing.T, factory Factory) {
	store, cleanup := factory(t)
	defer cleanup()
	_, err := store.Get(context.Background(), "missing")
	if !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func testPutOverwrites(t *testing.T, factory Factory) {
	store, cleanup := factory(t)
	defer cleanup()
	ctx := context.Background()
	_, err := store.Put(ctx, "k", []byte("v1"), blob.PutOptions{})
	mustNoErr(t, err)
	put2, err := store.Put(ctx, "k", []byte("v2"), blob.PutOptions{})
	mustNoErr(t, err)

	got, err := store.Get(ctx, "k")
	mustNoErr(t, err)
	defer got.Body.Close()
	if got.ETag != put2.ETag {
		t.Fatalf("expected etag from second put, got %q", got.ETag)
	}
	body, _ := io.ReadAll(got.Body)
	if string(body) != "v2" {
		t.Fatalf("expected v2, got %q", body)
	}
}

func testDeleteIsIdempotent(t *testing.T, factory Factory) {
	store, cleanup := factory(t)
	defer cleanup()
	ctx := context.Background()
	if err := store.Delete(ctx, "never-existed", blob.DeleteOptions{}); err != nil {
		t.Fatalf("delete of missing key should be nil, got %v", err)
	}
	_, err := store.Put(ctx, "k", []byte("v"), blob.PutOptions{})
	mustNoErr(t, err)
	mustNoErr(t, store.Delete(ctx, "k", blob.DeleteOptions{}))
	_, err = store.Get(ctx, "k")
	if !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func testIfNoneMatchStar(t *testing.T, factory Factory) {
	store, cleanup := factory(t)
	defer cleanup()
	ctx := context.Background()
	_, err := store.Put(ctx, "k", []byte("v1"), blob.PutOptions{IfNoneMatch: "*"})
	mustNoErr(t, err)
	_, err = store.Put(ctx, "k", []byte("v2"), blob.PutOptions{IfNoneMatch: "*"})
	if !errors.Is(err, blob.ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed on second IfNoneMatch=*, got %v", err)
	}
}

func testIfMatchRejectsStale(t *testing.T, factory Factory) {
	store, cleanup := factory(t)
	defer cleanup()
	ctx := context.Background()
	_, err := store.Put(ctx, "k", []byte("v1"), blob.PutOptions{})
	mustNoErr(t, err)
	_, err = store.Put(ctx, "k", []byte("v2"), blob.PutOptions{IfMatch: "\"not-the-right-etag\""})
	if !errors.Is(err, blob.ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed for stale IfMatch, got %v", err)
	}
}

func testIfMatchAllowsCurrent(t *testing.T, factory Factory) {
	store, cleanup := factory(t)
	defer cleanup()
	ctx := context.Background()
	put1, err := store.Put(ctx, "k", []byte("v1"), blob.PutOptions{})
	mustNoErr(t, err)
	put2, err := store.Put(ctx, "k", []byte("v2"), blob.PutOptions{IfMatch: put1.ETag})
	mustNoErr(t, err)
	if put2.ETag == put1.ETag {
		t.Fatal("expected new etag after CAS update")
	}
	got, err := store.Get(ctx, "k")
	mustNoErr(t, err)
	body, _ := io.ReadAll(got.Body)
	got.Body.Close()
	if string(body) != "v2" {
		t.Fatalf("expected v2 after CAS, got %q", body)
	}
}

func testDeleteIfMatchRejectsStale(t *testing.T, factory Factory) {
	store, cleanup := factory(t)
	defer cleanup()
	ctx := context.Background()
	_, err := store.Put(ctx, "k", []byte("v"), blob.PutOptions{})
	mustNoErr(t, err)
	err = store.Delete(ctx, "k", blob.DeleteOptions{IfMatch: "\"wrong\""})
	if !errors.Is(err, blob.ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed on stale Delete IfMatch, got %v", err)
	}
}

func testListPrefixSorted(t *testing.T, factory Factory) {
	store, cleanup := factory(t)
	defer cleanup()
	ctx := context.Background()
	keys := []string{"a/1", "a/3", "a/2", "b/1"}
	for _, k := range keys {
		_, err := store.Put(ctx, k, []byte(k), blob.PutOptions{})
		mustNoErr(t, err)
	}
	infos, err := store.List(ctx, "a/")
	mustNoErr(t, err)
	if len(infos) != 3 {
		t.Fatalf("expected 3 keys under a/, got %d (%+v)", len(infos), infos)
	}
	wantSorted := []string{"a/1", "a/2", "a/3"}
	for i, w := range wantSorted {
		if infos[i].Key != w {
			t.Fatalf("list[%d] = %q, want %q", i, infos[i].Key, w)
		}
		if infos[i].Size != int64(len(w)) {
			t.Fatalf("list[%d].Size = %d, want %d", i, infos[i].Size, len(w))
		}
		if infos[i].ETag == "" {
			t.Fatalf("list[%d].ETag empty", i)
		}
	}
}

func testListPrefixExclusive(t *testing.T, factory Factory) {
	store, cleanup := factory(t)
	defer cleanup()
	ctx := context.Background()
	_, err := store.Put(ctx, "namespaces/x", []byte("n"), blob.PutOptions{})
	mustNoErr(t, err)
	_, err = store.Put(ctx, "workflows/x", []byte("w"), blob.PutOptions{})
	mustNoErr(t, err)
	infos, err := store.List(ctx, "namespaces/")
	mustNoErr(t, err)
	if len(infos) != 1 || infos[0].Key != "namespaces/x" {
		t.Fatalf("expected only namespaces/x, got %+v", infos)
	}
}

func mustNoErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
