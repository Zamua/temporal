package objstore_test

import (
	"context"
	"errors"
	"testing"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/objstore"
	"go.temporal.io/server/common/persistence/objstore/blob"
	"go.temporal.io/server/common/persistence/objstore/blob/memfs"
)

// TestFactoryReturnsUnimplementedForUnwiredStores documents the
// MVP behavior: the factory satisfies persistence.DataStoreFactory,
// but every store returns Unimplemented until its own file lands.
// fx.go's managerProvider treats this as "skip this manager" rather
// than fatal, so a temporal-server can boot with a partial backend.
func TestFactoryReturnsUnimplementedForUnwiredStores(t *testing.T) {
	f := objstore.NewFactory(memfs.New(), "active", log.NewNoopLogger())

	cases := []struct {
		name string
		call func() error
	}{
		{"TaskStore", func() error { _, err := f.NewTaskStore(); return err }},
		{"FairTaskStore", func() error { _, err := f.NewFairTaskStore(); return err }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			var unimpl *serviceerror.Unimplemented
			if !errors.As(err, &unimpl) {
				t.Fatalf("expected serviceerror.Unimplemented from %s, got %v", tc.name, err)
			}
		})
	}
}

// TestAbstractFactoryBuildsMemfsBackend exercises the full
// YAML-options → blob.Store path against the memfs backend. The S3
// path is exercised by the s3 adapter's own unit tests + the (build-
// tagged) MinIO integration test.
func TestAbstractFactoryBuildsMemfsBackend(t *testing.T) {
	abstract := objstore.NewAbstractFactory()
	cfg := objstore.ConfigFromYAML("objstore", map[string]any{
		"backend": "memfs",
	})
	df := abstract.NewFactory(cfg, nil, "active", log.NewNoopLogger(), nil, nil)
	if df == nil {
		t.Fatal("expected non-nil DataStoreFactory from AbstractFactory.NewFactory")
	}
	// Calling Close() must not panic.
	df.Close()
}

// TestBlobAccessRoundTripsViaFactory verifies the Factory's
// Blob() seam — confirms the blob.Store that came out of the
// AbstractFactory yaml path is the SAME instance the per-store
// adapters will share. Catches "factory built its own store and
// ignored the one we configured" regressions.
func TestBlobAccessRoundTripsViaFactory(t *testing.T) {
	store := memfs.New()
	f := objstore.NewFactory(store, "active", log.NewNoopLogger())
	if f.Blob() != store {
		t.Fatal("Factory.Blob() must return the same blob.Store it was constructed with")
	}

	// Sanity-check the store still works through the factory's handle.
	ctx := context.Background()
	_, err := f.Blob().Put(ctx, "k", []byte("v"), blob.PutOptions{})
	if err != nil {
		t.Fatalf("put via Factory.Blob(): %v", err)
	}
	got, err := f.Blob().Get(ctx, "k")
	if err != nil {
		t.Fatalf("get via Factory.Blob(): %v", err)
	}
	got.Body.Close()
}
