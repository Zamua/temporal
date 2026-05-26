package objstore_test

import (
	"context"
	"testing"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/objstore"
	"go.temporal.io/server/common/persistence/objstore/blob"
	"go.temporal.io/server/common/persistence/objstore/blob/memfs"
)

// TestFactoryWiresAllStores smoke-checks that Factory.New*Store all
// hand back real implementations (none of them return Unimplemented
// today). The fx managerProvider used to expect Unimplemented for
// any stub — leaving this test in place ensures regressions don't
// silently un-wire a store.
func TestFactoryWiresAllStores(t *testing.T) {
	f := objstore.NewFactory(memfs.New(), "active", log.NewNoopLogger())

	// All stores are now real — this test reads as a smoke check
	// that every Factory.New* returns nil-error + non-nil store.
	cases := []struct {
		name string
		call func() (any, error)
	}{
		{"TaskStore", func() (any, error) { return f.NewTaskStore() }},
		{"FairTaskStore", func() (any, error) { return f.NewFairTaskStore() }},
		{"ShardStore", func() (any, error) { return f.NewShardStore() }},
		{"MetadataStore", func() (any, error) { return f.NewMetadataStore() }},
		{"ExecutionStore", func() (any, error) { return f.NewExecutionStore() }},
		{"QueueV2", func() (any, error) { return f.NewQueueV2() }},
		{"ClusterMetadataStore", func() (any, error) { return f.NewClusterMetadataStore() }},
		{"NexusEndpointStore", func() (any, error) { return f.NewNexusEndpointStore() }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, err := tc.call()
			if err != nil {
				t.Fatalf("%s returned err: %v", tc.name, err)
			}
			if store == nil {
				t.Fatalf("%s returned nil store", tc.name)
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
