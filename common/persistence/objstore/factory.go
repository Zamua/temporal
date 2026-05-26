package objstore

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/objstore/blob"
	"go.temporal.io/server/common/persistence/serialization"
)

// Factory is the objstore implementation of
// [persistence.DataStoreFactory]. Every New*Store method below
// returns a per-store adapter that goes through the same underlying
// [blob.Store]. Stores start as Unimplemented and get lit up one at
// a time — see the per-store files (`shard_store.go`,
// `execution_store.go`, …) as they land.
//
// fx.go's `managerProvider` treats [serviceerror.Unimplemented]
// returned by Factory.New*Store as "not wired" and skips the
// manager rather than fatal-erroring at boot. This lets us merge
// the factory before every store is implemented.
type Factory struct {
	blob        blob.Store
	clusterName string
	logger      log.Logger
	serializer  serialization.Serializer
}

// NewFactory constructs a [Factory] from a pre-built [blob.Store].
// Use this directly in tests (with a memfs store); production
// callers go through [NewAbstractFactory] → CustomDatastoreConfig.
//
// A nil serializer is allowed and gets replaced with the default —
// tests rarely care about which serializer is in use, and the
// default handles all the encoding types Temporal hands out.
func NewFactory(blobStore blob.Store, clusterName string, logger log.Logger) *Factory {
	return &Factory{
		blob:        blobStore,
		clusterName: clusterName,
		logger:      logger,
		serializer:  serialization.NewSerializer(),
	}
}

// NewFactoryWithSerializer is the variant used by the abstract
// factory — when fx wires us in, it provides the global
// [serialization.Serializer] instance.
func NewFactoryWithSerializer(blobStore blob.Store, clusterName string, logger log.Logger, serializer serialization.Serializer) *Factory {
	if serializer == nil {
		serializer = serialization.NewSerializer()
	}
	return &Factory{
		blob:        blobStore,
		clusterName: clusterName,
		logger:      logger,
		serializer:  serializer,
	}
}

// Close releases the underlying [blob.Store] (no-op for memfs;
// flushes pending writes for S3). Safe to call multiple times.
func (f *Factory) Close() {
	// Nothing to do today — blob.Store has no Close method
	// because S3/MinIO clients pool connections lazily and don't
	// need explicit shutdown. If a future backend needs cleanup,
	// add a Close() method to blob.Store and dispatch here.
}

// NewTaskStore returns the persistence.TaskStore. Unimplemented
// until task_store.go lands.
func (f *Factory) NewTaskStore() (persistence.TaskStore, error) {
	return nil, serviceerror.NewUnimplemented("objstore: TaskStore not yet implemented")
}

// NewFairTaskStore returns the fair-task persistence.TaskStore.
// Unimplemented until fair-task support lands.
func (f *Factory) NewFairTaskStore() (persistence.TaskStore, error) {
	return nil, serviceerror.NewUnimplemented("objstore: FairTaskStore not yet implemented")
}

// NewShardStore returns the persistence.ShardStore backed by the
// factory's blob.Store. Layout: shards/{cluster}/{shardID}/info.
func (f *Factory) NewShardStore() (persistence.ShardStore, error) {
	return newShardStore(f.blob, f.clusterName), nil
}

// NewMetadataStore returns the persistence.MetadataStore (namespaces).
func (f *Factory) NewMetadataStore() (persistence.MetadataStore, error) {
	return newMetadataStore(f.blob), nil
}

// NewExecutionStore returns the persistence.ExecutionStore. The
// happy-path methods (Create/Get/Update/Set/Delete/GetCurrent) are
// live; the deferred subsets (history V2 branches, task queues,
// DLQ, conflict resolve, list-concrete) are per-method Unimplemented
// stubs landing in #288/#289/#290.
func (f *Factory) NewExecutionStore() (persistence.ExecutionStore, error) {
	return newExecutionStore(f.blob, f.clusterName, f.serializer), nil
}

// NewQueue returns the persistence.Queue. Unimplemented until
// queue_store.go lands.
func (f *Factory) NewQueue(queueType persistence.QueueType) (persistence.Queue, error) {
	_ = queueType
	return nil, serviceerror.NewUnimplemented("objstore: Queue not yet implemented")
}

// NewQueueV2 returns the persistence.QueueV2.
func (f *Factory) NewQueueV2() (persistence.QueueV2, error) {
	return newQueueV2Store(f.blob), nil
}

// NewClusterMetadataStore returns the persistence.ClusterMetadataStore.
func (f *Factory) NewClusterMetadataStore() (persistence.ClusterMetadataStore, error) {
	return newClusterMetadataStore(f.blob), nil
}

// NewNexusEndpointStore returns the persistence.NexusEndpointStore.
func (f *Factory) NewNexusEndpointStore() (persistence.NexusEndpointStore, error) {
	return newNexusEndpointStore(f.blob), nil
}

// Blob is the test seam — returns the underlying [blob.Store] so
// per-store tests can introspect what's been written.
func (f *Factory) Blob() blob.Store { return f.blob }

// ensureBlobReady is the hook every store entrypoint will call
// once they're real — fails fast if the underlying blob.Store
// wasn't built. Today no store calls it (all are Unimplemented);
// kept here so the seam is obvious.
func (f *Factory) ensureBlobReady(ctx context.Context) error {
	_ = ctx
	if f.blob == nil {
		f.logger.Error("objstore: blob store is nil — factory was not initialized via NewFactory",
			tag.NewStringTag("backend", "unknown"))
		return serviceerror.NewInternal("objstore: blob store not initialized")
	}
	return nil
}
