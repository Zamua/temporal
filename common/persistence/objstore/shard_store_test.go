package objstore_test

import (
	"context"
	"errors"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/objstore"
	"go.temporal.io/server/common/persistence/objstore/blob/memfs"
)

// shardStoreFactory builds a fresh factory + a shard store off it.
// Helper because every test wants both.
func shardStoreFactory(t *testing.T) (persistence.ShardStore, *objstore.Factory) {
	t.Helper()
	f := objstore.NewFactory(memfs.New(), "active", log.NewNoopLogger())
	store, err := f.NewShardStore()
	if err != nil {
		t.Fatalf("NewShardStore: %v", err)
	}
	return store, f
}

func sampleShardInfo(payload string) *commonpb.DataBlob {
	return &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte(payload),
	}
}

func TestShardStore_GetOrCreate_CreatesWhenMissing(t *testing.T) {
	store, _ := shardStoreFactory(t)
	resp, err := store.GetOrCreateShard(context.Background(), &persistence.InternalGetOrCreateShardRequest{
		ShardID: 7,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
			return 42, sampleShardInfo("initial"), nil
		},
	})
	if err != nil {
		t.Fatalf("GetOrCreateShard: %v", err)
	}
	if resp == nil || resp.ShardInfo == nil {
		t.Fatal("expected ShardInfo in response")
	}
	if string(resp.ShardInfo.Data) != "initial" {
		t.Fatalf("expected data 'initial', got %q", resp.ShardInfo.Data)
	}
	if resp.ShardInfo.EncodingType != enumspb.ENCODING_TYPE_PROTO3 {
		t.Fatalf("expected Proto3 encoding, got %v", resp.ShardInfo.EncodingType)
	}
}

func TestShardStore_GetOrCreate_ReturnsExistingWhenPresent(t *testing.T) {
	store, _ := shardStoreFactory(t)
	ctx := context.Background()

	createCount := 0
	create := func() (int64, *commonpb.DataBlob, error) {
		createCount++
		return 1, sampleShardInfo("first"), nil
	}
	if _, err := store.GetOrCreateShard(ctx, &persistence.InternalGetOrCreateShardRequest{ShardID: 1, CreateShardInfo: create}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	resp, err := store.GetOrCreateShard(ctx, &persistence.InternalGetOrCreateShardRequest{ShardID: 1, CreateShardInfo: create})
	if err != nil {
		t.Fatalf("second GetOrCreateShard: %v", err)
	}
	if string(resp.ShardInfo.Data) != "first" {
		t.Fatalf("expected existing data 'first', got %q", resp.ShardInfo.Data)
	}
	if createCount != 1 {
		t.Fatalf("CreateShardInfo should only be invoked once, got %d", createCount)
	}
}

func TestShardStore_GetOrCreate_RaceLoserReturnsWinnerValue(t *testing.T) {
	// Seed shard 1 with rangeID=10.
	store, _ := shardStoreFactory(t)
	ctx := context.Background()
	if _, err := store.GetOrCreateShard(ctx, &persistence.InternalGetOrCreateShardRequest{
		ShardID:         1,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) { return 10, sampleShardInfo("winner"), nil },
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Second creator races in with different value — should see the winner's value.
	resp, err := store.GetOrCreateShard(ctx, &persistence.InternalGetOrCreateShardRequest{
		ShardID:         1,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) { return 99, sampleShardInfo("loser"), nil },
	})
	if err != nil {
		t.Fatalf("racing GetOrCreateShard: %v", err)
	}
	if string(resp.ShardInfo.Data) != "winner" {
		t.Fatalf("expected winner's data, got %q", resp.ShardInfo.Data)
	}
}

func TestShardStore_UpdateShard_SuccessAdvancesRange(t *testing.T) {
	store, _ := shardStoreFactory(t)
	ctx := context.Background()
	if _, err := store.GetOrCreateShard(ctx, &persistence.InternalGetOrCreateShardRequest{
		ShardID:         3,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) { return 1, sampleShardInfo("v1"), nil },
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	err := store.UpdateShard(ctx, &persistence.InternalUpdateShardRequest{
		ShardID:         3,
		RangeID:         2,
		PreviousRangeID: 1,
		ShardInfo:       sampleShardInfo("v2"),
	})
	if err != nil {
		t.Fatalf("UpdateShard: %v", err)
	}

	// Confirm by re-reading.
	resp, err := store.GetOrCreateShard(ctx, &persistence.InternalGetOrCreateShardRequest{ShardID: 3})
	if err != nil {
		t.Fatalf("re-read: %v", err)
	}
	if string(resp.ShardInfo.Data) != "v2" {
		t.Fatalf("expected v2 after update, got %q", resp.ShardInfo.Data)
	}
}

func TestShardStore_UpdateShard_StaleRangeIDReturnsOwnershipLost(t *testing.T) {
	store, _ := shardStoreFactory(t)
	ctx := context.Background()
	if _, err := store.GetOrCreateShard(ctx, &persistence.InternalGetOrCreateShardRequest{
		ShardID:         3,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) { return 5, sampleShardInfo("v1"), nil },
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Update with PreviousRangeID=99 (wrong) — must return ShardOwnershipLostError.
	err := store.UpdateShard(ctx, &persistence.InternalUpdateShardRequest{
		ShardID:         3,
		RangeID:         6,
		PreviousRangeID: 99,
		ShardInfo:       sampleShardInfo("attempted"),
	})
	var owned *persistence.ShardOwnershipLostError
	if !errors.As(err, &owned) {
		t.Fatalf("expected ShardOwnershipLostError, got %v", err)
	}
	if owned.ShardID != 3 {
		t.Fatalf("expected ShardID=3 in error, got %d", owned.ShardID)
	}
}

func TestShardStore_UpdateShard_OnMissingShardReturnsOwnershipLost(t *testing.T) {
	store, _ := shardStoreFactory(t)
	err := store.UpdateShard(context.Background(), &persistence.InternalUpdateShardRequest{
		ShardID:         404,
		RangeID:         1,
		PreviousRangeID: 0,
		ShardInfo:       sampleShardInfo("nope"),
	})
	var owned *persistence.ShardOwnershipLostError
	if !errors.As(err, &owned) {
		t.Fatalf("expected ShardOwnershipLostError on missing shard, got %v", err)
	}
}

func TestShardStore_GetOrCreate_NoCallbackOnMissingShardReturnsOwnershipLost(t *testing.T) {
	store, _ := shardStoreFactory(t)
	_, err := store.GetOrCreateShard(context.Background(), &persistence.InternalGetOrCreateShardRequest{
		ShardID: 99,
	})
	var owned *persistence.ShardOwnershipLostError
	if !errors.As(err, &owned) {
		t.Fatalf("expected ShardOwnershipLostError without callback, got %v", err)
	}
}

func TestShardStore_AssertShardOwnership_IsNoop(t *testing.T) {
	store, _ := shardStoreFactory(t)
	err := store.AssertShardOwnership(context.Background(), &persistence.AssertShardOwnershipRequest{
		ShardID: 1,
		RangeID: 999,
	})
	if err != nil {
		t.Fatalf("AssertShardOwnership must be a no-op, got %v", err)
	}
}

func TestShardStore_NameAndCluster(t *testing.T) {
	store, _ := shardStoreFactory(t)
	if store.GetName() != "objstore" {
		t.Errorf("expected name 'objstore', got %q", store.GetName())
	}
	if store.GetClusterName() != "active" {
		t.Errorf("expected cluster 'active', got %q", store.GetClusterName())
	}
}
