package objstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/objstore/blob"
)

// shardStore implements [persistence.ShardStore] on top of a
// [blob.Store]. One object per shard at
// `shards/{cluster}/{shardID}/info`. Concurrency is handled via the
// blob store's conditional-write primitives:
//
//   - GetOrCreateShard's create path uses IfNoneMatch=* — first
//     writer wins, racing creators get ErrPreconditionFailed and
//     recurse to read the winner's value.
//
//   - UpdateShard reads the current object (capturing its ETag),
//     verifies that the stored RangeID matches the caller's
//     PreviousRangeID, then writes the new value with IfMatch on
//     the captured ETag. A failed precondition surfaces as a
//     ShardOwnershipLostError, which is what every Temporal caller
//     handles already.
//
//   - AssertShardOwnership is a no-op (matches cassandra) — the
//     RangeID check in UpdateShard is enough for the shard
//     controller's invariant.
type shardStore struct {
	blob        blob.Store
	clusterName string
}

// shardEnvelope is the JSON object we persist. The fields are kept
// minimal — RangeID for the CAS predicate, Encoding+Data to
// reconstruct the *commonpb.DataBlob the caller hands us. JSON makes
// the on-disk layout debuggable (`aws s3 cp ... -` → cat) without
// schema migrations.
type shardEnvelope struct {
	Encoding string `json:"encoding"`
	Data     []byte `json:"data"`
	RangeID  int64  `json:"range_id"`
}

// objstoreName is the value returned by GetName(). Mirrors the
// pattern used by sql/cassandra (`cassandraPersistenceName`).
const objstoreName = "objstore"

// shardKey is the object key for a given shard.
func shardKey(cluster string, shardID int32) string {
	return fmt.Sprintf("shards/%s/%d/info", cluster, shardID)
}

// NewShardStore is exported for the factory; per-store impls live
// in this package and the factory hands out the right concrete type.
func newShardStore(b blob.Store, clusterName string) persistence.ShardStore {
	return &shardStore{blob: b, clusterName: clusterName}
}

func (s *shardStore) GetName() string        { return objstoreName }
func (s *shardStore) GetClusterName() string { return s.clusterName }
func (s *shardStore) Close()                 {}

// GetOrCreateShard reads the shard at request.ShardID, or — if it
// doesn't exist and the caller supplied a CreateShardInfo callback
// — creates it atomically. Race losers on create reach the readback
// path; the recursive call is bounded by the absence of
// CreateShardInfo on retry.
func (s *shardStore) GetOrCreateShard(
	ctx context.Context,
	request *persistence.InternalGetOrCreateShardRequest,
) (*persistence.InternalGetOrCreateShardResponse, error) {
	key := shardKey(s.clusterName, request.ShardID)

	env, _, err := s.readEnvelope(ctx, key)
	if err == nil {
		return &persistence.InternalGetOrCreateShardResponse{
			ShardInfo: envelopeToBlob(env),
		}, nil
	}
	if !errors.Is(err, blob.ErrNotFound) {
		return nil, fmt.Errorf("objstore: shard get %d: %w", request.ShardID, err)
	}
	// Not found — create if the caller said it was OK.
	if request.CreateShardInfo == nil {
		return nil, &persistence.ShardOwnershipLostError{
			ShardID: request.ShardID,
			Msg:     fmt.Sprintf("objstore: shard %d does not exist and no CreateShardInfo provided", request.ShardID),
		}
	}
	rangeID, blobInfo, err := request.CreateShardInfo()
	if err != nil {
		return nil, fmt.Errorf("objstore: CreateShardInfo: %w", err)
	}
	body, err := json.Marshal(shardEnvelope{
		Encoding: blobInfo.EncodingType.String(),
		Data:     blobInfo.Data,
		RangeID:  rangeID,
	})
	if err != nil {
		return nil, fmt.Errorf("objstore: marshal shard envelope: %w", err)
	}
	_, err = s.blob.Put(ctx, key, body, blob.PutOptions{
		ContentType: "application/json",
		IfNoneMatch: "*",
	})
	if errors.Is(err, blob.ErrPreconditionFailed) {
		// Lost the create race — read the winner's value.
		request.CreateShardInfo = nil
		return s.GetOrCreateShard(ctx, request)
	}
	if err != nil {
		return nil, fmt.Errorf("objstore: shard create %d: %w", request.ShardID, err)
	}
	return &persistence.InternalGetOrCreateShardResponse{ShardInfo: blobInfo}, nil
}

// UpdateShard CAS-updates a shard. The predicate is twofold:
// (1) stored RangeID must equal request.PreviousRangeID; (2) the
// blob store's IfMatch must hold the ETag we read in step 1. The
// double predicate makes the failure mode obvious: domain mismatch
// vs concurrent writer.
func (s *shardStore) UpdateShard(
	ctx context.Context,
	request *persistence.InternalUpdateShardRequest,
) error {
	key := shardKey(s.clusterName, request.ShardID)

	env, etag, err := s.readEnvelope(ctx, key)
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return &persistence.ShardOwnershipLostError{
				ShardID: request.ShardID,
				Msg:     fmt.Sprintf("objstore: shard %d does not exist", request.ShardID),
			}
		}
		return fmt.Errorf("objstore: shard read for update %d: %w", request.ShardID, err)
	}
	if env.RangeID != request.PreviousRangeID {
		return &persistence.ShardOwnershipLostError{
			ShardID: request.ShardID,
			Msg: fmt.Sprintf("objstore: shard %d rangeID mismatch (stored=%d, expected=%d)",
				request.ShardID, env.RangeID, request.PreviousRangeID),
		}
	}

	body, err := json.Marshal(shardEnvelope{
		Encoding: request.ShardInfo.EncodingType.String(),
		Data:     request.ShardInfo.Data,
		RangeID:  request.RangeID,
	})
	if err != nil {
		return fmt.Errorf("objstore: marshal shard envelope: %w", err)
	}
	_, err = s.blob.Put(ctx, key, body, blob.PutOptions{
		ContentType: "application/json",
		IfMatch:     etag,
	})
	if errors.Is(err, blob.ErrPreconditionFailed) {
		return &persistence.ShardOwnershipLostError{
			ShardID: request.ShardID,
			Msg:     fmt.Sprintf("objstore: shard %d concurrent writer (etag CAS failed)", request.ShardID),
		}
	}
	if err != nil {
		return fmt.Errorf("objstore: shard update %d: %w", request.ShardID, err)
	}
	return nil
}

// AssertShardOwnership is a no-op for objstore — same as cassandra.
// The RangeID check in UpdateShard is the load-bearing invariant.
func (s *shardStore) AssertShardOwnership(
	_ context.Context,
	_ *persistence.AssertShardOwnershipRequest,
) error {
	return nil
}

// readEnvelope fetches + decodes a shard envelope at key,
// returning the parsed envelope and the underlying blob ETag for
// downstream CAS.
func (s *shardStore) readEnvelope(ctx context.Context, key string) (shardEnvelope, string, error) {
	res, err := s.blob.Get(ctx, key)
	if err != nil {
		return shardEnvelope{}, "", err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return shardEnvelope{}, "", fmt.Errorf("read body: %w", err)
	}
	var env shardEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return shardEnvelope{}, "", fmt.Errorf("unmarshal envelope: %w", err)
	}
	return env, res.ETag, nil
}

// envelopeToBlob rebuilds the *commonpb.DataBlob the persistence
// layer expects. Encoding strings come from EncodingType.String();
// the enums package's _value map is the inverse.
func envelopeToBlob(env shardEnvelope) *commonpb.DataBlob {
	enc := enumspb.EncodingType_value[env.Encoding]
	return &commonpb.DataBlob{
		EncodingType: enumspb.EncodingType(enc),
		Data:         env.Data,
	}
}
