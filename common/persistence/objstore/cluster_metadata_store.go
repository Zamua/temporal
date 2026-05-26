package objstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/objstore/blob"
)

// clusterMetadataStore implements [persistence.ClusterMetadataStore]:
//
//	cluster-metadata/{clusterName}                  → JSON clusterMetadataEnv
//	cluster-members/by-id/{hostIDHex}               → JSON clusterMemberEnv
//
// Cluster metadata supports CAS via the Version field — a write
// only succeeds if request.Version+1 == stored Version+1 (i.e. the
// caller saw the prior version). We back this with IfMatch on the
// blob ETag for the same effect.
type clusterMetadataStore struct {
	blob blob.Store
}

func newClusterMetadataStore(b blob.Store) persistence.ClusterMetadataStore {
	return &clusterMetadataStore{blob: b}
}

func (c *clusterMetadataStore) GetName() string { return objstoreName }
func (c *clusterMetadataStore) Close()          {}

type clusterMetadataEnv struct {
	ClusterName string   `json:"name"`
	Metadata    *blobEnv `json:"md,omitempty"`
	Version     int64    `json:"v"`
}

type clusterMemberEnv struct {
	HostID        []byte    `json:"id"`
	Role          int32     `json:"role"`
	RPCAddress    string    `json:"addr"`
	RPCPort       uint16    `json:"port"`
	SessionStart  time.Time `json:"start"`
	LastHeartbeat time.Time `json:"hb"`
	RecordExpiry  time.Time `json:"exp"`
}

func clusterMetadataKey(name string) string { return "cluster-metadata/" + safeID(name) }
func clusterMemberKey(hostID []byte) string {
	return "cluster-members/by-id/" + hex.EncodeToString(hostID)
}

func (c *clusterMetadataStore) ListClusterMetadata(ctx context.Context, _ *persistence.InternalListClusterMetadataRequest) (*persistence.InternalListClusterMetadataResponse, error) {
	infos, err := c.blob.List(ctx, "cluster-metadata/")
	if err != nil {
		return nil, fmt.Errorf("objstore: list cluster metadata: %w", err)
	}
	out := make([]*persistence.InternalGetClusterMetadataResponse, 0, len(infos))
	for _, info := range infos {
		body, err := readBlobBody(ctx, c.blob, info.Key)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", info.Key, err)
		}
		var env clusterMetadataEnv
		if err := json.Unmarshal(body, &env); err != nil {
			return nil, fmt.Errorf("unmarshal: %w", err)
		}
		out = append(out, &persistence.InternalGetClusterMetadataResponse{
			ClusterMetadata: envToBlob(env.Metadata),
			Version:         env.Version,
		})
	}
	return &persistence.InternalListClusterMetadataResponse{ClusterMetadata: out}, nil
}

func (c *clusterMetadataStore) GetClusterMetadata(ctx context.Context, request *persistence.InternalGetClusterMetadataRequest) (*persistence.InternalGetClusterMetadataResponse, error) {
	body, err := readBlobBody(ctx, c.blob, clusterMetadataKey(request.ClusterName))
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return nil, serviceerror.NewNotFoundf("objstore: cluster %q not found", request.ClusterName)
		}
		return nil, fmt.Errorf("objstore: get cluster metadata: %w", err)
	}
	var env clusterMetadataEnv
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("unmarshal cluster metadata: %w", err)
	}
	return &persistence.InternalGetClusterMetadataResponse{
		ClusterMetadata: envToBlob(env.Metadata),
		Version:         env.Version,
	}, nil
}

func (c *clusterMetadataStore) SaveClusterMetadata(ctx context.Context, request *persistence.InternalSaveClusterMetadataRequest) (bool, error) {
	key := clusterMetadataKey(request.ClusterName)
	body, etag, err := c.readClusterMetadata(ctx, request.ClusterName)
	if err != nil && !errors.Is(err, blob.ErrNotFound) {
		return false, fmt.Errorf("objstore: read for save: %w", err)
	}
	storedVersion := int64(0)
	if body != nil {
		storedVersion = body.Version
	}
	if storedVersion != request.Version {
		// CAS predicate: caller's Version must match stored. If not,
		// the caller has stale state.
		return false, nil
	}

	newEnv := clusterMetadataEnv{
		ClusterName: request.ClusterName,
		Metadata:    blobToEnv(request.ClusterMetadata),
		Version:     request.Version + 1,
	}
	newBody, err := json.Marshal(newEnv)
	if err != nil {
		return false, fmt.Errorf("marshal cluster metadata: %w", err)
	}
	opts := blob.PutOptions{ContentType: "application/json"}
	if etag != "" {
		opts.IfMatch = etag
	} else {
		opts.IfNoneMatch = "*"
	}
	if _, err := c.blob.Put(ctx, key, newBody, opts); err != nil {
		if errors.Is(err, blob.ErrPreconditionFailed) {
			return false, nil
		}
		return false, fmt.Errorf("objstore: save cluster metadata: %w", err)
	}
	return true, nil
}

func (c *clusterMetadataStore) DeleteClusterMetadata(ctx context.Context, request *persistence.InternalDeleteClusterMetadataRequest) error {
	return c.blob.Delete(ctx, clusterMetadataKey(request.ClusterName), blob.DeleteOptions{})
}

func (c *clusterMetadataStore) readClusterMetadata(ctx context.Context, name string) (*clusterMetadataEnv, string, error) {
	res, err := c.blob.Get(ctx, clusterMetadataKey(name))
	if err != nil {
		return nil, "", err
	}
	defer res.Body.Close()
	body := bytes.Buffer{}
	if _, err := body.ReadFrom(res.Body); err != nil {
		return nil, "", err
	}
	var env clusterMetadataEnv
	if err := json.Unmarshal(body.Bytes(), &env); err != nil {
		return nil, "", fmt.Errorf("unmarshal cluster metadata: %w", err)
	}
	return &env, res.ETag, nil
}

func (c *clusterMetadataStore) GetClusterMembers(ctx context.Context, request *persistence.GetClusterMembersRequest) (*persistence.GetClusterMembersResponse, error) {
	infos, err := c.blob.List(ctx, "cluster-members/by-id/")
	if err != nil {
		return nil, fmt.Errorf("objstore: list cluster members: %w", err)
	}
	out := make([]*persistence.ClusterMember, 0, len(infos))
	for _, info := range infos {
		body, err := readBlobBody(ctx, c.blob, info.Key)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", info.Key, err)
		}
		var env clusterMemberEnv
		if err := json.Unmarshal(body, &env); err != nil {
			return nil, fmt.Errorf("unmarshal: %w", err)
		}
		// Expiry purge — drop stale rows on read instead of running
		// an explicit GC task. Matches cassandra TTL behavior.
		if !env.RecordExpiry.IsZero() && env.RecordExpiry.Before(time.Now()) {
			continue
		}
		// Heartbeat filter.
		if request.LastHeartbeatWithin > 0 && time.Since(env.LastHeartbeat) > request.LastHeartbeatWithin {
			continue
		}
		// Role filter.
		if request.RoleEquals != 0 && persistence.ServiceType(env.Role) != request.RoleEquals {
			continue
		}
		if len(request.HostIDEquals) > 0 && !bytes.Equal(env.HostID, request.HostIDEquals) {
			continue
		}
		if request.RPCAddressEquals != nil && net.ParseIP(env.RPCAddress).Equal(request.RPCAddressEquals) == false {
			continue
		}
		if !request.SessionStartedAfter.IsZero() && !env.SessionStart.After(request.SessionStartedAfter) {
			continue
		}
		out = append(out, &persistence.ClusterMember{
			Role:          persistence.ServiceType(env.Role),
			HostID:        env.HostID,
			RPCAddress:    net.ParseIP(env.RPCAddress),
			RPCPort:       env.RPCPort,
			SessionStart:  env.SessionStart,
			LastHeartbeat: env.LastHeartbeat,
			RecordExpiry:  env.RecordExpiry,
		})
	}
	// Stable order by HostID for determinism.
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].HostID, out[j].HostID) < 0 })
	return &persistence.GetClusterMembersResponse{ActiveMembers: out}, nil
}

func (c *clusterMetadataStore) UpsertClusterMembership(ctx context.Context, request *persistence.UpsertClusterMembershipRequest) error {
	env := clusterMemberEnv{
		HostID:        request.HostID,
		Role:          int32(request.Role),
		RPCAddress:    request.RPCAddress.String(),
		RPCPort:       request.RPCPort,
		SessionStart:  request.SessionStart,
		LastHeartbeat: time.Now(),
		RecordExpiry:  time.Now().Add(request.RecordExpiry),
	}
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	_, err = c.blob.Put(ctx, clusterMemberKey(request.HostID), body, blob.PutOptions{
		ContentType: "application/json",
	})
	return err
}

func (c *clusterMetadataStore) PruneClusterMembership(ctx context.Context, request *persistence.PruneClusterMembershipRequest) error {
	infos, err := c.blob.List(ctx, "cluster-members/by-id/")
	if err != nil {
		return fmt.Errorf("objstore: list members for prune: %w", err)
	}
	pruned := 0
	for _, info := range infos {
		body, err := readBlobBody(ctx, c.blob, info.Key)
		if err != nil {
			continue
		}
		var env clusterMemberEnv
		if err := json.Unmarshal(body, &env); err != nil {
			continue
		}
		if !env.RecordExpiry.IsZero() && env.RecordExpiry.Before(time.Now()) {
			_ = c.blob.Delete(ctx, info.Key, blob.DeleteOptions{})
			pruned++
			if request.MaxRecordsPruned > 0 && pruned >= request.MaxRecordsPruned {
				break
			}
		}
	}
	return nil
}
