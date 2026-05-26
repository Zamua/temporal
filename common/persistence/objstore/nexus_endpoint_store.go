package objstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/objstore/blob"
)

// nexusEndpointStore implements [persistence.NexusEndpointStore]:
//
//	nexus/endpoints/{endpointID}                → JSON nexusEndpointEnv
//	nexus/table-version                         → JSON nexusTableVersionEnv
//
// CreateOrUpdate uses table-level CAS — request.LastKnownTableVersion
// must match the stored table version; on success the table version
// bumps by 1.
type nexusEndpointStore struct {
	blob blob.Store
}

func newNexusEndpointStore(b blob.Store) persistence.NexusEndpointStore {
	return &nexusEndpointStore{blob: b}
}

func (n *nexusEndpointStore) GetName() string { return objstoreName }
func (n *nexusEndpointStore) Close()          {}

type nexusEndpointEnv struct {
	ID      string   `json:"id"`
	Version int64    `json:"v"`
	Data    *blobEnv `json:"d,omitempty"`
}

type nexusTableVersionEnv struct {
	Version int64 `json:"v"`
}

const nexusTableVersionKey = "nexus/table-version"

func nexusEndpointKey(id string) string {
	return "nexus/endpoints/" + id
}

func (n *nexusEndpointStore) CreateOrUpdateNexusEndpoint(ctx context.Context, request *persistence.InternalCreateOrUpdateNexusEndpointRequest) error {
	// CAS table version.
	tableVer, tableEtag, err := n.readTableVersion(ctx)
	if err != nil && !errors.Is(err, blob.ErrNotFound) {
		return fmt.Errorf("objstore: read nexus table version: %w", err)
	}
	if tableVer != request.LastKnownTableVersion {
		return &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("objstore: nexus table version mismatch (stored=%d, expected=%d)", tableVer, request.LastKnownTableVersion),
		}
	}

	endpoint := nexusEndpointEnv{
		ID:      request.Endpoint.ID,
		Version: request.Endpoint.Version,
		Data:    blobToEnv(request.Endpoint.Data),
	}
	endpointBody, err := json.Marshal(endpoint)
	if err != nil {
		return fmt.Errorf("marshal endpoint: %w", err)
	}
	if _, err := n.blob.Put(ctx, nexusEndpointKey(request.Endpoint.ID), endpointBody, blob.PutOptions{
		ContentType: "application/json",
	}); err != nil {
		return fmt.Errorf("objstore: put endpoint: %w", err)
	}

	// Bump table version via CAS.
	newVer := tableVer + 1
	verBody, _ := json.Marshal(nexusTableVersionEnv{Version: newVer})
	opts := blob.PutOptions{ContentType: "application/json"}
	if tableEtag != "" {
		opts.IfMatch = tableEtag
	} else {
		opts.IfNoneMatch = "*"
	}
	if _, err := n.blob.Put(ctx, nexusTableVersionKey, verBody, opts); err != nil {
		return fmt.Errorf("objstore: bump nexus table version: %w", err)
	}
	return nil
}

func (n *nexusEndpointStore) DeleteNexusEndpoint(ctx context.Context, request *persistence.DeleteNexusEndpointRequest) error {
	tableVer, tableEtag, err := n.readTableVersion(ctx)
	if err != nil && !errors.Is(err, blob.ErrNotFound) {
		return fmt.Errorf("objstore: read nexus table version: %w", err)
	}
	if tableVer != request.LastKnownTableVersion {
		return &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("objstore: nexus table version mismatch (stored=%d, expected=%d)", tableVer, request.LastKnownTableVersion),
		}
	}
	if err := n.blob.Delete(ctx, nexusEndpointKey(request.ID), blob.DeleteOptions{}); err != nil {
		return fmt.Errorf("objstore: delete endpoint: %w", err)
	}
	newVer := tableVer + 1
	verBody, _ := json.Marshal(nexusTableVersionEnv{Version: newVer})
	opts := blob.PutOptions{ContentType: "application/json"}
	if tableEtag != "" {
		opts.IfMatch = tableEtag
	} else {
		opts.IfNoneMatch = "*"
	}
	if _, err := n.blob.Put(ctx, nexusTableVersionKey, verBody, opts); err != nil {
		return fmt.Errorf("objstore: bump nexus table version: %w", err)
	}
	return nil
}

func (n *nexusEndpointStore) GetNexusEndpoint(ctx context.Context, request *persistence.GetNexusEndpointRequest) (*persistence.InternalNexusEndpoint, error) {
	body, err := readBlobBody(ctx, n.blob, nexusEndpointKey(request.ID))
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return nil, serviceerror.NewNotFoundf("objstore: nexus endpoint %q not found", request.ID)
		}
		return nil, fmt.Errorf("objstore: get nexus endpoint: %w", err)
	}
	var env nexusEndpointEnv
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("unmarshal endpoint: %w", err)
	}
	return &persistence.InternalNexusEndpoint{
		ID:      env.ID,
		Version: env.Version,
		Data:    envToBlob(env.Data),
	}, nil
}

func (n *nexusEndpointStore) ListNexusEndpoints(ctx context.Context, request *persistence.ListNexusEndpointsRequest) (*persistence.InternalListNexusEndpointsResponse, error) {
	tableVer, _, err := n.readTableVersion(ctx)
	if err != nil && !errors.Is(err, blob.ErrNotFound) {
		return nil, fmt.Errorf("objstore: read nexus table version: %w", err)
	}
	if request.LastKnownTableVersion != 0 && tableVer != request.LastKnownTableVersion {
		return nil, &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("objstore: nexus table version mismatch (stored=%d, expected=%d)", tableVer, request.LastKnownTableVersion),
		}
	}
	infos, err := n.blob.List(ctx, "nexus/endpoints/")
	if err != nil {
		return nil, fmt.Errorf("objstore: list endpoints: %w", err)
	}
	out := make([]persistence.InternalNexusEndpoint, 0, len(infos))
	for _, info := range infos {
		body, err := readBlobBody(ctx, n.blob, info.Key)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", info.Key, err)
		}
		var env nexusEndpointEnv
		if err := json.Unmarshal(body, &env); err != nil {
			return nil, fmt.Errorf("unmarshal: %w", err)
		}
		out = append(out, persistence.InternalNexusEndpoint{
			ID:      env.ID,
			Version: env.Version,
			Data:    envToBlob(env.Data),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return &persistence.InternalListNexusEndpointsResponse{
		TableVersion: tableVer,
		Endpoints:    out,
	}, nil
}

func (n *nexusEndpointStore) readTableVersion(ctx context.Context) (int64, string, error) {
	res, err := n.blob.Get(ctx, nexusTableVersionKey)
	if err != nil {
		return 0, "", err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return 0, "", err
	}
	var env nexusTableVersionEnv
	if err := json.Unmarshal(body, &env); err != nil {
		return 0, "", fmt.Errorf("unmarshal table version: %w", err)
	}
	return env.Version, res.ETag, nil
}
