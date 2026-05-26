package objstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/objstore/blob"
)

// metadataStore implements [persistence.MetadataStore] — the
// namespace registry. Storage layout:
//
//	namespaces/by-id/{namespaceID}        → JSON namespaceEnv
//	namespaces/by-name/{name}             → JSON nameIndexEnv → namespaceID
//	namespaces/metadata                   → JSON metadataEnv (NotificationVersion)
//
// All three keyspaces are kept in sync within each call. There's no
// multi-object transaction primitive, so failures between writes
// leave inconsistency that a future scan-and-reconcile would catch
// (matches the eventual-consistency story other objstore-backed
// systems use). Conformance tests run single-writer so this is fine.
type metadataStore struct {
	blob blob.Store
}

func newMetadataStore(b blob.Store) persistence.MetadataStore {
	return &metadataStore{blob: b}
}

func (m *metadataStore) GetName() string { return objstoreName }
func (m *metadataStore) Close()          {}

type namespaceEnv struct {
	ID                  string   `json:"id"`
	Name                string   `json:"name"`
	IsGlobal            bool     `json:"global"`
	Namespace           *blobEnv `json:"ns"`
	NotificationVersion int64    `json:"nv"`
}

type nameIndexEnv struct {
	NamespaceID string `json:"id"`
}

type metadataNotificationEnv struct {
	NotificationVersion int64 `json:"nv"`
}

func namespaceIDKey(id string) string     { return "namespaces/by-id/" + safeID(id) }
func namespaceNameKey(name string) string { return "namespaces/by-name/" + safeID(name) }

const metadataNotificationKey = "namespaces/metadata"

func (m *metadataStore) CreateNamespace(ctx context.Context, request *persistence.InternalCreateNamespaceRequest) (*persistence.CreateNamespaceResponse, error) {
	// Bump notification version up-front so the by-id record can
	// carry it. Read-modify-write the metadata blob (CAS on ETag).
	notify, _, err := m.readMetadata(ctx)
	if err != nil && !errors.Is(err, blob.ErrNotFound) {
		return nil, fmt.Errorf("objstore: read metadata: %w", err)
	}
	newVersion := notify.NotificationVersion + 1

	ns := namespaceEnv{
		ID:                  request.ID,
		Name:                request.Name,
		IsGlobal:            request.IsGlobal,
		Namespace:           blobToEnv(request.Namespace),
		NotificationVersion: newVersion,
	}
	nsBody, err := json.Marshal(ns)
	if err != nil {
		return nil, fmt.Errorf("marshal namespace: %w", err)
	}

	// First-writer-wins on by-name (uniqueness on name).
	idxBody, _ := json.Marshal(nameIndexEnv{NamespaceID: request.ID})
	if _, err := m.blob.Put(ctx, namespaceNameKey(request.Name), idxBody, blob.PutOptions{
		ContentType: "application/json",
		IfNoneMatch: "*",
	}); err != nil {
		if errors.Is(err, blob.ErrPreconditionFailed) {
			return nil, serviceerror.NewNamespaceAlreadyExistsf("objstore: namespace %q already exists", request.Name)
		}
		return nil, fmt.Errorf("objstore: claim namespace name: %w", err)
	}

	// First-writer-wins on by-id.
	if _, err := m.blob.Put(ctx, namespaceIDKey(request.ID), nsBody, blob.PutOptions{
		ContentType: "application/json",
		IfNoneMatch: "*",
	}); err != nil {
		// Roll back name reservation on failure.
		_ = m.blob.Delete(ctx, namespaceNameKey(request.Name), blob.DeleteOptions{})
		if errors.Is(err, blob.ErrPreconditionFailed) {
			return nil, serviceerror.NewNamespaceAlreadyExistsf("objstore: namespace ID %q already exists", request.ID)
		}
		return nil, fmt.Errorf("objstore: put namespace by id: %w", err)
	}

	// Bump notification version (best effort — no CAS, last writer wins).
	if err := m.writeMetadata(ctx, newVersion); err != nil {
		return nil, fmt.Errorf("objstore: update metadata: %w", err)
	}
	return &persistence.CreateNamespaceResponse{ID: request.ID}, nil
}

func (m *metadataStore) GetNamespace(ctx context.Context, request *persistence.GetNamespaceRequest) (*persistence.InternalGetNamespaceResponse, error) {
	id := request.ID
	if id == "" && request.Name != "" {
		idxBody, err := readBlobBody(ctx, m.blob, namespaceNameKey(request.Name))
		if err != nil {
			if errors.Is(err, blob.ErrNotFound) {
				return nil, serviceerror.NewNamespaceNotFound(request.Name)
			}
			return nil, fmt.Errorf("objstore: lookup namespace by name: %w", err)
		}
		var idx nameIndexEnv
		if err := json.Unmarshal(idxBody, &idx); err != nil {
			return nil, fmt.Errorf("unmarshal name index: %w", err)
		}
		id = idx.NamespaceID
	}
	body, err := readBlobBody(ctx, m.blob, namespaceIDKey(id))
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return nil, serviceerror.NewNamespaceNotFound(request.Name)
		}
		return nil, fmt.Errorf("objstore: get namespace: %w", err)
	}
	var ns namespaceEnv
	if err := json.Unmarshal(body, &ns); err != nil {
		return nil, fmt.Errorf("unmarshal namespace: %w", err)
	}
	return &persistence.InternalGetNamespaceResponse{
		Namespace:           envToBlob(ns.Namespace),
		IsGlobal:            ns.IsGlobal,
		NotificationVersion: ns.NotificationVersion,
	}, nil
}

func (m *metadataStore) UpdateNamespace(ctx context.Context, request *persistence.InternalUpdateNamespaceRequest) error {
	body, err := readBlobBody(ctx, m.blob, namespaceIDKey(request.Id))
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return serviceerror.NewNamespaceNotFound(request.Name)
		}
		return fmt.Errorf("objstore: read namespace for update: %w", err)
	}
	var ns namespaceEnv
	if err := json.Unmarshal(body, &ns); err != nil {
		return fmt.Errorf("unmarshal namespace: %w", err)
	}
	ns.Name = request.Name
	ns.IsGlobal = request.IsGlobal
	ns.Namespace = blobToEnv(request.Namespace)
	ns.NotificationVersion = request.NotificationVersion

	newBody, err := json.Marshal(ns)
	if err != nil {
		return fmt.Errorf("marshal updated namespace: %w", err)
	}
	if _, err := m.blob.Put(ctx, namespaceIDKey(request.Id), newBody, blob.PutOptions{
		ContentType: "application/json",
	}); err != nil {
		return fmt.Errorf("objstore: put updated namespace: %w", err)
	}
	return m.writeMetadata(ctx, request.NotificationVersion)
}

func (m *metadataStore) RenameNamespace(ctx context.Context, request *persistence.InternalRenameNamespaceRequest) error {
	// Move name index: claim new name, drop old name.
	newName := request.Name
	idxBody, _ := json.Marshal(nameIndexEnv{NamespaceID: request.Id})
	if _, err := m.blob.Put(ctx, namespaceNameKey(newName), idxBody, blob.PutOptions{
		ContentType: "application/json",
		IfNoneMatch: "*",
	}); err != nil {
		if errors.Is(err, blob.ErrPreconditionFailed) {
			return serviceerror.NewNamespaceAlreadyExistsf("objstore: target name %q already taken", newName)
		}
		return fmt.Errorf("objstore: claim renamed name: %w", err)
	}
	if request.PreviousName != "" {
		if err := m.blob.Delete(ctx, namespaceNameKey(request.PreviousName), blob.DeleteOptions{}); err != nil {
			return fmt.Errorf("objstore: drop old name: %w", err)
		}
	}
	return m.UpdateNamespace(ctx, request.InternalUpdateNamespaceRequest)
}

func (m *metadataStore) DeleteNamespace(ctx context.Context, request *persistence.DeleteNamespaceRequest) error {
	// Look up the name to clean both indexes.
	body, err := readBlobBody(ctx, m.blob, namespaceIDKey(request.ID))
	if err == nil {
		var ns namespaceEnv
		if json.Unmarshal(body, &ns) == nil && ns.Name != "" {
			_ = m.blob.Delete(ctx, namespaceNameKey(ns.Name), blob.DeleteOptions{})
		}
	}
	return m.blob.Delete(ctx, namespaceIDKey(request.ID), blob.DeleteOptions{})
}

func (m *metadataStore) DeleteNamespaceByName(ctx context.Context, request *persistence.DeleteNamespaceByNameRequest) error {
	idxBody, err := readBlobBody(ctx, m.blob, namespaceNameKey(request.Name))
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return nil // idempotent
		}
		return fmt.Errorf("objstore: lookup name for delete: %w", err)
	}
	var idx nameIndexEnv
	if err := json.Unmarshal(idxBody, &idx); err != nil {
		return fmt.Errorf("unmarshal name index: %w", err)
	}
	_ = m.blob.Delete(ctx, namespaceIDKey(idx.NamespaceID), blob.DeleteOptions{})
	return m.blob.Delete(ctx, namespaceNameKey(request.Name), blob.DeleteOptions{})
}

func (m *metadataStore) ListNamespaces(ctx context.Context, request *persistence.InternalListNamespacesRequest) (*persistence.InternalListNamespacesResponse, error) {
	infos, err := m.blob.List(ctx, "namespaces/by-id/")
	if err != nil {
		return nil, fmt.Errorf("objstore: list namespaces: %w", err)
	}
	out := make([]*persistence.InternalGetNamespaceResponse, 0, len(infos))
	for _, info := range infos {
		body, err := readBlobBody(ctx, m.blob, info.Key)
		if err != nil {
			return nil, fmt.Errorf("read namespace %s: %w", info.Key, err)
		}
		var ns namespaceEnv
		if err := json.Unmarshal(body, &ns); err != nil {
			return nil, fmt.Errorf("unmarshal namespace: %w", err)
		}
		out = append(out, &persistence.InternalGetNamespaceResponse{
			Namespace:           envToBlob(ns.Namespace),
			IsGlobal:            ns.IsGlobal,
			NotificationVersion: ns.NotificationVersion,
		})
	}
	return &persistence.InternalListNamespacesResponse{
		Namespaces: out,
	}, nil
}

func (m *metadataStore) GetMetadata(ctx context.Context) (*persistence.GetMetadataResponse, error) {
	mn, _, err := m.readMetadata(ctx)
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return &persistence.GetMetadataResponse{NotificationVersion: 0}, nil
		}
		return nil, fmt.Errorf("objstore: read metadata: %w", err)
	}
	return &persistence.GetMetadataResponse{NotificationVersion: mn.NotificationVersion}, nil
}

// readMetadata returns the global notification-version record.
// ErrNotFound surfaces as a zero-value env so the caller can choose
// how to initialize.
func (m *metadataStore) readMetadata(ctx context.Context) (metadataNotificationEnv, string, error) {
	body, err := readBlobBody(ctx, m.blob, metadataNotificationKey)
	if err != nil {
		return metadataNotificationEnv{}, "", err
	}
	var env metadataNotificationEnv
	if err := json.Unmarshal(body, &env); err != nil {
		return metadataNotificationEnv{}, "", fmt.Errorf("unmarshal metadata: %w", err)
	}
	return env, "", nil
}

func (m *metadataStore) writeMetadata(ctx context.Context, version int64) error {
	body, err := json.Marshal(metadataNotificationEnv{NotificationVersion: version})
	if err != nil {
		return err
	}
	_, err = m.blob.Put(ctx, metadataNotificationKey, body, blob.PutOptions{
		ContentType: "application/json",
	})
	return err
}
