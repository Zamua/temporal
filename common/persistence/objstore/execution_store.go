package objstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/objstore/blob"
	"go.temporal.io/server/common/persistence/serialization"
)

// executionStore implements [persistence.ExecutionStore] on top of
// a [blob.Store]. Storage layout (ported from driftwood, see
// driftwood/internal/repository/keys.go):
//
//	executions/{namespace}/{workflowID}/current_run
//	  → JSON pointer object: {"run_id": "...", "exec_state_blob": "..."}.
//	    Updated via blob IfMatch on its ETag for CAS semantics. Reads
//	    serve GetCurrentExecution.
//
//	executions/{namespace}/{workflowID}/runs/{runID}/snapshot
//	  → JSON envelope wrapping the InternalWorkflowSnapshot we received
//	    on Create. UpdateWorkflowExecution does CAS read-modify-write:
//	    read snapshot + ETag, apply the InternalWorkflowMutation's
//	    Upsert*/Delete* deltas, write back via IfMatch on the ETag.
//	    Concurrent writers race for the ETag and the loser sees
//	    ErrPreconditionFailed → persistence.ConditionFailedError.
//
//	executions/{namespace}/{workflowID}/runs/{runID}/segments/{startID:020d}-{endID:020d}
//	  → Append-once event segments (immutable). IfNoneMatch=* on write.
//	    Range reads via blob.List with the segments/ prefix; the
//	    20-digit zero-padded ID encoding makes lexicographic ordering
//	    match event ordering. (See history_branch_store.go — that's
//	    task #288's job, not #282.)
//
// Mode-specific create rules:
//
//   - CreateWorkflowMode_BypassCurrent: write snapshot only,
//     IfNoneMatch=*. Conflict → ConditionFailedError.
//   - CreateWorkflowMode_BrandNew: write snapshot AND current_run,
//     both IfNoneMatch=*. The current_run write races against itself
//     — if the workflow already exists we surface
//     WorkflowConditionFailedError with the existing RunID.
//   - CreateWorkflowMode_UpdateCurrent: snapshot is IfNoneMatch=*,
//     current_run is IfMatch on the previous pointer's ETag. The
//     caller verifies PreviousRunID matches what they read.
//
// Per-method docstrings explain the exact CAS chain each one runs.
type executionStore struct {
	blob        blob.Store
	clusterName string
	serializer  serialization.Serializer
}

func newExecutionStore(b blob.Store, clusterName string, serializer serialization.Serializer) persistence.ExecutionStore {
	return &executionStore{
		blob:        b,
		clusterName: clusterName,
		serializer:  serializer,
	}
}

func (e *executionStore) GetName() string { return objstoreName }
func (e *executionStore) Close()           {}

// GetHistoryBranchUtil returns the helper Temporal uses to mint
// branch tokens for history V2. We use the persistence package's
// default implementation — branch IDs are opaque to objstore.
func (e *executionStore) GetHistoryBranchUtil() persistence.HistoryBranchUtil {
	return &persistence.HistoryBranchUtilImpl{}
}

// --- key layout helpers ---

func executionCurrentRunKey(namespaceID, workflowID string) string {
	return fmt.Sprintf("executions/%s/%s/current_run", namespaceID, workflowID)
}

func executionSnapshotKey(namespaceID, workflowID, runID string) string {
	return fmt.Sprintf("executions/%s/%s/runs/%s/snapshot", namespaceID, workflowID, runID)
}

// --- workflow execution lifecycle (Create / Get / Update / Set / Delete) ---
// Implementations track in execution_store_lifecycle.go. Methods
// below are wired stubs; landing each one is its own slice of #282.

func (e *executionStore) CreateWorkflowExecution(
	ctx context.Context,
	request *persistence.InternalCreateWorkflowExecutionRequest,
) (*persistence.InternalCreateWorkflowExecutionResponse, error) {
	snap := request.NewWorkflowSnapshot
	env := snapshotToEnv(&snap)
	env.UpdatedAt = time.Now().UnixNano()
	body, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("objstore: marshal create snapshot: %w", err)
	}

	snapKey := executionSnapshotKey(snap.NamespaceID, snap.WorkflowID, snap.RunID)

	switch request.Mode {
	case persistence.CreateWorkflowModeBypassCurrent:
		// Write the run's snapshot, fail-if-exists.
		_, err = e.blob.Put(ctx, snapKey, body, blob.PutOptions{
			ContentType: "application/json",
			IfNoneMatch: "*",
		})
		if errors.Is(err, blob.ErrPreconditionFailed) {
			return nil, &persistence.WorkflowConditionFailedError{
				Msg:             fmt.Sprintf("objstore: workflow %s/%s/%s already exists", snap.NamespaceID, snap.WorkflowID, snap.RunID),
				NextEventID:     snap.NextEventID,
				DBRecordVersion: snap.DBRecordVersion,
			}
		}
		if err != nil {
			return nil, fmt.Errorf("objstore: put snapshot: %w", err)
		}

	case persistence.CreateWorkflowModeBrandNew:
		// Pointer-swap: first create the current_run pointer (fail-if-exists),
		// then write the snapshot. If current_run already exists, surface
		// the existing run's state so the caller can react.
		if err := e.createCurrentRun(ctx, &snap); err != nil {
			return nil, err
		}
		if _, err = e.blob.Put(ctx, snapKey, body, blob.PutOptions{
			ContentType: "application/json",
			IfNoneMatch: "*",
		}); err != nil {
			return nil, fmt.Errorf("objstore: put snapshot after current_run claim: %w", err)
		}

	case persistence.CreateWorkflowModeUpdateCurrent:
		// CAS the current_run pointer: it must currently point at
		// request.PreviousRunID (any other value → conflict).
		if err := e.updateCurrentRun(ctx, &snap, request.PreviousRunID, request.PreviousLastWriteVersion); err != nil {
			return nil, err
		}
		if _, err = e.blob.Put(ctx, snapKey, body, blob.PutOptions{
			ContentType: "application/json",
			IfNoneMatch: "*",
		}); err != nil {
			return nil, fmt.Errorf("objstore: put snapshot after current_run update: %w", err)
		}

	default:
		return nil, serviceerror.NewInternalf("objstore: CreateWorkflowExecution: unknown mode: %v", request.Mode)
	}

	return &persistence.InternalCreateWorkflowExecutionResponse{}, nil
}

func (e *executionStore) UpdateWorkflowExecution(
	ctx context.Context,
	request *persistence.InternalUpdateWorkflowExecutionRequest,
) error {
	mutation := request.UpdateWorkflowMutation
	snapKey := executionSnapshotKey(mutation.NamespaceID, mutation.WorkflowID, mutation.RunID)

	env, etag, err := e.readSnapshot(ctx, snapKey)
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return &persistence.ConditionFailedError{
				Msg: fmt.Sprintf("objstore: workflow %s/%s/%s not found", mutation.NamespaceID, mutation.WorkflowID, mutation.RunID),
			}
		}
		return fmt.Errorf("objstore: read snapshot for update: %w", err)
	}

	// CAS predicate: stored DBRecordVersion must match the mutation's
	// Condition (the value the caller read on its last GetWorkflowExecution).
	if mutation.Condition != 0 && env.DBRecordVersion != mutation.Condition {
		return &persistence.WorkflowConditionFailedError{
			Msg:             fmt.Sprintf("objstore: DBRecordVersion mismatch (stored=%d, expected=%d)", env.DBRecordVersion, mutation.Condition),
			NextEventID:     env.NextEventID,
			DBRecordVersion: env.DBRecordVersion,
		}
	}

	applyMutation(env, &mutation)
	env.UpdatedAt = time.Now().UnixNano()
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("objstore: marshal updated snapshot: %w", err)
	}

	_, err = e.blob.Put(ctx, snapKey, body, blob.PutOptions{
		ContentType: "application/json",
		IfMatch:     etag,
	})
	if errors.Is(err, blob.ErrPreconditionFailed) {
		return &persistence.WorkflowConditionFailedError{
			Msg:             "objstore: concurrent writer (etag CAS failed)",
			NextEventID:     env.NextEventID,
			DBRecordVersion: env.DBRecordVersion,
		}
	}
	if err != nil {
		return fmt.Errorf("objstore: put updated snapshot: %w", err)
	}

	// Mode-specific: when this update is a continue-as-new, the
	// caller hands us a NewWorkflowSnapshot. Persist the new run's
	// snapshot and (for UpdateCurrent mode) swing current_run to it.
	if request.NewWorkflowSnapshot != nil {
		newSnap := *request.NewWorkflowSnapshot
		newEnv := snapshotToEnv(&newSnap)
		newEnv.UpdatedAt = time.Now().UnixNano()
		newBody, err := json.Marshal(newEnv)
		if err != nil {
			return fmt.Errorf("objstore: marshal new-run snapshot: %w", err)
		}
		newKey := executionSnapshotKey(newSnap.NamespaceID, newSnap.WorkflowID, newSnap.RunID)
		if _, err = e.blob.Put(ctx, newKey, newBody, blob.PutOptions{
			ContentType: "application/json",
			IfNoneMatch: "*",
		}); err != nil && !errors.Is(err, blob.ErrPreconditionFailed) {
			return fmt.Errorf("objstore: put new-run snapshot: %w", err)
		}
		if request.Mode == persistence.UpdateWorkflowModeUpdateCurrent {
			if err := e.updateCurrentRun(ctx, &newSnap, mutation.RunID, mutation.LastWriteVersion); err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *executionStore) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *persistence.InternalConflictResolveWorkflowExecutionRequest,
) error {
	_ = ctx
	_ = request
	return serviceerror.NewUnimplemented("objstore: ConflictResolveWorkflowExecution deferred — task #290")
}

func (e *executionStore) DeleteWorkflowExecution(
	ctx context.Context,
	request *persistence.DeleteWorkflowExecutionRequest,
) error {
	key := executionSnapshotKey(request.NamespaceID, request.WorkflowID, request.RunID)
	return e.blob.Delete(ctx, key, blob.DeleteOptions{})
}

func (e *executionStore) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *persistence.DeleteCurrentWorkflowExecutionRequest,
) error {
	// Delete is idempotent — repeated calls on a missing key return nil.
	key := executionCurrentRunKey(request.NamespaceID, request.WorkflowID)
	return e.blob.Delete(ctx, key, blob.DeleteOptions{})
}

func (e *executionStore) GetCurrentExecution(
	ctx context.Context,
	request *persistence.GetCurrentExecutionRequest,
) (*persistence.InternalGetCurrentExecutionResponse, error) {
	ptr, _, err := e.readCurrentRun(ctx, request.NamespaceID, request.WorkflowID)
	if err != nil {
		return nil, err
	}
	state, err := e.serializer.WorkflowExecutionStateFromBlob(envToBlob(ptr.ExecutionState))
	if err != nil {
		return nil, fmt.Errorf("objstore: decode execution state: %w", err)
	}
	return &persistence.InternalGetCurrentExecutionResponse{
		RunID:          ptr.RunID,
		ExecutionState: state,
	}, nil
}

func (e *executionStore) GetWorkflowExecution(
	ctx context.Context,
	request *persistence.GetWorkflowExecutionRequest,
) (*persistence.InternalGetWorkflowExecutionResponse, error) {
	key := executionSnapshotKey(request.NamespaceID, request.WorkflowID, request.RunID)
	env, _, err := e.readSnapshot(ctx, key)
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return nil, serviceerror.NewNotFoundf("objstore: workflow %s/%s/%s not found",
				request.NamespaceID, request.WorkflowID, request.RunID)
		}
		return nil, fmt.Errorf("objstore: read snapshot: %w", err)
	}
	return &persistence.InternalGetWorkflowExecutionResponse{
		State:           envToMutableState(env),
		DBRecordVersion: env.DBRecordVersion,
	}, nil
}

func (e *executionStore) SetWorkflowExecution(
	ctx context.Context,
	request *persistence.InternalSetWorkflowExecutionRequest,
) error {
	snap := request.SetWorkflowSnapshot
	env := snapshotToEnv(&snap)
	env.UpdatedAt = time.Now().UnixNano()
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("objstore: marshal set snapshot: %w", err)
	}
	key := executionSnapshotKey(snap.NamespaceID, snap.WorkflowID, snap.RunID)
	if _, err = e.blob.Put(ctx, key, body, blob.PutOptions{
		ContentType: "application/json",
	}); err != nil {
		return fmt.Errorf("objstore: put set snapshot: %w", err)
	}
	return nil
}

// --- helpers shared across lifecycle methods ---

// currentRunPtr is the on-disk shape of the `executions/.../current_run`
// pointer. We persist enough state to satisfy GetCurrentExecution
// without re-reading the run's snapshot.
type currentRunPtr struct {
	RunID            string   `json:"rid"`
	ExecutionState   *blobEnv `json:"es,omitempty"`
	LastWriteVersion int64    `json:"lwv,omitempty"`
	UpdatedAt        int64    `json:"ts,omitempty"`
}

// readSnapshot fetches + decodes a workflow snapshot envelope at
// the given key. ETag is returned for downstream CAS.
func (e *executionStore) readSnapshot(ctx context.Context, key string) (*workflowEnv, string, error) {
	res, err := e.blob.Get(ctx, key)
	if err != nil {
		return nil, "", err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, "", fmt.Errorf("read snapshot body: %w", err)
	}
	var env workflowEnv
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, "", fmt.Errorf("unmarshal snapshot: %w", err)
	}
	return &env, res.ETag, nil
}

// readCurrentRun returns the current_run pointer for (ns, workflowID)
// + the underlying blob ETag for CAS. ErrNotFound is wrapped into a
// NotFound serviceerror to match cassandra's behavior.
func (e *executionStore) readCurrentRun(ctx context.Context, namespaceID, workflowID string) (*currentRunPtr, string, error) {
	key := executionCurrentRunKey(namespaceID, workflowID)
	res, err := e.blob.Get(ctx, key)
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return nil, "", serviceerror.NewNotFoundf("objstore: workflow %s/%s has no current run", namespaceID, workflowID)
		}
		return nil, "", fmt.Errorf("objstore: get current_run: %w", err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, "", fmt.Errorf("read current_run body: %w", err)
	}
	var ptr currentRunPtr
	if err := json.Unmarshal(body, &ptr); err != nil {
		return nil, "", fmt.Errorf("unmarshal current_run: %w", err)
	}
	return &ptr, res.ETag, nil
}

// createCurrentRun writes the current_run pointer for a brand-new
// workflow. Fails with CurrentWorkflowConditionFailedError if a
// pointer already exists (some other run is currently active).
func (e *executionStore) createCurrentRun(ctx context.Context, snap *persistence.InternalWorkflowSnapshot) error {
	ptr := &currentRunPtr{
		RunID:            snap.RunID,
		ExecutionState:   blobToEnv(snap.ExecutionStateBlob),
		LastWriteVersion: snap.LastWriteVersion,
		UpdatedAt:        time.Now().UnixNano(),
	}
	body, err := json.Marshal(ptr)
	if err != nil {
		return fmt.Errorf("marshal current_run: %w", err)
	}
	key := executionCurrentRunKey(snap.NamespaceID, snap.WorkflowID)
	_, err = e.blob.Put(ctx, key, body, blob.PutOptions{
		ContentType: "application/json",
		IfNoneMatch: "*",
	})
	if errors.Is(err, blob.ErrPreconditionFailed) {
		// Someone else owns this workflow. Return the existing
		// pointer's runID so the caller can decide how to react.
		existing, _, readErr := e.readCurrentRun(ctx, snap.NamespaceID, snap.WorkflowID)
		conflict := &persistence.CurrentWorkflowConditionFailedError{
			Msg: fmt.Sprintf("objstore: workflow %s/%s already exists", snap.NamespaceID, snap.WorkflowID),
		}
		if readErr == nil && existing != nil {
			conflict.RunID = existing.RunID
			conflict.LastWriteVersion = existing.LastWriteVersion
		}
		return conflict
	}
	if err != nil {
		return fmt.Errorf("objstore: put current_run: %w", err)
	}
	return nil
}

// updateCurrentRun CAS-swings the current_run pointer from
// previousRunID to snap.RunID. Verifies both the stored runID
// matches expectations AND the ETag we observed matches at write
// time. Failure on either gives CurrentWorkflowConditionFailedError.
func (e *executionStore) updateCurrentRun(ctx context.Context, snap *persistence.InternalWorkflowSnapshot, previousRunID string, previousLastWriteVersion int64) error {
	existing, etag, err := e.readCurrentRun(ctx, snap.NamespaceID, snap.WorkflowID)
	if err != nil {
		return err
	}
	if existing.RunID != previousRunID {
		return &persistence.CurrentWorkflowConditionFailedError{
			Msg:              fmt.Sprintf("objstore: current_run mismatch (stored=%q, expected=%q)", existing.RunID, previousRunID),
			RunID:            existing.RunID,
			LastWriteVersion: existing.LastWriteVersion,
		}
	}
	if previousLastWriteVersion != 0 && existing.LastWriteVersion != previousLastWriteVersion {
		return &persistence.CurrentWorkflowConditionFailedError{
			Msg:              fmt.Sprintf("objstore: current_run version mismatch (stored=%d, expected=%d)", existing.LastWriteVersion, previousLastWriteVersion),
			RunID:            existing.RunID,
			LastWriteVersion: existing.LastWriteVersion,
		}
	}

	ptr := &currentRunPtr{
		RunID:            snap.RunID,
		ExecutionState:   blobToEnv(snap.ExecutionStateBlob),
		LastWriteVersion: snap.LastWriteVersion,
		UpdatedAt:        time.Now().UnixNano(),
	}
	body, err := json.Marshal(ptr)
	if err != nil {
		return fmt.Errorf("marshal new current_run: %w", err)
	}
	key := executionCurrentRunKey(snap.NamespaceID, snap.WorkflowID)
	_, err = e.blob.Put(ctx, key, body, blob.PutOptions{
		ContentType: "application/json",
		IfMatch:     etag,
	})
	if errors.Is(err, blob.ErrPreconditionFailed) {
		return &persistence.CurrentWorkflowConditionFailedError{
			Msg:              "objstore: current_run concurrent writer (etag CAS failed)",
			RunID:            existing.RunID,
			LastWriteVersion: existing.LastWriteVersion,
		}
	}
	if err != nil {
		return fmt.Errorf("objstore: put current_run: %w", err)
	}
	return nil
}

func (e *executionStore) ListConcreteExecutions(
	ctx context.Context,
	request *persistence.ListConcreteExecutionsRequest,
) (*persistence.InternalListConcreteExecutionsResponse, error) {
	_ = ctx
	_ = request
	return nil, serviceerror.NewUnimplemented("objstore: ListConcreteExecutions deferred — task #290")
}

// --- history tasks (transfer / timer / visibility / replication queues) ---
// Implementations land in execution_store_tasks.go (task #289).

func (e *executionStore) AddHistoryTasks(ctx context.Context, request *persistence.InternalAddHistoryTasksRequest) error {
	_ = ctx
	_ = request
	return serviceerror.NewUnimplemented("objstore: AddHistoryTasks deferred — task #289")
}

func (e *executionStore) GetHistoryTasks(ctx context.Context, request *persistence.GetHistoryTasksRequest) (*persistence.InternalGetHistoryTasksResponse, error) {
	_ = ctx
	_ = request
	return nil, serviceerror.NewUnimplemented("objstore: GetHistoryTasks deferred — task #289")
}

func (e *executionStore) CompleteHistoryTask(ctx context.Context, request *persistence.CompleteHistoryTaskRequest) error {
	_ = ctx
	_ = request
	return serviceerror.NewUnimplemented("objstore: CompleteHistoryTask deferred — task #289")
}

func (e *executionStore) RangeCompleteHistoryTasks(ctx context.Context, request *persistence.RangeCompleteHistoryTasksRequest) error {
	_ = ctx
	_ = request
	return serviceerror.NewUnimplemented("objstore: RangeCompleteHistoryTasks deferred — task #289")
}

// --- replication DLQ ---

func (e *executionStore) PutReplicationTaskToDLQ(ctx context.Context, request *persistence.PutReplicationTaskToDLQRequest) error {
	_ = ctx
	_ = request
	return serviceerror.NewUnimplemented("objstore: PutReplicationTaskToDLQ deferred — task #289")
}

func (e *executionStore) GetReplicationTasksFromDLQ(ctx context.Context, request *persistence.GetReplicationTasksFromDLQRequest) (*persistence.InternalGetReplicationTasksFromDLQResponse, error) {
	_ = ctx
	_ = request
	return nil, serviceerror.NewUnimplemented("objstore: GetReplicationTasksFromDLQ deferred — task #289")
}

func (e *executionStore) DeleteReplicationTaskFromDLQ(ctx context.Context, request *persistence.DeleteReplicationTaskFromDLQRequest) error {
	_ = ctx
	_ = request
	return serviceerror.NewUnimplemented("objstore: DeleteReplicationTaskFromDLQ deferred — task #289")
}

func (e *executionStore) RangeDeleteReplicationTaskFromDLQ(ctx context.Context, request *persistence.RangeDeleteReplicationTaskFromDLQRequest) error {
	_ = ctx
	_ = request
	return serviceerror.NewUnimplemented("objstore: RangeDeleteReplicationTaskFromDLQ deferred — task #289")
}

func (e *executionStore) IsReplicationDLQEmpty(ctx context.Context, request *persistence.GetReplicationTasksFromDLQRequest) (bool, error) {
	_ = ctx
	_ = request
	return false, serviceerror.NewUnimplemented("objstore: IsReplicationDLQEmpty deferred — task #289")
}

// --- history V2 branch APIs (task #288) ---

func (e *executionStore) AppendHistoryNodes(ctx context.Context, request *persistence.InternalAppendHistoryNodesRequest) error {
	return e.appendHistoryNodes(ctx, request)
}

func (e *executionStore) DeleteHistoryNodes(ctx context.Context, request *persistence.InternalDeleteHistoryNodesRequest) error {
	return e.deleteHistoryNodes(ctx, request)
}

func (e *executionStore) ReadHistoryBranch(ctx context.Context, request *persistence.InternalReadHistoryBranchRequest) (*persistence.InternalReadHistoryBranchResponse, error) {
	return e.readHistoryBranch(ctx, request)
}

func (e *executionStore) ForkHistoryBranch(ctx context.Context, request *persistence.InternalForkHistoryBranchRequest) error {
	return e.forkHistoryBranch(ctx, request)
}

func (e *executionStore) DeleteHistoryBranch(ctx context.Context, request *persistence.InternalDeleteHistoryBranchRequest) error {
	return e.deleteHistoryBranch(ctx, request)
}

func (e *executionStore) GetHistoryTreeContainingBranch(ctx context.Context, request *persistence.InternalGetHistoryTreeContainingBranchRequest) (*persistence.InternalGetHistoryTreeContainingBranchResponse, error) {
	return e.getHistoryTreeContainingBranch(ctx, request)
}

func (e *executionStore) GetAllHistoryTreeBranches(ctx context.Context, request *persistence.GetAllHistoryTreeBranchesRequest) (*persistence.InternalGetAllHistoryTreeBranchesResponse, error) {
	return e.getAllHistoryTreeBranches(ctx, request)
}
