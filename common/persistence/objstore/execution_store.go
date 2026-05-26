package objstore

import (
	"context"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/objstore/blob"
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
}

func newExecutionStore(b blob.Store, clusterName string) persistence.ExecutionStore {
	return &executionStore{blob: b, clusterName: clusterName}
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
	_ = ctx
	_ = request
	return nil, serviceerror.NewUnimplemented("objstore: CreateWorkflowExecution pending — see execution_store.go storage layout")
}

func (e *executionStore) UpdateWorkflowExecution(
	ctx context.Context,
	request *persistence.InternalUpdateWorkflowExecutionRequest,
) error {
	_ = ctx
	_ = request
	return serviceerror.NewUnimplemented("objstore: UpdateWorkflowExecution pending")
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
	_ = ctx
	_ = request
	return nil, serviceerror.NewUnimplemented("objstore: GetCurrentExecution pending — see execution_store.go storage layout")
}

func (e *executionStore) GetWorkflowExecution(
	ctx context.Context,
	request *persistence.GetWorkflowExecutionRequest,
) (*persistence.InternalGetWorkflowExecutionResponse, error) {
	_ = ctx
	_ = request
	return nil, serviceerror.NewUnimplemented("objstore: GetWorkflowExecution pending")
}

func (e *executionStore) SetWorkflowExecution(
	ctx context.Context,
	request *persistence.InternalSetWorkflowExecutionRequest,
) error {
	_ = ctx
	_ = request
	return serviceerror.NewUnimplemented("objstore: SetWorkflowExecution pending")
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
	_ = ctx
	_ = request
	return serviceerror.NewUnimplemented("objstore: AppendHistoryNodes deferred — task #288")
}

func (e *executionStore) DeleteHistoryNodes(ctx context.Context, request *persistence.InternalDeleteHistoryNodesRequest) error {
	_ = ctx
	_ = request
	return serviceerror.NewUnimplemented("objstore: DeleteHistoryNodes deferred — task #288")
}

func (e *executionStore) ReadHistoryBranch(ctx context.Context, request *persistence.InternalReadHistoryBranchRequest) (*persistence.InternalReadHistoryBranchResponse, error) {
	_ = ctx
	_ = request
	return nil, serviceerror.NewUnimplemented("objstore: ReadHistoryBranch deferred — task #288")
}

func (e *executionStore) ForkHistoryBranch(ctx context.Context, request *persistence.InternalForkHistoryBranchRequest) error {
	_ = ctx
	_ = request
	return serviceerror.NewUnimplemented("objstore: ForkHistoryBranch deferred — task #288")
}

func (e *executionStore) DeleteHistoryBranch(ctx context.Context, request *persistence.InternalDeleteHistoryBranchRequest) error {
	_ = ctx
	_ = request
	return serviceerror.NewUnimplemented("objstore: DeleteHistoryBranch deferred — task #288")
}

func (e *executionStore) GetHistoryTreeContainingBranch(ctx context.Context, request *persistence.InternalGetHistoryTreeContainingBranchRequest) (*persistence.InternalGetHistoryTreeContainingBranchResponse, error) {
	_ = ctx
	_ = request
	return nil, serviceerror.NewUnimplemented("objstore: GetHistoryTreeContainingBranch deferred — task #288")
}

func (e *executionStore) GetAllHistoryTreeBranches(ctx context.Context, request *persistence.GetAllHistoryTreeBranchesRequest) (*persistence.InternalGetAllHistoryTreeBranchesResponse, error) {
	_ = ctx
	_ = request
	return nil, serviceerror.NewUnimplemented("objstore: GetAllHistoryTreeBranches deferred — task #288")
}
