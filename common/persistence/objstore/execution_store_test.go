package objstore_test

import (
	"context"
	"errors"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/objstore"
	"go.temporal.io/server/common/persistence/objstore/blob/memfs"
	"go.temporal.io/server/common/persistence/serialization"
)

func executionStoreFactory(t *testing.T) persistence.ExecutionStore {
	t.Helper()
	f := objstore.NewFactory(memfs.New(), "active", log.NewNoopLogger())
	store, err := f.NewExecutionStore()
	if err != nil {
		t.Fatalf("NewExecutionStore: %v", err)
	}
	return store
}

func TestExecutionStore_Name(t *testing.T) {
	store := executionStoreFactory(t)
	if store.GetName() != "objstore" {
		t.Errorf("expected 'objstore', got %q", store.GetName())
	}
}

// Delete is idempotent against missing keys per blob.Store contract,
// which surfaces as no-error from the persistence layer too.
func TestExecutionStore_DeleteWorkflowExecution_OnMissing(t *testing.T) {
	store := executionStoreFactory(t)
	err := store.DeleteWorkflowExecution(context.Background(), &persistence.DeleteWorkflowExecutionRequest{
		ShardID:     1,
		NamespaceID: "ns",
		WorkflowID:  "wf",
		RunID:       "run",
	})
	if err != nil {
		t.Fatalf("delete on missing should succeed (idempotent), got %v", err)
	}
}

func TestExecutionStore_DeleteCurrentWorkflowExecution_OnMissing(t *testing.T) {
	store := executionStoreFactory(t)
	err := store.DeleteCurrentWorkflowExecution(context.Background(), &persistence.DeleteCurrentWorkflowExecutionRequest{
		ShardID:     1,
		NamespaceID: "ns",
		WorkflowID:  "wf",
		RunID:       "run",
	})
	if err != nil {
		t.Fatalf("delete on missing should succeed (idempotent), got %v", err)
	}
}

// --- snapshot helpers ---

// sampleSnapshot builds a minimal but real InternalWorkflowSnapshot.
// Both ExecutionInfo + ExecutionState are typed protos that get
// serialized to the matching *Blob fields — the same path that
// production code uses.
func sampleSnapshot(t *testing.T, namespaceID, workflowID, runID string, dbVersion int64) persistence.InternalWorkflowSnapshot {
	t.Helper()
	ser := serialization.NewSerializer()

	execInfo := &persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
	}
	infoBlob, err := ser.WorkflowExecutionInfoToBlob(execInfo)
	if err != nil {
		t.Fatalf("WorkflowExecutionInfoToBlob: %v", err)
	}

	execState := &persistencespb.WorkflowExecutionState{
		RunId:  runID,
		State:  4, // WORKFLOW_EXECUTION_STATE_RUNNING
		Status: 1, // WORKFLOW_EXECUTION_STATUS_RUNNING
	}
	stateBlob, err := ser.WorkflowExecutionStateToBlob(execState)
	if err != nil {
		t.Fatalf("WorkflowExecutionStateToBlob: %v", err)
	}

	return persistence.InternalWorkflowSnapshot{
		NamespaceID:        namespaceID,
		WorkflowID:         workflowID,
		RunID:              runID,
		ExecutionInfo:      execInfo,
		ExecutionInfoBlob:  infoBlob,
		ExecutionState:     execState,
		ExecutionStateBlob: stateBlob,
		NextEventID:        2,
		DBRecordVersion:    dbVersion,
		LastWriteVersion:   1,
		ActivityInfos: map[int64]*commonpb.DataBlob{
			1: {EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("activity-1-bytes")},
		},
	}
}

func TestExecutionStore_CreateThenGet_RoundTrip(t *testing.T) {
	store := executionStoreFactory(t)
	ctx := context.Background()
	snap := sampleSnapshot(t, "ns1", "wf1", "run1", 1)

	_, err := store.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID:             1,
		RangeID:             1,
		Mode:                persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: snap,
	})
	if err != nil {
		t.Fatalf("CreateWorkflowExecution: %v", err)
	}

	got, err := store.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID:     1,
		NamespaceID: "ns1",
		WorkflowID:  "wf1",
		RunID:       "run1",
	})
	if err != nil {
		t.Fatalf("GetWorkflowExecution: %v", err)
	}
	if got.DBRecordVersion != 1 {
		t.Errorf("expected DBRecordVersion=1, got %d", got.DBRecordVersion)
	}
	if got.State.NextEventID != 2 {
		t.Errorf("expected NextEventID=2, got %d", got.State.NextEventID)
	}
	if got.State.ExecutionInfo == nil || len(got.State.ExecutionInfo.Data) == 0 {
		t.Errorf("expected ExecutionInfo blob, got %v", got.State.ExecutionInfo)
	}
	if got.State.ActivityInfos == nil || got.State.ActivityInfos[1] == nil {
		t.Fatalf("expected ActivityInfos[1], got %+v", got.State.ActivityInfos)
	}
	if string(got.State.ActivityInfos[1].Data) != "activity-1-bytes" {
		t.Errorf("activity data round-trip mismatch: got %q", got.State.ActivityInfos[1].Data)
	}
}

func TestExecutionStore_BrandNew_RejectsExistingWorkflow(t *testing.T) {
	store := executionStoreFactory(t)
	ctx := context.Background()
	snap := sampleSnapshot(t, "ns1", "wf-collide", "run1", 1)

	_, err := store.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		Mode:                persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: snap,
	})
	if err != nil {
		t.Fatalf("first create: %v", err)
	}

	// Second BrandNew with a different runID on the same workflowID
	// must hit the current_run conflict.
	snap2 := sampleSnapshot(t, "ns1", "wf-collide", "run2", 1)
	_, err = store.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		Mode:                persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: snap2,
	})
	var conflict *persistence.CurrentWorkflowConditionFailedError
	if !errors.As(err, &conflict) {
		t.Fatalf("expected CurrentWorkflowConditionFailedError, got %v", err)
	}
	if conflict.RunID != "run1" {
		t.Errorf("expected RunID=run1 in conflict, got %q", conflict.RunID)
	}
}

func TestExecutionStore_BypassCurrent_DoesntTouchCurrentRun(t *testing.T) {
	store := executionStoreFactory(t)
	ctx := context.Background()

	// Seed a running workflow at run1 via BrandNew.
	if _, err := store.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		Mode:                persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: sampleSnapshot(t, "ns", "wf", "run1", 1),
	}); err != nil {
		t.Fatalf("seed BrandNew: %v", err)
	}

	// BypassCurrent creates a sidecar run (run2) without contesting current_run.
	if _, err := store.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		Mode:                persistence.CreateWorkflowModeBypassCurrent,
		NewWorkflowSnapshot: sampleSnapshot(t, "ns", "wf", "run2", 1),
	}); err != nil {
		t.Fatalf("BypassCurrent create: %v", err)
	}

	// current_run still points at run1.
	cur, err := store.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{NamespaceID: "ns", WorkflowID: "wf"})
	if err != nil {
		t.Fatalf("GetCurrentExecution: %v", err)
	}
	if cur.RunID != "run1" {
		t.Errorf("expected current_run=run1 after Bypass, got %q", cur.RunID)
	}
}

func TestExecutionStore_GetCurrentExecution_AfterCreate(t *testing.T) {
	store := executionStoreFactory(t)
	ctx := context.Background()
	snap := sampleSnapshot(t, "ns", "wf", "run-current", 1)
	if _, err := store.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		Mode:                persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: snap,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	got, err := store.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
		NamespaceID: "ns",
		WorkflowID:  "wf",
	})
	if err != nil {
		t.Fatalf("GetCurrentExecution: %v", err)
	}
	if got.RunID != "run-current" {
		t.Errorf("expected run-current, got %q", got.RunID)
	}
	if got.ExecutionState == nil {
		t.Fatal("expected ExecutionState to be decoded")
	}
}

func TestExecutionStore_GetWorkflowExecution_OnMissingReturnsNotFound(t *testing.T) {
	store := executionStoreFactory(t)
	_, err := store.GetWorkflowExecution(context.Background(), &persistence.GetWorkflowExecutionRequest{
		NamespaceID: "ns",
		WorkflowID:  "missing",
		RunID:       "missing",
	})
	var nf *serviceerror.NotFound
	if !errors.As(err, &nf) {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestExecutionStore_GetCurrentExecution_OnMissingReturnsNotFound(t *testing.T) {
	store := executionStoreFactory(t)
	_, err := store.GetCurrentExecution(context.Background(), &persistence.GetCurrentExecutionRequest{
		NamespaceID: "ns",
		WorkflowID:  "missing",
	})
	var nf *serviceerror.NotFound
	if !errors.As(err, &nf) {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestExecutionStore_Update_AppliesMutation(t *testing.T) {
	store := executionStoreFactory(t)
	ctx := context.Background()

	snap := sampleSnapshot(t, "ns", "wf", "run", 1)
	if _, err := store.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		Mode:                persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: snap,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	mutation := persistence.InternalWorkflowMutation{
		NamespaceID: "ns",
		WorkflowID:  "wf",
		RunID:       "run",
		// We need to keep ExecutionInfo + State around so re-reads work.
		ExecutionInfoBlob:  snap.ExecutionInfoBlob,
		ExecutionStateBlob: snap.ExecutionStateBlob,
		NextEventID:        5,
		LastWriteVersion:   1,
		DBRecordVersion:    2,
		Condition:          1, // must match stored DBRecordVersion
		UpsertActivityInfos: map[int64]*commonpb.DataBlob{
			2: {EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("activity-2")},
		},
		DeleteActivityInfos: map[int64]struct{}{1: {}},
		UpsertTimerInfos: map[string]*commonpb.DataBlob{
			"t1": {EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("timer-1")},
		},
	}
	if err := store.UpdateWorkflowExecution(ctx, &persistence.InternalUpdateWorkflowExecutionRequest{
		Mode:                   persistence.UpdateWorkflowModeBypassCurrent,
		UpdateWorkflowMutation: mutation,
	}); err != nil {
		t.Fatalf("UpdateWorkflowExecution: %v", err)
	}

	got, err := store.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		NamespaceID: "ns",
		WorkflowID:  "wf",
		RunID:       "run",
	})
	if err != nil {
		t.Fatalf("GetWorkflowExecution: %v", err)
	}
	if got.State.NextEventID != 5 {
		t.Errorf("expected NextEventID=5 after update, got %d", got.State.NextEventID)
	}
	if got.DBRecordVersion != 2 {
		t.Errorf("expected DBRecordVersion=2 after update, got %d", got.DBRecordVersion)
	}
	if _, ok := got.State.ActivityInfos[1]; ok {
		t.Errorf("activity 1 should have been deleted, still present")
	}
	if got.State.ActivityInfos[2] == nil || string(got.State.ActivityInfos[2].Data) != "activity-2" {
		t.Errorf("expected ActivityInfos[2] = activity-2, got %+v", got.State.ActivityInfos[2])
	}
	if got.State.TimerInfos["t1"] == nil {
		t.Errorf("expected TimerInfos[t1] after update, got %+v", got.State.TimerInfos)
	}
}

func TestExecutionStore_Update_StaleConditionRejected(t *testing.T) {
	store := executionStoreFactory(t)
	ctx := context.Background()
	snap := sampleSnapshot(t, "ns", "wf", "run", 5)
	if _, err := store.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		Mode:                persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: snap,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Stale Condition (we say 99 but stored is 5).
	err := store.UpdateWorkflowExecution(ctx, &persistence.InternalUpdateWorkflowExecutionRequest{
		Mode: persistence.UpdateWorkflowModeBypassCurrent,
		UpdateWorkflowMutation: persistence.InternalWorkflowMutation{
			NamespaceID:        "ns",
			WorkflowID:         "wf",
			RunID:              "run",
			ExecutionInfoBlob:  snap.ExecutionInfoBlob,
			ExecutionStateBlob: snap.ExecutionStateBlob,
			Condition:          99,
			DBRecordVersion:    6,
		},
	})
	var fail *persistence.WorkflowConditionFailedError
	if !errors.As(err, &fail) {
		t.Fatalf("expected WorkflowConditionFailedError, got %v", err)
	}
}

func TestExecutionStore_Update_OnMissingReturnsConditionFailed(t *testing.T) {
	store := executionStoreFactory(t)
	err := store.UpdateWorkflowExecution(context.Background(), &persistence.InternalUpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: persistence.InternalWorkflowMutation{
			NamespaceID: "ns",
			WorkflowID:  "ghost",
			RunID:       "ghost",
		},
	})
	var cf *persistence.ConditionFailedError
	if !errors.As(err, &cf) {
		t.Fatalf("expected ConditionFailedError on missing workflow update, got %v", err)
	}
}

func TestExecutionStore_Set_OverwritesUnconditionally(t *testing.T) {
	store := executionStoreFactory(t)
	ctx := context.Background()
	snap := sampleSnapshot(t, "ns", "wf", "run", 1)
	if _, err := store.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		Mode:                persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: snap,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Set with a different snapshot — unconditional overwrite.
	newSnap := sampleSnapshot(t, "ns", "wf", "run", 99)
	if err := store.SetWorkflowExecution(ctx, &persistence.InternalSetWorkflowExecutionRequest{
		SetWorkflowSnapshot: newSnap,
	}); err != nil {
		t.Fatalf("SetWorkflowExecution: %v", err)
	}

	got, err := store.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		NamespaceID: "ns",
		WorkflowID:  "wf",
		RunID:       "run",
	})
	if err != nil {
		t.Fatalf("re-read: %v", err)
	}
	if got.DBRecordVersion != 99 {
		t.Errorf("expected DBRecordVersion=99 after Set, got %d", got.DBRecordVersion)
	}
}

func TestExecutionStore_ContinueAsNew_SwitchesCurrentRun(t *testing.T) {
	store := executionStoreFactory(t)
	ctx := context.Background()

	snap := sampleSnapshot(t, "ns", "wf", "run1", 1)
	if _, err := store.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		Mode:                persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: snap,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// continue-as-new: close run1, open run2, swing current_run.
	newSnap := sampleSnapshot(t, "ns", "wf", "run2", 1)
	mutation := persistence.InternalWorkflowMutation{
		NamespaceID:        "ns",
		WorkflowID:         "wf",
		RunID:              "run1",
		ExecutionInfoBlob:  snap.ExecutionInfoBlob,
		ExecutionStateBlob: snap.ExecutionStateBlob,
		Condition:          1,
		DBRecordVersion:    2,
		LastWriteVersion:   1,
	}
	err := store.UpdateWorkflowExecution(ctx, &persistence.InternalUpdateWorkflowExecutionRequest{
		Mode:                   persistence.UpdateWorkflowModeUpdateCurrent,
		UpdateWorkflowMutation: mutation,
		NewWorkflowSnapshot:    &newSnap,
	})
	if err != nil {
		t.Fatalf("continue-as-new update: %v", err)
	}

	// current_run now points at run2.
	cur, err := store.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{NamespaceID: "ns", WorkflowID: "wf"})
	if err != nil {
		t.Fatalf("GetCurrentExecution: %v", err)
	}
	if cur.RunID != "run2" {
		t.Errorf("expected current_run=run2 after continue-as-new, got %q", cur.RunID)
	}

	// Both runs are readable.
	for _, runID := range []string{"run1", "run2"} {
		if _, err := store.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
			NamespaceID: "ns",
			WorkflowID:  "wf",
			RunID:       runID,
		}); err != nil {
			t.Errorf("get %s after continue-as-new: %v", runID, err)
		}
	}
}

func TestExecutionStore_DeleteThenGetReturnsNotFound(t *testing.T) {
	store := executionStoreFactory(t)
	ctx := context.Background()
	snap := sampleSnapshot(t, "ns", "wf", "run", 1)
	if _, err := store.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		Mode:                persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: snap,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := store.DeleteWorkflowExecution(ctx, &persistence.DeleteWorkflowExecutionRequest{
		NamespaceID: "ns",
		WorkflowID:  "wf",
		RunID:       "run",
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err := store.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		NamespaceID: "ns",
		WorkflowID:  "wf",
		RunID:       "run",
	})
	var nf *serviceerror.NotFound
	if !errors.As(err, &nf) {
		t.Fatalf("expected NotFound after delete, got %v", err)
	}
}
