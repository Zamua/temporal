package objstore_test

import (
	"context"
	"errors"
	"testing"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/objstore"
	"go.temporal.io/server/common/persistence/objstore/blob/memfs"
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

// The deferred methods must surface Unimplemented (not panic, not
// nil) so a Temporal server boot with this backend can detect what's
// available — the fx managerProvider treats Unimplemented as "skip"
// rather than fatal.
func TestExecutionStore_DeferredMethodsReturnUnimplemented(t *testing.T) {
	store := executionStoreFactory(t)
	ctx := context.Background()
	cases := []struct {
		name string
		call func() error
	}{
		{"Create", func() error {
			_, err := store.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{})
			return err
		}},
		{"Update", func() error {
			return store.UpdateWorkflowExecution(ctx, &persistence.InternalUpdateWorkflowExecutionRequest{})
		}},
		{"Get", func() error {
			_, err := store.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{})
			return err
		}},
		{"GetCurrent", func() error {
			_, err := store.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{})
			return err
		}},
		{"Set", func() error {
			return store.SetWorkflowExecution(ctx, &persistence.InternalSetWorkflowExecutionRequest{})
		}},
		{"AppendHistoryNodes", func() error {
			return store.AppendHistoryNodes(ctx, &persistence.InternalAppendHistoryNodesRequest{})
		}},
		{"AddHistoryTasks", func() error {
			return store.AddHistoryTasks(ctx, &persistence.InternalAddHistoryTasksRequest{})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			var unimpl *serviceerror.Unimplemented
			if !errors.As(err, &unimpl) {
				t.Fatalf("expected Unimplemented from %s, got %v", tc.name, err)
			}
		})
	}
}
