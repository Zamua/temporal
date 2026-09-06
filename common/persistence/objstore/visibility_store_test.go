package objstore

import (
	"context"
	"errors"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/objstore/blob/memfs"
	"go.temporal.io/server/common/persistence/visibility/manager"
	visstore "go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/searchattribute"
)

func testVisibilityStore(t *testing.T) *visibilityStore {
	t.Helper()
	return newVisibilityStore(
		memfs.New(),
		searchattribute.NewTestEsProvider(),
		searchattribute.NewTestMapperProvider(&searchattribute.NoopMapper{}),
		nil,
	).(*visibilityStore)
}

func testVisibilitySearchAttributes(t *testing.T, attrs map[string]any) *commonpb.SearchAttributes {
	t.Helper()
	typeMap, err := searchattribute.NewTestEsProvider().GetSearchAttributes("objstore-visibility", false)
	if err != nil {
		t.Fatalf("GetSearchAttributes: %v", err)
	}
	encoded, err := searchattribute.Encode(attrs, &typeMap)
	if err != nil {
		t.Fatalf("Encode search attributes: %v", err)
	}
	return encoded
}

func testVisibilityBase(t *testing.T, workflowID, runID string, status enumspb.WorkflowExecutionStatus, start time.Time, attrs map[string]any) *visstore.InternalVisibilityRequestBase {
	t.Helper()
	return &visstore.InternalVisibilityRequestBase{
		NamespaceID:      "ns1",
		WorkflowID:       workflowID,
		RunID:            runID,
		WorkflowTypeName: "TransferWorkflow",
		StartTime:        start,
		ExecutionTime:    start,
		Status:           status,
		TaskID:           1,
		TaskQueue:        "payments",
		Memo:             &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3},
		SearchAttributes: testVisibilitySearchAttributes(t, attrs),
		RootWorkflowID:   workflowID,
		RootRunID:        runID,
	}
}

func TestVisibilityStore_RecordListCountAndGet(t *testing.T) {
	store := testVisibilityStore(t)
	ctx := context.Background()
	start1 := time.Date(2026, 5, 26, 10, 0, 0, 0, time.UTC)
	start2 := start1.Add(time.Minute)

	err := store.RecordWorkflowExecutionStarted(ctx, &visstore.InternalRecordWorkflowExecutionStartedRequest{
		InternalVisibilityRequestBase: testVisibilityBase(t, "wf-1", "run-1", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, start1, map[string]any{
			"CustomKeywordField": "alpha",
			"CustomIntField":     int64(3),
		}),
	})
	if err != nil {
		t.Fatalf("RecordWorkflowExecutionStarted run-1: %v", err)
	}
	err = store.RecordWorkflowExecutionStarted(ctx, &visstore.InternalRecordWorkflowExecutionStartedRequest{
		InternalVisibilityRequestBase: testVisibilityBase(t, "wf-2", "run-2", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, start2, map[string]any{
			"CustomKeywordField": "beta",
			"CustomIntField":     int64(9),
		}),
	})
	if err != nil {
		t.Fatalf("RecordWorkflowExecutionStarted run-2: %v", err)
	}

	list, err := store.ListWorkflowExecutions(ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: namespace.ID("ns1"),
		Namespace:   namespace.Name("ns1"),
		Query:       `WorkflowType = "TransferWorkflow" and CustomIntField > 5`,
		PageSize:    10,
	})
	if err != nil {
		t.Fatalf("ListWorkflowExecutions: %v", err)
	}
	if len(list.Executions) != 1 || list.Executions[0].WorkflowID != "wf-2" {
		t.Fatalf("expected wf-2 only, got %+v", list.Executions)
	}

	count, err := store.CountWorkflowExecutions(ctx, &manager.CountWorkflowExecutionsRequest{
		NamespaceID: namespace.ID("ns1"),
		Namespace:   namespace.Name("ns1"),
		Query:       `ExecutionStatus = "Running"`,
	})
	if err != nil {
		t.Fatalf("CountWorkflowExecutions: %v", err)
	}
	if count.Count != 2 {
		t.Fatalf("expected count=2, got %d", count.Count)
	}

	got, err := store.GetWorkflowExecution(ctx, &manager.GetWorkflowExecutionRequest{
		NamespaceID: namespace.ID("ns1"),
		Namespace:   namespace.Name("ns1"),
		RunID:       "run-1",
	})
	if err != nil {
		t.Fatalf("GetWorkflowExecution: %v", err)
	}
	if got.Execution.WorkflowID != "wf-1" || got.Execution.TaskQueue != "payments" {
		t.Fatalf("unexpected execution: %+v", got.Execution)
	}
}

func TestVisibilityStore_CloseDeleteAndPagination(t *testing.T) {
	store := testVisibilityStore(t)
	ctx := context.Background()
	start := time.Date(2026, 5, 26, 10, 0, 0, 0, time.UTC)

	for _, item := range []struct {
		workflowID string
		runID      string
		closeTime  time.Time
	}{
		{"wf-1", "run-1", start.Add(3 * time.Minute)},
		{"wf-2", "run-2", start.Add(2 * time.Minute)},
	} {
		err := store.RecordWorkflowExecutionClosed(ctx, &visstore.InternalRecordWorkflowExecutionClosedRequest{
			InternalVisibilityRequestBase: testVisibilityBase(t, item.workflowID, item.runID, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, start, map[string]any{
				"CustomKeywordField": "done",
			}),
			CloseTime:            item.closeTime,
			ExecutionDuration:    item.closeTime.Sub(start),
			HistoryLength:        7,
			HistorySizeBytes:     99,
			StateTransitionCount: 5,
		})
		if err != nil {
			t.Fatalf("RecordWorkflowExecutionClosed %s: %v", item.runID, err)
		}
	}

	first, err := store.ListWorkflowExecutions(ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: namespace.ID("ns1"),
		Namespace:   namespace.Name("ns1"),
		Query:       `CustomKeywordField = "done"`,
		PageSize:    1,
	})
	if err != nil {
		t.Fatalf("List first page: %v", err)
	}
	if len(first.Executions) != 1 || first.Executions[0].RunID != "run-1" || len(first.NextPageToken) == 0 {
		t.Fatalf("unexpected first page: %+v token=%q", first.Executions, first.NextPageToken)
	}
	second, err := store.ListWorkflowExecutions(ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   namespace.ID("ns1"),
		Namespace:     namespace.Name("ns1"),
		Query:         `CustomKeywordField = "done"`,
		PageSize:      1,
		NextPageToken: first.NextPageToken,
	})
	if err != nil {
		t.Fatalf("List second page: %v", err)
	}
	if len(second.Executions) != 1 || second.Executions[0].RunID != "run-2" || len(second.NextPageToken) != 0 {
		t.Fatalf("unexpected second page: %+v token=%q", second.Executions, second.NextPageToken)
	}

	err = store.DeleteWorkflowExecution(ctx, &manager.VisibilityDeleteWorkflowExecutionRequest{
		NamespaceID: namespace.ID("ns1"),
		RunID:       "run-1",
		WorkflowID:  "wf-1",
	})
	if err != nil {
		t.Fatalf("DeleteWorkflowExecution: %v", err)
	}
	_, err = store.GetWorkflowExecution(ctx, &manager.GetWorkflowExecutionRequest{
		NamespaceID: namespace.ID("ns1"),
		Namespace:   namespace.Name("ns1"),
		RunID:       "run-1",
	})
	var notFound *serviceerror.NotFound
	if !errors.As(err, &notFound) {
		t.Fatalf("expected NotFound after delete, got %v", err)
	}
}
