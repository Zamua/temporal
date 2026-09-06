package objstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/temporalio/sqlparser"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence/objstore/blob"
	"go.temporal.io/server/common/persistence/visibility/manager"
	visstore "go.temporal.io/server/common/persistence/visibility/store"
	visquery "go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

// visibilityStore implements Temporal visibility over the same blob.Store
// abstraction as the rest of objstore. It is intentionally scan-based:
// object storage is the source of truth, not a query engine. The layout is:
//
//	visibility/{namespaceID}/runs/{runID} -> JSON visibilityEnv
//
// List/Count scan the namespace prefix, apply Temporal's visibility query
// parser in memory, then sort and paginate. This is correct for local/dev
// and conformance-sized workloads, but it is not pretending to be an ES/SQL
// replacement at scale.
type visibilityStore struct {
	blob                           blob.Store
	searchAttributesProvider       searchattribute.Provider
	searchAttributesMapperProvider searchattribute.MapperProvider
	chasmRegistry                  *chasm.Registry
	indexName                      string
}

var _ visstore.VisibilityStore = (*visibilityStore)(nil)

func newVisibilityStore(
	b blob.Store,
	saProvider searchattribute.Provider,
	saMapperProvider searchattribute.MapperProvider,
	chasmRegistry *chasm.Registry,
) visstore.VisibilityStore {
	return &visibilityStore{
		blob:                           b,
		searchAttributesProvider:       saProvider,
		searchAttributesMapperProvider: saMapperProvider,
		chasmRegistry:                  chasmRegistry,
		indexName:                      "objstore-visibility",
	}
}

type visibilityEnv struct {
	NamespaceID          string                          `json:"ns"`
	WorkflowID           string                          `json:"wid"`
	RunID                string                          `json:"rid"`
	TypeName             string                          `json:"type,omitempty"`
	StartTime            time.Time                       `json:"st"`
	ExecutionTime        time.Time                       `json:"et,omitempty"`
	CloseTime            time.Time                       `json:"ct,omitempty"`
	ExecutionDurationNs  int64                           `json:"dur,omitempty"`
	Status               enumspb.WorkflowExecutionStatus `json:"status,omitempty"`
	HistoryLength        int64                           `json:"hlen,omitempty"`
	HistorySizeBytes     int64                           `json:"hbytes,omitempty"`
	StateTransitionCount int64                           `json:"stc,omitempty"`
	Memo                 *blobEnv                        `json:"memo,omitempty"`
	TaskQueue            string                          `json:"tq,omitempty"`
	SearchAttributes     *commonpb.SearchAttributes      `json:"sa,omitempty"`
	ParentWorkflowID     string                          `json:"pwid,omitempty"`
	ParentRunID          string                          `json:"prid,omitempty"`
	RootWorkflowID       string                          `json:"rwid,omitempty"`
	RootRunID            string                          `json:"rrid,omitempty"`
}

type visibilityRecord struct {
	env              *visibilityEnv
	searchAttributes map[string]any
}

func visibilityKey(namespaceID, runID string) string {
	return fmt.Sprintf("visibility/%s/runs/%s", safeID(namespaceID), safeID(runID))
}

func visibilityNamespacePrefix(namespaceID string) string {
	return fmt.Sprintf("visibility/%s/runs/", safeID(namespaceID))
}

func (v *visibilityStore) Close() {}

func (v *visibilityStore) GetName() string {
	return objstoreName
}

func (v *visibilityStore) GetIndexName() string {
	return v.indexName
}

func (v *visibilityStore) ValidateCustomSearchAttributes(searchAttributes map[string]any) (map[string]any, error) {
	return searchAttributes, nil
}

func (v *visibilityStore) RecordWorkflowExecutionStarted(
	ctx context.Context,
	request *visstore.InternalRecordWorkflowExecutionStartedRequest,
) error {
	env := visibilityEnvFromBase(request.InternalVisibilityRequestBase)
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("objstore visibility: marshal started record: %w", err)
	}
	_, err = v.blob.Put(ctx, visibilityKey(env.NamespaceID, env.RunID), body, blob.PutOptions{
		ContentType: "application/json",
	})
	if err != nil {
		return fmt.Errorf("objstore visibility: put started record: %w", err)
	}
	return nil
}

func (v *visibilityStore) RecordWorkflowExecutionClosed(
	ctx context.Context,
	request *visstore.InternalRecordWorkflowExecutionClosedRequest,
) error {
	env := visibilityEnvFromBase(request.InternalVisibilityRequestBase)
	env.CloseTime = request.CloseTime
	env.HistoryLength = request.HistoryLength
	env.HistorySizeBytes = request.HistorySizeBytes
	env.ExecutionDurationNs = request.ExecutionDuration.Nanoseconds()
	env.StateTransitionCount = request.StateTransitionCount
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("objstore visibility: marshal closed record: %w", err)
	}
	_, err = v.blob.Put(ctx, visibilityKey(env.NamespaceID, env.RunID), body, blob.PutOptions{
		ContentType: "application/json",
	})
	if err != nil {
		return fmt.Errorf("objstore visibility: put closed record: %w", err)
	}
	return nil
}

func (v *visibilityStore) UpsertWorkflowExecution(
	ctx context.Context,
	request *visstore.InternalUpsertWorkflowExecutionRequest,
) error {
	env := visibilityEnvFromBase(request.InternalVisibilityRequestBase)
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("objstore visibility: marshal upsert record: %w", err)
	}
	_, err = v.blob.Put(ctx, visibilityKey(env.NamespaceID, env.RunID), body, blob.PutOptions{
		ContentType: "application/json",
	})
	if err != nil {
		return fmt.Errorf("objstore visibility: put upsert record: %w", err)
	}
	return nil
}

func (v *visibilityStore) DeleteWorkflowExecution(
	ctx context.Context,
	request *manager.VisibilityDeleteWorkflowExecutionRequest,
) error {
	if err := v.blob.Delete(ctx, visibilityKey(request.NamespaceID.String(), request.RunID), blob.DeleteOptions{}); err != nil {
		return fmt.Errorf("objstore visibility: delete record: %w", err)
	}
	return nil
}

func (v *visibilityStore) ListWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*visstore.InternalListExecutionsResponse, error) {
	return v.listExecutions(ctx, listVisibilityRequest{
		namespaceID:   request.NamespaceID.String(),
		namespaceName: request.Namespace,
		query:         request.Query,
		pageSize:      request.PageSize,
		nextPageToken: request.NextPageToken,
		chasmMapper:   nil,
		archetypeID:   chasm.UnspecifiedArchetypeID,
	})
}

func (v *visibilityStore) CountWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*visstore.InternalCountExecutionsResponse, error) {
	return v.countExecutions(ctx, countVisibilityRequest{
		namespaceID:   request.NamespaceID.String(),
		namespaceName: request.Namespace,
		query:         request.Query,
		chasmMapper:   nil,
		archetypeID:   chasm.UnspecifiedArchetypeID,
	})
}

func (v *visibilityStore) GetWorkflowExecution(
	ctx context.Context,
	request *manager.GetWorkflowExecutionRequest,
) (*visstore.InternalGetWorkflowExecutionResponse, error) {
	env, err := v.readVisibilityEnv(ctx, visibilityKey(request.NamespaceID.String(), request.RunID))
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return nil, serviceerror.NewNotFoundf("objstore visibility: workflow %s/%s not found", request.NamespaceID, request.RunID)
		}
		return nil, err
	}
	return &visstore.InternalGetWorkflowExecutionResponse{
		Execution: visibilityEnvToInfo(env),
	}, nil
}

func (v *visibilityStore) ListChasmExecutions(
	ctx context.Context,
	request *visibilityservice.ListChasmExecutionsRequest,
) (*visstore.InternalListExecutionsResponse, error) {
	mapper, err := v.chasmMapper(request.ArchetypeId)
	if err != nil {
		return nil, err
	}
	return v.listExecutions(ctx, listVisibilityRequest{
		namespaceID:   request.NamespaceId,
		namespaceName: namespace.Name(request.Namespace),
		query:         request.Query,
		pageSize:      int(request.PageSize),
		nextPageToken: request.NextPageToken,
		chasmMapper:   mapper,
		archetypeID:   request.ArchetypeId,
	})
}

func (v *visibilityStore) CountChasmExecutions(
	ctx context.Context,
	request *visibilityservice.CountChasmExecutionsRequest,
) (*visstore.InternalCountExecutionsResponse, error) {
	mapper, err := v.chasmMapper(request.ArchetypeId)
	if err != nil {
		return nil, err
	}
	return v.countExecutions(ctx, countVisibilityRequest{
		namespaceID:   request.NamespaceId,
		namespaceName: namespace.Name(request.Namespace),
		query:         request.Query,
		chasmMapper:   mapper,
		archetypeID:   request.ArchetypeId,
	})
}

func (v *visibilityStore) AddSearchAttributes(context.Context, *manager.AddSearchAttributesRequest) error {
	// Objstore visibility is schema-less: registered search attributes are
	// interpreted through searchattribute.Provider at query time.
	return nil
}

type listVisibilityRequest struct {
	namespaceID   string
	namespaceName namespace.Name
	query         string
	pageSize      int
	nextPageToken []byte
	chasmMapper   *chasm.VisibilitySearchAttributesMapper
	archetypeID   chasm.ArchetypeID
}

type countVisibilityRequest struct {
	namespaceID   string
	namespaceName namespace.Name
	query         string
	chasmMapper   *chasm.VisibilitySearchAttributesMapper
	archetypeID   chasm.ArchetypeID
}

func (v *visibilityStore) listExecutions(
	ctx context.Context,
	request listVisibilityRequest,
) (*visstore.InternalListExecutionsResponse, error) {
	predicate, orderBy, _, err := v.buildPredicate(ctx, request.namespaceName, request.query, request.chasmMapper, request.archetypeID)
	if err != nil {
		return nil, err
	}
	records, err := v.scanVisibilityRecords(ctx, request.namespaceID, request.chasmMapper)
	if err != nil {
		return nil, err
	}
	filtered := filterVisibilityRecords(records, predicate)
	sortVisibilityRecords(filtered, orderBy)

	pageStart, err := visibilityPageStart(request.nextPageToken)
	if err != nil {
		return nil, err
	}
	if pageStart > len(filtered) {
		pageStart = len(filtered)
	}
	pageSize := request.pageSize
	if pageSize <= 0 {
		pageSize = len(filtered)
	}
	pageEnd := len(filtered)
	if pageStart+pageSize < pageEnd {
		pageEnd = pageStart + pageSize
	}

	out := make([]*visstore.InternalExecutionInfo, 0, pageEnd-pageStart)
	for _, rec := range filtered[pageStart:pageEnd] {
		out = append(out, visibilityEnvToInfo(rec.env))
	}
	var nextToken []byte
	if pageEnd < len(filtered) {
		nextToken = []byte(strconv.Itoa(pageEnd))
	}
	return &visstore.InternalListExecutionsResponse{
		Executions:    out,
		NextPageToken: nextToken,
	}, nil
}

func (v *visibilityStore) countExecutions(
	ctx context.Context,
	request countVisibilityRequest,
) (*visstore.InternalCountExecutionsResponse, error) {
	predicate, _, groupBy, err := v.buildPredicate(ctx, request.namespaceName, request.query, request.chasmMapper, request.archetypeID)
	if err != nil {
		return nil, err
	}
	records, err := v.scanVisibilityRecords(ctx, request.namespaceID, request.chasmMapper)
	if err != nil {
		return nil, err
	}
	filtered := filterVisibilityRecords(records, predicate)
	if len(groupBy) == 0 {
		return &visstore.InternalCountExecutionsResponse{Count: int64(len(filtered))}, nil
	}
	groups, err := countVisibilityGroups(filtered, groupBy)
	if err != nil {
		return nil, err
	}
	return &visstore.InternalCountExecutionsResponse{
		Count:  int64(len(filtered)),
		Groups: groups,
	}, nil
}

func (v *visibilityStore) buildPredicate(
	ctx context.Context,
	nsName namespace.Name,
	queryString string,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
	archetypeID chasm.ArchetypeID,
) (visibilityPredicate, sqlparser.OrderBy, []*visquery.SAColumn, error) {
	saTypeMap, err := v.searchAttributesProvider.GetSearchAttributes(v.GetIndexName(), false)
	if err != nil {
		return nil, nil, nil, err
	}
	saMapper, err := v.searchAttributesMapperProvider.GetMapper(nsName)
	if err != nil {
		return nil, nil, nil, err
	}
	params, err := visquery.NewQueryConverter[visibilityPredicate](
		visibilityPredicateConverter{},
		nsName,
		saTypeMap,
		saMapper,
	).
		WithChasmMapper(chasmMapper).
		WithArchetypeID(archetypeID).
		Convert(queryString)
	if err != nil {
		var converterErr *visquery.ConverterError
		if errors.As(err, &converterErr) {
			return nil, nil, nil, converterErr.ToInvalidArgument()
		}
		return nil, nil, nil, err
	}
	return params.QueryExpr, params.OrderBy, params.GroupBy, nil
}

func (v *visibilityStore) scanVisibilityRecords(
	ctx context.Context,
	namespaceID string,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
) ([]*visibilityRecord, error) {
	infos, err := v.blob.List(ctx, visibilityNamespacePrefix(namespaceID))
	if err != nil {
		return nil, fmt.Errorf("objstore visibility: list namespace records: %w", err)
	}
	saTypeMap, err := v.searchAttributesProvider.GetSearchAttributes(v.GetIndexName(), false)
	if err != nil {
		return nil, err
	}
	out := make([]*visibilityRecord, 0, len(infos))
	for _, info := range infos {
		env, err := v.readVisibilityEnv(ctx, info.Key)
		if err != nil {
			return nil, err
		}
		decoded, err := decodeVisibilitySearchAttributes(env.SearchAttributes, saTypeMap, chasmMapper)
		if err != nil {
			return nil, err
		}
		out = append(out, &visibilityRecord{env: env, searchAttributes: decoded})
	}
	return out, nil
}

func (v *visibilityStore) readVisibilityEnv(ctx context.Context, key string) (*visibilityEnv, error) {
	res, err := v.blob.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("objstore visibility: read record: %w", err)
	}
	var env visibilityEnv
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("objstore visibility: unmarshal record: %w", err)
	}
	return &env, nil
}

func (v *visibilityStore) chasmMapper(archetypeID uint32) (*chasm.VisibilitySearchAttributesMapper, error) {
	if v.chasmRegistry == nil {
		return nil, serviceerror.NewInvalidArgumentf("unknown archetype ID: %d", archetypeID)
	}
	rc, ok := v.chasmRegistry.ComponentByID(archetypeID)
	if !ok {
		return nil, serviceerror.NewInvalidArgumentf("unknown archetype ID: %d", archetypeID)
	}
	return rc.SearchAttributesMapper(), nil
}

func visibilityEnvFromBase(base *visstore.InternalVisibilityRequestBase) visibilityEnv {
	if base == nil {
		return visibilityEnv{}
	}
	env := visibilityEnv{
		NamespaceID:      base.NamespaceID,
		WorkflowID:       base.WorkflowID,
		RunID:            base.RunID,
		TypeName:         base.WorkflowTypeName,
		StartTime:        base.StartTime,
		ExecutionTime:    base.ExecutionTime,
		Status:           base.Status,
		Memo:             blobToEnv(base.Memo),
		TaskQueue:        base.TaskQueue,
		SearchAttributes: base.SearchAttributes,
		RootWorkflowID:   base.RootWorkflowID,
		RootRunID:        base.RootRunID,
	}
	if base.ParentWorkflowID != nil {
		env.ParentWorkflowID = *base.ParentWorkflowID
	}
	if base.ParentRunID != nil {
		env.ParentRunID = *base.ParentRunID
	}
	return env
}

func visibilityEnvToInfo(env *visibilityEnv) *visstore.InternalExecutionInfo {
	if env == nil {
		return nil
	}
	executionTime := env.ExecutionTime
	if executionTime.IsZero() {
		executionTime = env.StartTime
	}
	return &visstore.InternalExecutionInfo{
		WorkflowID:           env.WorkflowID,
		RunID:                env.RunID,
		TypeName:             env.TypeName,
		StartTime:            env.StartTime,
		ExecutionTime:        executionTime,
		CloseTime:            env.CloseTime,
		ExecutionDuration:    time.Duration(env.ExecutionDurationNs),
		Status:               env.Status,
		HistoryLength:        env.HistoryLength,
		HistorySizeBytes:     env.HistorySizeBytes,
		StateTransitionCount: env.StateTransitionCount,
		Memo:                 envToBlob(env.Memo),
		TaskQueue:            env.TaskQueue,
		SearchAttributes:     env.SearchAttributes,
		ParentWorkflowID:     env.ParentWorkflowID,
		ParentRunID:          env.ParentRunID,
		RootWorkflowID:       env.RootWorkflowID,
		RootRunID:            env.RootRunID,
	}
}

func decodeVisibilitySearchAttributes(
	searchAttributes *commonpb.SearchAttributes,
	saTypeMap searchattribute.NameTypeMap,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
) (map[string]any, error) {
	if len(searchAttributes.GetIndexedFields()) == 0 {
		return nil, nil
	}
	combinedTypeMap := saTypeMap
	if chasmMapper != nil {
		combinedTypeMap = visstore.CombineTypeMaps(saTypeMap, chasmMapper)
	}
	decoded, err := searchattribute.Decode(searchAttributes, &combinedTypeMap, true)
	if err != nil {
		return nil, err
	}
	return decoded, nil
}

type visibilityPredicate func(*visibilityRecord) bool

type visibilityPredicateConverter struct{}

func (visibilityPredicateConverter) GetDatetimeFormat() string {
	return time.RFC3339Nano
}

func (visibilityPredicateConverter) BuildParenExpr(expr visibilityPredicate) (visibilityPredicate, error) {
	return expr, nil
}

func (visibilityPredicateConverter) BuildNotExpr(expr visibilityPredicate) (visibilityPredicate, error) {
	return func(rec *visibilityRecord) bool { return !evalVisibilityPredicate(expr, rec) }, nil
}

func (visibilityPredicateConverter) BuildAndExpr(exprs ...visibilityPredicate) (visibilityPredicate, error) {
	return func(rec *visibilityRecord) bool {
		for _, expr := range exprs {
			if !evalVisibilityPredicate(expr, rec) {
				return false
			}
		}
		return true
	}, nil
}

func (visibilityPredicateConverter) BuildOrExpr(exprs ...visibilityPredicate) (visibilityPredicate, error) {
	return func(rec *visibilityRecord) bool {
		for _, expr := range exprs {
			if evalVisibilityPredicate(expr, rec) {
				return true
			}
		}
		return false
	}, nil
}

func (visibilityPredicateConverter) ConvertComparisonExpr(
	operator string,
	col *visquery.SAColumn,
	value any,
) (visibilityPredicate, error) {
	return visibilityComparisonPredicate(operator, col, value), nil
}

func (visibilityPredicateConverter) ConvertKeywordComparisonExpr(
	operator string,
	col *visquery.SAColumn,
	value any,
) (visibilityPredicate, error) {
	return visibilityComparisonPredicate(operator, col, value), nil
}

func (visibilityPredicateConverter) ConvertKeywordListComparisonExpr(
	operator string,
	col *visquery.SAColumn,
	value any,
) (visibilityPredicate, error) {
	return visibilityComparisonPredicate(operator, col, value), nil
}

func (visibilityPredicateConverter) ConvertTextComparisonExpr(
	operator string,
	col *visquery.SAColumn,
	value any,
) (visibilityPredicate, error) {
	return visibilityComparisonPredicate(operator, col, value), nil
}

func (visibilityPredicateConverter) ConvertRangeExpr(
	operator string,
	col *visquery.SAColumn,
	from any,
	to any,
) (visibilityPredicate, error) {
	return func(rec *visibilityRecord) bool {
		got, ok := visibilityFieldValue(rec, col.FieldName)
		if !ok {
			return operator == sqlparser.NotBetweenStr
		}
		inRange := compareVisibilityValue(got, from) >= 0 && compareVisibilityValue(got, to) <= 0
		if operator == sqlparser.NotBetweenStr {
			return !inRange
		}
		return inRange
	}, nil
}

func (visibilityPredicateConverter) ConvertIsExpr(
	operator string,
	col *visquery.SAColumn,
) (visibilityPredicate, error) {
	return func(rec *visibilityRecord) bool {
		_, ok := visibilityFieldValue(rec, col.FieldName)
		if operator == sqlparser.IsNotNullStr {
			return ok
		}
		return !ok
	}, nil
}

func visibilityComparisonPredicate(operator string, col *visquery.SAColumn, value any) visibilityPredicate {
	return func(rec *visibilityRecord) bool {
		got, ok := visibilityFieldValue(rec, col.FieldName)
		return compareVisibilityByOperator(got, ok, operator, value)
	}
}

func evalVisibilityPredicate(expr visibilityPredicate, rec *visibilityRecord) bool {
	if expr == nil {
		return true
	}
	return expr(rec)
}

func filterVisibilityRecords(records []*visibilityRecord, predicate visibilityPredicate) []*visibilityRecord {
	out := records[:0]
	for _, rec := range records {
		if evalVisibilityPredicate(predicate, rec) {
			out = append(out, rec)
		}
	}
	return out
}

func visibilityFieldValue(rec *visibilityRecord, field string) (any, bool) {
	if rec == nil || rec.env == nil {
		return nil, false
	}
	env := rec.env
	switch field {
	case sadefs.NamespaceID:
		return env.NamespaceID, env.NamespaceID != ""
	case sadefs.WorkflowID:
		return env.WorkflowID, env.WorkflowID != ""
	case sadefs.RunID:
		return env.RunID, env.RunID != ""
	case sadefs.WorkflowType:
		return env.TypeName, env.TypeName != ""
	case sadefs.StartTime:
		return env.StartTime, !env.StartTime.IsZero()
	case sadefs.ExecutionTime:
		if env.ExecutionTime.IsZero() {
			return env.StartTime, !env.StartTime.IsZero()
		}
		return env.ExecutionTime, true
	case sadefs.CloseTime:
		return env.CloseTime, !env.CloseTime.IsZero()
	case sadefs.ExecutionStatus:
		return env.Status.String(), env.Status != enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED
	case sadefs.TaskQueue:
		return env.TaskQueue, env.TaskQueue != ""
	case sadefs.HistoryLength:
		return env.HistoryLength, env.HistoryLength != 0
	case sadefs.HistorySizeBytes:
		return env.HistorySizeBytes, env.HistorySizeBytes != 0
	case sadefs.ExecutionDuration:
		return env.ExecutionDurationNs, env.ExecutionDurationNs != 0
	case sadefs.StateTransitionCount:
		return env.StateTransitionCount, env.StateTransitionCount != 0
	case sadefs.ParentWorkflowID:
		return env.ParentWorkflowID, env.ParentWorkflowID != ""
	case sadefs.ParentRunID:
		return env.ParentRunID, env.ParentRunID != ""
	case sadefs.RootWorkflowID:
		return env.RootWorkflowID, env.RootWorkflowID != ""
	case sadefs.RootRunID:
		return env.RootRunID, env.RootRunID != ""
	default:
		if rec.searchAttributes == nil {
			return nil, false
		}
		val, ok := rec.searchAttributes[field]
		return val, ok && val != nil
	}
}

func compareVisibilityByOperator(got any, ok bool, operator string, want any) bool {
	if !ok {
		switch operator {
		case sqlparser.NotEqualStr, sqlparser.NotInStr, sqlparser.NotStartsWithStr:
			return true
		default:
			return false
		}
	}
	switch operator {
	case sqlparser.EqualStr:
		return visibilityValueEqual(got, want)
	case sqlparser.NotEqualStr:
		return !visibilityValueEqual(got, want)
	case sqlparser.InStr:
		return visibilityValueIn(got, want)
	case sqlparser.NotInStr:
		return !visibilityValueIn(got, want)
	case sqlparser.LessThanStr:
		return compareVisibilityValue(got, want) < 0
	case sqlparser.LessEqualStr:
		return compareVisibilityValue(got, want) <= 0
	case sqlparser.GreaterThanStr:
		return compareVisibilityValue(got, want) > 0
	case sqlparser.GreaterEqualStr:
		return compareVisibilityValue(got, want) >= 0
	case sqlparser.StartsWithStr:
		return visibilityStartsWith(got, want)
	case sqlparser.NotStartsWithStr:
		return !visibilityStartsWith(got, want)
	default:
		return false
	}
}

func visibilityValueIn(got any, want any) bool {
	values, ok := want.([]any)
	if !ok {
		return visibilityValueEqual(got, want)
	}
	for _, item := range values {
		if visibilityValueEqual(got, item) {
			return true
		}
	}
	return false
}

func visibilityValueEqual(got any, want any) bool {
	switch g := got.(type) {
	case []string:
		for _, item := range g {
			if visibilityScalarEqual(item, want) {
				return true
			}
		}
		return false
	case []any:
		for _, item := range g {
			if visibilityScalarEqual(item, want) {
				return true
			}
		}
		return false
	default:
		return visibilityScalarEqual(got, want)
	}
}

func visibilityScalarEqual(got any, want any) bool {
	return compareVisibilityValue(got, want) == 0
}

func visibilityStartsWith(got any, want any) bool {
	prefix, ok := normalizeVisibilityString(want)
	if !ok {
		return false
	}
	switch g := got.(type) {
	case []string:
		for _, item := range g {
			if strings.HasPrefix(item, prefix) {
				return true
			}
		}
		return false
	case []any:
		for _, item := range g {
			if s, ok := normalizeVisibilityString(item); ok && strings.HasPrefix(s, prefix) {
				return true
			}
		}
		return false
	default:
		s, ok := normalizeVisibilityString(got)
		return ok && strings.HasPrefix(s, prefix)
	}
}

func compareVisibilityValue(a any, b any) int {
	if at, ok := normalizeVisibilityTime(a); ok {
		if bt, ok := normalizeVisibilityTime(b); ok {
			if at.Before(bt) {
				return -1
			}
			if at.After(bt) {
				return 1
			}
			return 0
		}
	}
	if af, ok := normalizeVisibilityFloat(a); ok {
		if bf, ok := normalizeVisibilityFloat(b); ok {
			if af < bf {
				return -1
			}
			if af > bf {
				return 1
			}
			return 0
		}
	}
	as := fmt.Sprint(a)
	bs := fmt.Sprint(b)
	if as < bs {
		return -1
	}
	if as > bs {
		return 1
	}
	return 0
}

func normalizeVisibilityFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

func normalizeVisibilityTime(v any) (time.Time, bool) {
	switch t := v.(type) {
	case time.Time:
		return t.UTC(), true
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, t)
		if err != nil {
			return time.Time{}, false
		}
		return parsed.UTC(), true
	default:
		return time.Time{}, false
	}
}

func normalizeVisibilityString(v any) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	default:
		return fmt.Sprint(v), true
	}
}

func sortVisibilityRecords(records []*visibilityRecord, orderBy sqlparser.OrderBy) {
	if len(orderBy) == 0 {
		sort.SliceStable(records, func(i, j int) bool {
			return defaultVisibilityLess(records[i], records[j])
		})
		return
	}
	sort.SliceStable(records, func(i, j int) bool {
		for _, order := range orderBy {
			col, ok := order.Expr.(*visquery.SAColumn)
			if !ok {
				continue
			}
			left, leftOK := visibilityFieldValue(records[i], col.FieldName)
			right, rightOK := visibilityFieldValue(records[j], col.FieldName)
			if leftOK != rightOK {
				return leftOK
			}
			cmp := compareVisibilityValue(left, right)
			if cmp == 0 {
				continue
			}
			if order.Direction == sqlparser.DescScr {
				return cmp > 0
			}
			return cmp < 0
		}
		return records[i].env.RunID < records[j].env.RunID
	})
}

func defaultVisibilityLess(a *visibilityRecord, b *visibilityRecord) bool {
	aTime := visibilitySortTime(a.env)
	bTime := visibilitySortTime(b.env)
	if !aTime.Equal(bTime) {
		return aTime.After(bTime)
	}
	if !a.env.StartTime.Equal(b.env.StartTime) {
		return a.env.StartTime.After(b.env.StartTime)
	}
	return a.env.RunID > b.env.RunID
}

func visibilitySortTime(env *visibilityEnv) time.Time {
	if env.CloseTime.IsZero() {
		return time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return env.CloseTime
}

func visibilityPageStart(token []byte) (int, error) {
	if len(token) == 0 {
		return 0, nil
	}
	v, err := strconv.Atoi(string(token))
	if err != nil || v < 0 {
		return 0, serviceerror.NewInvalidArgumentf("invalid visibility page token %q", string(token))
	}
	return v, nil
}

func countVisibilityGroups(records []*visibilityRecord, groupBy []*visquery.SAColumn) ([]visstore.InternalAggregationGroup, error) {
	type group struct {
		values []*commonpb.Payload
		count  int64
	}
	groups := map[string]*group{}
	for _, rec := range records {
		keyParts := make([]string, 0, len(groupBy))
		payloads := make([]*commonpb.Payload, 0, len(groupBy))
		for _, col := range groupBy {
			value, ok := visibilityFieldValue(rec, col.FieldName)
			if !ok {
				value = nil
			}
			encoded, err := payload.Encode(value)
			if err != nil {
				return nil, err
			}
			payloads = append(payloads, encoded)
			keyParts = append(keyParts, fmt.Sprintf("%#v", value))
		}
		key := strings.Join(keyParts, "\x00")
		if groups[key] == nil {
			groups[key] = &group{values: payloads}
		}
		groups[key].count++
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]visstore.InternalAggregationGroup, 0, len(keys))
	for _, key := range keys {
		out = append(out, visstore.InternalAggregationGroup{
			GroupValues: groups[key].values,
			Count:       groups[key].count,
		})
	}
	return out, nil
}
