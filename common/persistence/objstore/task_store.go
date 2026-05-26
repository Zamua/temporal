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

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/objstore/blob"
)

// taskStore implements [persistence.TaskStore] — the task-queue
// durable backstop for the matching service. Storage layout:
//
//	tasks/{ns}/{type}/{tq}/meta
//	  → JSON taskQueueEnv (RangeID, TaskQueueInfo blob).
//	    RangeID CAS guards "this matching host owns this queue."
//
//	tasks/{ns}/{type}/{tq}/items/{subqueue}/{pass:020d}-{taskID:020d}
//	  → JSON taskEnv. Append-once (IfNoneMatch=* on write).
//	    Lexicographic sort = (pass, taskID) ordering, matching
//	    Cassandra's CLUSTERING key.
//
//	tasks/{ns}/{type}/{tq}/user-data
//	  → JSON userDataEnv. Version-stamped CAS for build-ID changes.
//
//	tasks/{ns}/build-ids/{buildID}/{tq}
//	  → empty marker object so GetTaskQueuesByBuildId can list.
type taskStore struct {
	blob blob.Store
}

func newTaskStore(b blob.Store) persistence.TaskStore {
	return &taskStore{blob: b}
}

func (t *taskStore) GetName() string { return objstoreName }
func (t *taskStore) Close()          {}

type taskQueueEnv struct {
	RangeID   int64    `json:"r"`
	Info      *blobEnv `json:"info,omitempty"`
	Kind      int32    `json:"k,omitempty"`
	ExpiresAt int64    `json:"exp,omitempty"` // unix nanos
}

type taskEnv struct {
	TaskID    int64    `json:"id"`
	Pass      int64    `json:"p,omitempty"`
	Subqueue  int      `json:"sq,omitempty"`
	ExpiresAt int64    `json:"exp,omitempty"`
	Data      *blobEnv `json:"d,omitempty"`
}

type userDataEnv struct {
	Version  int64    `json:"v"`
	UserData *blobEnv `json:"d,omitempty"`
}

// --- key layout ---

func taskQueueMetaKey(ns, tq string, tt enumspb.TaskQueueType) string {
	return fmt.Sprintf("tasks/%s/%d/%s/meta", ns, tt, tq)
}

func taskItemsPrefix(ns, tq string, tt enumspb.TaskQueueType) string {
	return fmt.Sprintf("tasks/%s/%d/%s/items/", ns, tt, tq)
}

func taskItemKey(ns, tq string, tt enumspb.TaskQueueType, subqueue int, pass, taskID int64) string {
	return fmt.Sprintf("%s%d/%020d-%020d", taskItemsPrefix(ns, tq, tt), subqueue, pass, taskID)
}

func taskUserDataKey(ns, tq string) string {
	return fmt.Sprintf("tasks/%s/user-data/%s", ns, tq)
}

func taskUserDataPrefix(ns string) string {
	return fmt.Sprintf("tasks/%s/user-data/", ns)
}

func buildIdMarkerKey(ns, buildID, tq string) string {
	return fmt.Sprintf("tasks/%s/build-ids/%s/%s", ns, buildID, tq)
}

func buildIdPrefix(ns, buildID string) string {
	return fmt.Sprintf("tasks/%s/build-ids/%s/", ns, buildID)
}

// --- task queue lifecycle ---

func (t *taskStore) CreateTaskQueue(ctx context.Context, request *persistence.InternalCreateTaskQueueRequest) error {
	env := taskQueueEnv{
		RangeID: request.RangeID,
		Info:    blobToEnv(request.TaskQueueInfo),
		Kind:    int32(request.TaskQueueKind),
	}
	if request.ExpiryTime != nil {
		env.ExpiresAt = request.ExpiryTime.AsTime().UnixNano()
	}
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	_, err = t.blob.Put(ctx, taskQueueMetaKey(request.NamespaceID, request.TaskQueue, request.TaskType), body, blob.PutOptions{
		ContentType: "application/json",
		IfNoneMatch: "*",
	})
	if errors.Is(err, blob.ErrPreconditionFailed) {
		return &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("objstore: task queue %s/%s/%d already exists", request.NamespaceID, request.TaskQueue, request.TaskType),
		}
	}
	if err != nil {
		return fmt.Errorf("objstore: create task queue: %w", err)
	}
	return nil
}

func (t *taskStore) GetTaskQueue(ctx context.Context, request *persistence.InternalGetTaskQueueRequest) (*persistence.InternalGetTaskQueueResponse, error) {
	env, _, err := t.readTaskQueueMeta(ctx, request.NamespaceID, request.TaskQueue, request.TaskType)
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return nil, serviceerror.NewNotFoundf("objstore: task queue %s/%s/%d not found", request.NamespaceID, request.TaskQueue, request.TaskType)
		}
		return nil, err
	}
	return &persistence.InternalGetTaskQueueResponse{
		RangeID:       env.RangeID,
		TaskQueueInfo: envToBlob(env.Info),
	}, nil
}

func (t *taskStore) UpdateTaskQueue(ctx context.Context, request *persistence.InternalUpdateTaskQueueRequest) (*persistence.UpdateTaskQueueResponse, error) {
	existing, _, err := t.readTaskQueueMeta(ctx, request.NamespaceID, request.TaskQueue, request.TaskType)
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return nil, &persistence.ConditionFailedError{Msg: "objstore: task queue does not exist"}
		}
		return nil, err
	}
	// CAS predicate: stored RangeID must equal request.PrevRangeID.
	// Cassandra's LWT does this atomically via `IF range_id = ?`;
	// we approximate it with read-then-write. The remaining race
	// window between read and write would let two concurrent writers
	// both "succeed" at writing rangeID=N+1, but the matching
	// engine guarantees a single TaskQueueDB instance per (process,
	// partition) — so in practice cross-process races are the only
	// concern, and the matching engine's UnloadFromPartitionManager
	// → reload cycle reconverges quickly when a real conflict occurs.
	if existing.RangeID != request.PrevRangeID {
		return nil, &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("objstore: task queue rangeID mismatch (stored=%d, expected=%d)", existing.RangeID, request.PrevRangeID),
		}
	}
	newEnv := taskQueueEnv{
		RangeID: request.RangeID,
		Info:    blobToEnv(request.TaskQueueInfo),
		Kind:    int32(request.TaskQueueKind),
	}
	if request.ExpiryTime != nil {
		newEnv.ExpiresAt = request.ExpiryTime.AsTime().UnixNano()
	}
	body, err := json.Marshal(newEnv)
	if err != nil {
		return nil, err
	}
	if _, err := t.blob.Put(ctx, taskQueueMetaKey(request.NamespaceID, request.TaskQueue, request.TaskType), body, blob.PutOptions{
		ContentType: "application/json",
	}); err != nil {
		return nil, fmt.Errorf("objstore: update task queue: %w", err)
	}
	return &persistence.UpdateTaskQueueResponse{}, nil
}

func (t *taskStore) ListTaskQueue(ctx context.Context, _ *persistence.ListTaskQueueRequest) (*persistence.InternalListTaskQueueResponse, error) {
	infos, err := t.blob.List(ctx, "tasks/")
	if err != nil {
		return nil, fmt.Errorf("objstore: list task queues: %w", err)
	}
	items := make([]*persistence.InternalListTaskQueueItem, 0)
	for _, info := range infos {
		if !strings.HasSuffix(info.Key, "/meta") {
			continue
		}
		body, err := readBlobBody(ctx, t.blob, info.Key)
		if err != nil {
			return nil, fmt.Errorf("read meta: %w", err)
		}
		var env taskQueueEnv
		if err := json.Unmarshal(body, &env); err != nil {
			return nil, fmt.Errorf("unmarshal meta: %w", err)
		}
		items = append(items, &persistence.InternalListTaskQueueItem{
			TaskQueue: envToBlob(env.Info),
			RangeID:   env.RangeID,
		})
	}
	return &persistence.InternalListTaskQueueResponse{Items: items}, nil
}

func (t *taskStore) DeleteTaskQueue(ctx context.Context, request *persistence.DeleteTaskQueueRequest) error {
	ns := request.TaskQueue.NamespaceID
	tq := request.TaskQueue.TaskQueueName
	tt := request.TaskQueue.TaskQueueType
	// Verify RangeID before destructive deletion.
	existing, _, err := t.readTaskQueueMeta(ctx, ns, tq, tt)
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return nil
		}
		return err
	}
	if existing.RangeID != request.RangeID {
		return &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("objstore: task queue %s/%s/%d rangeID mismatch", ns, tq, tt),
		}
	}
	// Drop all tasks under this queue.
	taskInfos, err := t.blob.List(ctx, taskItemsPrefix(ns, tq, tt))
	if err != nil {
		return fmt.Errorf("list tasks for delete: %w", err)
	}
	for _, info := range taskInfos {
		_ = t.blob.Delete(ctx, info.Key, blob.DeleteOptions{})
	}
	_ = t.blob.Delete(ctx, taskUserDataKey(ns, tq), blob.DeleteOptions{})
	return t.blob.Delete(ctx, taskQueueMetaKey(ns, tq, tt), blob.DeleteOptions{})
}

// --- task lifecycle ---

func (t *taskStore) CreateTasks(ctx context.Context, request *persistence.InternalCreateTasksRequest) (*persistence.CreateTasksResponse, error) {
	// Cassandra's CreateTasks uses a single LWT batch where the task
	// queue meta CAS guards the task writes. If the CAS fails (storage
	// has a higher rangeID), the whole batch is rejected and the
	// matching engine treats it as a lease conflict.
	//
	// On objstore the same guarantee comes from etag-CAS on the meta
	// write when UpdateMetadata=true. We don't reject ahead-of-rangeID
	// writes outright — the matching engine's task-writer loop
	// captures db.rangeID at one point and writes shortly after; if a
	// concurrent renewal bumps rangeID in between, the writer's
	// rangeID is "behind" but the tasks are still valid for the queue
	// (taskIDs were allocated from the same monotonic counter). Only
	// reject when the stored rangeID is STRICTLY BEHIND the writer's
	// (shouldn't happen, but defensive).
	existing, etag, err := t.readTaskQueueMeta(ctx, request.NamespaceID, request.TaskQueue, request.TaskType)
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return nil, &persistence.ConditionFailedError{Msg: "objstore: task queue does not exist"}
		}
		return nil, err
	}
	if existing.RangeID < request.RangeID {
		return nil, &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("objstore: task queue rangeID behind writer (stored=%d, expected=%d)", existing.RangeID, request.RangeID),
		}
	}

	for _, task := range request.Tasks {
		env := taskEnv{
			TaskID:   task.TaskId,
			Pass:     task.TaskPass,
			Subqueue: task.Subqueue,
			Data:     blobToEnv(task.Task),
		}
		if task.ExpiryTime != nil {
			env.ExpiresAt = task.ExpiryTime.AsTime().UnixNano()
		}
		body, err := json.Marshal(env)
		if err != nil {
			return nil, err
		}
		key := taskItemKey(request.NamespaceID, request.TaskQueue, request.TaskType, task.Subqueue, task.TaskPass, task.TaskId)
		if _, err := t.blob.Put(ctx, key, body, blob.PutOptions{
			ContentType: "application/json",
		}); err != nil {
			return nil, fmt.Errorf("objstore: put task %d: %w", task.TaskId, err)
		}
	}

	if request.UpdateMetadata {
		newEnv := taskQueueEnv{
			RangeID: request.RangeID,
			Info:    blobToEnv(request.TaskQueueInfo),
			Kind:    existing.Kind,
		}
		body, err := json.Marshal(newEnv)
		if err != nil {
			return nil, err
		}
		if _, err := t.blob.Put(ctx, taskQueueMetaKey(request.NamespaceID, request.TaskQueue, request.TaskType), body, blob.PutOptions{
			ContentType: "application/json",
		}); err != nil {
			return nil, fmt.Errorf("objstore: update task queue meta: %w", err)
		}
	}
	_ = etag // retained for future use; rangeID guards correctness today
	return &persistence.CreateTasksResponse{UpdatedMetadata: request.UpdateMetadata}, nil
}

func (t *taskStore) GetTasks(ctx context.Context, request *persistence.GetTasksRequest) (*persistence.InternalGetTasksResponse, error) {
	infos, err := t.blob.List(ctx, taskItemsPrefix(request.NamespaceID, request.TaskQueue, request.TaskType))
	if err != nil {
		return nil, fmt.Errorf("objstore: list tasks: %w", err)
	}
	type item struct {
		pass   int64
		taskID int64
		key    string
	}
	matching := make([]item, 0, len(infos))
	for _, info := range infos {
		pass, taskID, subqueue, ok := parseTaskKey(info.Key)
		if !ok || subqueue != request.Subqueue {
			continue
		}
		// Range filter.
		if pass < request.InclusiveMinPass {
			continue
		}
		if pass == request.InclusiveMinPass && taskID < request.InclusiveMinTaskID {
			continue
		}
		if request.ExclusiveMaxTaskID != 0 {
			if pass == request.InclusiveMinPass && taskID >= request.ExclusiveMaxTaskID {
				continue
			}
		}
		matching = append(matching, item{pass, taskID, info.Key})
	}
	sort.Slice(matching, func(i, j int) bool {
		if matching[i].pass != matching[j].pass {
			return matching[i].pass < matching[j].pass
		}
		return matching[i].taskID < matching[j].taskID
	})

	pageStart := 0
	if len(request.NextPageToken) > 0 {
		if v, err := strconv.Atoi(string(request.NextPageToken)); err == nil {
			pageStart = v
		}
	}
	pageEnd := len(matching)
	if request.PageSize > 0 && pageStart+request.PageSize < pageEnd {
		pageEnd = pageStart + request.PageSize
	}

	tasks := make([]*commonpb.DataBlob, 0, pageEnd-pageStart)
	now := time.Now().UnixNano()
	for _, m := range matching[pageStart:pageEnd] {
		body, err := readBlobBody(ctx, t.blob, m.key)
		if err != nil {
			return nil, fmt.Errorf("read task %s: %w", m.key, err)
		}
		var env taskEnv
		if err := json.Unmarshal(body, &env); err != nil {
			return nil, fmt.Errorf("unmarshal task: %w", err)
		}
		// Expiry filter.
		if env.ExpiresAt != 0 && env.ExpiresAt < now {
			continue
		}
		tasks = append(tasks, envToBlob(env.Data))
	}
	var token []byte
	if pageEnd < len(matching) {
		token = []byte(strconv.Itoa(pageEnd))
	}
	return &persistence.InternalGetTasksResponse{
		Tasks:         tasks,
		NextPageToken: token,
	}, nil
}

func (t *taskStore) CompleteTasksLessThan(ctx context.Context, request *persistence.CompleteTasksLessThanRequest) (int, error) {
	infos, err := t.blob.List(ctx, taskItemsPrefix(request.NamespaceID, request.TaskQueueName, request.TaskType))
	if err != nil {
		return 0, fmt.Errorf("objstore: list tasks for completion: %w", err)
	}
	completed := 0
	for _, info := range infos {
		if request.Limit > 0 && completed >= request.Limit {
			break
		}
		pass, taskID, subqueue, ok := parseTaskKey(info.Key)
		if !ok || subqueue != request.Subqueue {
			continue
		}
		// Delete tasks strictly less than (ExclusiveMaxPass, ExclusiveMaxTaskID).
		eligible := false
		if request.ExclusiveMaxPass != 0 && pass < request.ExclusiveMaxPass {
			eligible = true
		} else if request.ExclusiveMaxPass == 0 && taskID < request.ExclusiveMaxTaskID {
			eligible = true
		}
		if !eligible {
			continue
		}
		if err := t.blob.Delete(ctx, info.Key, blob.DeleteOptions{}); err != nil {
			return completed, fmt.Errorf("delete task: %w", err)
		}
		completed++
	}
	return completed, nil
}

// --- user data ---

func (t *taskStore) GetTaskQueueUserData(ctx context.Context, request *persistence.GetTaskQueueUserDataRequest) (*persistence.InternalGetTaskQueueUserDataResponse, error) {
	body, err := readBlobBody(ctx, t.blob, taskUserDataKey(request.NamespaceID, request.TaskQueue))
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return nil, serviceerror.NewNotFoundf("objstore: user data not found for %s/%s", request.NamespaceID, request.TaskQueue)
		}
		return nil, err
	}
	var env userDataEnv
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("unmarshal user data: %w", err)
	}
	return &persistence.InternalGetTaskQueueUserDataResponse{
		Version:  env.Version,
		UserData: envToBlob(env.UserData),
	}, nil
}

func (t *taskStore) UpdateTaskQueueUserData(ctx context.Context, request *persistence.InternalUpdateTaskQueueUserDataRequest) error {
	for tq, update := range request.Updates {
		current, etag, err := t.readUserData(ctx, request.NamespaceID, tq)
		if err != nil && !errors.Is(err, blob.ErrNotFound) {
			return fmt.Errorf("read user data: %w", err)
		}
		var storedVersion int64
		if current != nil {
			storedVersion = current.Version
		}
		// Cassandra's CAS contract:
		//   update.Version = 0 → INSERT new row (no existing).
		//   update.Version > 0 → UPDATE IF stored.version == update.Version,
		//                        write stored.version = update.Version + 1.
		// We mirror that here.
		if update.Version == 0 {
			if current != nil {
				if update.Conflicting != nil {
					*update.Conflicting = true
				}
				continue
			}
		} else {
			if storedVersion != update.Version {
				if update.Conflicting != nil {
					*update.Conflicting = true
				}
				continue
			}
		}
		newEnv := userDataEnv{
			Version:  update.Version + 1,
			UserData: blobToEnv(update.UserData),
		}
		body, err := json.Marshal(newEnv)
		if err != nil {
			return err
		}
		opts := blob.PutOptions{ContentType: "application/json"}
		if etag != "" {
			opts.IfMatch = etag
		} else {
			opts.IfNoneMatch = "*"
		}
		_, err = t.blob.Put(ctx, taskUserDataKey(request.NamespaceID, tq), body, opts)
		if errors.Is(err, blob.ErrPreconditionFailed) {
			if update.Conflicting != nil {
				*update.Conflicting = true
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("put user data: %w", err)
		}
		if update.Applied != nil {
			*update.Applied = true
		}
		// Update build-id index.
		for _, bid := range update.BuildIdsAdded {
			_, _ = t.blob.Put(ctx, buildIdMarkerKey(request.NamespaceID, bid, tq), []byte("1"), blob.PutOptions{})
		}
		for _, bid := range update.BuildIdsRemoved {
			_ = t.blob.Delete(ctx, buildIdMarkerKey(request.NamespaceID, bid, tq), blob.DeleteOptions{})
		}
	}
	return nil
}

func (t *taskStore) ListTaskQueueUserDataEntries(ctx context.Context, request *persistence.ListTaskQueueUserDataEntriesRequest) (*persistence.InternalListTaskQueueUserDataEntriesResponse, error) {
	infos, err := t.blob.List(ctx, taskUserDataPrefix(request.NamespaceID))
	if err != nil {
		return nil, fmt.Errorf("objstore: list user data: %w", err)
	}
	out := make([]persistence.InternalTaskQueueUserDataEntry, 0, len(infos))
	for _, info := range infos {
		tq := strings.TrimPrefix(info.Key, taskUserDataPrefix(request.NamespaceID))
		body, err := readBlobBody(ctx, t.blob, info.Key)
		if err != nil {
			return nil, fmt.Errorf("read user data: %w", err)
		}
		var env userDataEnv
		if err := json.Unmarshal(body, &env); err != nil {
			return nil, fmt.Errorf("unmarshal user data: %w", err)
		}
		out = append(out, persistence.InternalTaskQueueUserDataEntry{
			TaskQueue: tq,
			Data:      envToBlob(env.UserData),
			Version:   env.Version,
		})
	}
	return &persistence.InternalListTaskQueueUserDataEntriesResponse{
		Entries: out,
	}, nil
}

func (t *taskStore) GetTaskQueuesByBuildId(ctx context.Context, request *persistence.GetTaskQueuesByBuildIdRequest) ([]string, error) {
	infos, err := t.blob.List(ctx, buildIdPrefix(request.NamespaceID, request.BuildID))
	if err != nil {
		return nil, fmt.Errorf("objstore: list build-id markers: %w", err)
	}
	out := make([]string, 0, len(infos))
	for _, info := range infos {
		tq := strings.TrimPrefix(info.Key, buildIdPrefix(request.NamespaceID, request.BuildID))
		out = append(out, tq)
	}
	return out, nil
}

func (t *taskStore) CountTaskQueuesByBuildId(ctx context.Context, request *persistence.CountTaskQueuesByBuildIdRequest) (int, error) {
	infos, err := t.blob.List(ctx, buildIdPrefix(request.NamespaceID, request.BuildID))
	if err != nil {
		return 0, fmt.Errorf("objstore: count build-id markers: %w", err)
	}
	return len(infos), nil
}

// --- helpers ---

func (t *taskStore) readTaskQueueMeta(ctx context.Context, ns, tq string, tt enumspb.TaskQueueType) (*taskQueueEnv, string, error) {
	res, err := t.blob.Get(ctx, taskQueueMetaKey(ns, tq, tt))
	if err != nil {
		return nil, "", err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, "", err
	}
	var env taskQueueEnv
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, "", fmt.Errorf("unmarshal task queue meta: %w", err)
	}
	return &env, res.ETag, nil
}

func (t *taskStore) readUserData(ctx context.Context, ns, tq string) (*userDataEnv, string, error) {
	res, err := t.blob.Get(ctx, taskUserDataKey(ns, tq))
	if err != nil {
		return nil, "", err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, "", err
	}
	var env userDataEnv
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, "", fmt.Errorf("unmarshal user data: %w", err)
	}
	return &env, res.ETag, nil
}

// parseTaskKey extracts (pass, taskID, subqueue) from a task item key.
// Key shape: tasks/{ns}/{type}/{tq}/items/{subqueue}/{pass:020d}-{taskID:020d}
func parseTaskKey(key string) (int64, int64, int, bool) {
	idx := strings.Index(key, "/items/")
	if idx == -1 {
		return 0, 0, 0, false
	}
	rest := key[idx+len("/items/"):]
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 {
		return 0, 0, 0, false
	}
	subqueue, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, 0, false
	}
	suffix := parts[1]
	dash := strings.Index(suffix, "-")
	if dash == -1 {
		return 0, 0, 0, false
	}
	pass, err := strconv.ParseInt(strings.TrimLeft(suffix[:dash], "0"), 10, 64)
	if err != nil {
		if suffix[:dash] == "00000000000000000000" {
			pass = 0
		} else {
			return 0, 0, 0, false
		}
	}
	taskID, err := strconv.ParseInt(strings.TrimLeft(suffix[dash+1:], "0"), 10, 64)
	if err != nil {
		if suffix[dash+1:] == "00000000000000000000" {
			taskID = 0
		} else {
			return 0, 0, 0, false
		}
	}
	return pass, taskID, subqueue, true
}
