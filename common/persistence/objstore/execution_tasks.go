package objstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/objstore/blob"
	"go.temporal.io/server/service/history/tasks"
)

// History task storage layout:
//
//	history-tasks/{shardID}/{categoryID}/{fireTimeNs:020d}-{taskID:020d}
//	  → JSON historyTaskEnv. Lex-sort = (FireTime, TaskID) order.
//	    The matching service / history service polls this prefix on
//	    restart to re-hydrate its in-memory queues.
//
//	dlq/replication/{sourceCluster}/{shardID}/{fireTimeNs:020d}-{taskID:020d}
//	  → JSON replicationDLQEnv.

func historyTaskKey(shardID int32, categoryID int, fireTime time.Time, taskID int64) string {
	return fmt.Sprintf("history-tasks/%d/%d/%020d-%020d", shardID, categoryID, fireTime.UnixNano(), taskID)
}

func historyTaskPrefix(shardID int32, categoryID int) string {
	return fmt.Sprintf("history-tasks/%d/%d/", shardID, categoryID)
}

func dlqTaskKey(sourceCluster string, shardID int32, fireTime time.Time, taskID int64) string {
	return fmt.Sprintf("dlq/replication/%s/%d/%020d-%020d", sourceCluster, shardID, fireTime.UnixNano(), taskID)
}

func dlqTaskPrefix(sourceCluster string, shardID int32) string {
	return fmt.Sprintf("dlq/replication/%s/%d/", sourceCluster, shardID)
}

type historyTaskEnv struct {
	FireTimeNs int64    `json:"ft"`
	TaskID     int64    `json:"id"`
	Blob       *blobEnv `json:"d,omitempty"`
}

type replicationDLQEnv struct {
	FireTimeNs int64    `json:"ft"`
	TaskID     int64    `json:"id"`
	Info       *blobEnv `json:"d,omitempty"`
}

// AddHistoryTasks persists a batch of tasks grouped by category.
// All tasks in one request are durable when the function returns;
// the matching / history service then re-reads them via GetHistoryTasks.
func (e *executionStore) addHistoryTasks(ctx context.Context, request *persistence.InternalAddHistoryTasksRequest) error {
	for category, taskList := range request.Tasks {
		for _, task := range taskList {
			env := historyTaskEnv{
				FireTimeNs: task.Key.FireTime.UnixNano(),
				TaskID:     task.Key.TaskID,
				Blob:       blobToEnv(task.Blob),
			}
			body, err := json.Marshal(env)
			if err != nil {
				return fmt.Errorf("marshal history task: %w", err)
			}
			key := historyTaskKey(request.ShardID, category.ID(), task.Key.FireTime, task.Key.TaskID)
			if _, err := e.blob.Put(ctx, key, body, blob.PutOptions{
				ContentType: "application/json",
			}); err != nil {
				return fmt.Errorf("put history task: %w", err)
			}
		}
	}
	return nil
}

func (e *executionStore) getHistoryTasks(ctx context.Context, request *persistence.GetHistoryTasksRequest) (*persistence.InternalGetHistoryTasksResponse, error) {
	infos, err := e.blob.List(ctx, historyTaskPrefix(request.ShardID, request.TaskCategory.ID()))
	if err != nil {
		return nil, fmt.Errorf("list history tasks: %w", err)
	}
	type item struct {
		key      string
		fireTime time.Time
		taskID   int64
	}
	matching := make([]item, 0, len(infos))
	minFire := request.InclusiveMinTaskKey.FireTime
	maxFire := request.ExclusiveMaxTaskKey.FireTime
	for _, info := range infos {
		fireNs, taskID, ok := parseHistoryTaskKey(info.Key)
		if !ok {
			continue
		}
		fireTime := time.Unix(0, fireNs)
		// (FireTime, TaskID) range filter.
		if fireTime.Before(minFire) {
			continue
		}
		if fireTime.Equal(minFire) && taskID < request.InclusiveMinTaskKey.TaskID {
			continue
		}
		if fireTime.After(maxFire) {
			continue
		}
		if fireTime.Equal(maxFire) && taskID >= request.ExclusiveMaxTaskKey.TaskID {
			continue
		}
		matching = append(matching, item{info.Key, fireTime, taskID})
	}
	sort.Slice(matching, func(i, j int) bool {
		if matching[i].fireTime.Equal(matching[j].fireTime) {
			return matching[i].taskID < matching[j].taskID
		}
		return matching[i].fireTime.Before(matching[j].fireTime)
	})

	pageStart := 0
	if len(request.NextPageToken) > 0 {
		if v, err := strconv.Atoi(string(request.NextPageToken)); err == nil {
			pageStart = v
		}
	}
	pageEnd := len(matching)
	if request.BatchSize > 0 && pageStart+request.BatchSize < pageEnd {
		pageEnd = pageStart + request.BatchSize
	}
	out := make([]persistence.InternalHistoryTask, 0, pageEnd-pageStart)
	for _, m := range matching[pageStart:pageEnd] {
		body, err := readBlobBody(ctx, e.blob, m.key)
		if err != nil {
			return nil, fmt.Errorf("read task: %w", err)
		}
		var env historyTaskEnv
		if err := json.Unmarshal(body, &env); err != nil {
			return nil, fmt.Errorf("unmarshal task: %w", err)
		}
		out = append(out, persistence.InternalHistoryTask{
			Key:  tasks.NewKey(m.fireTime, env.TaskID),
			Blob: envToBlob(env.Blob),
		})
	}
	var token []byte
	if pageEnd < len(matching) {
		token = []byte(strconv.Itoa(pageEnd))
	}
	return &persistence.InternalGetHistoryTasksResponse{
		Tasks:         out,
		NextPageToken: token,
	}, nil
}

func (e *executionStore) completeHistoryTask(ctx context.Context, request *persistence.CompleteHistoryTaskRequest) error {
	key := historyTaskKey(request.ShardID, request.TaskCategory.ID(), request.TaskKey.FireTime, request.TaskKey.TaskID)
	if err := e.blob.Delete(ctx, key, blob.DeleteOptions{}); err != nil {
		if request.BestEffort {
			return nil
		}
		return fmt.Errorf("delete history task: %w", err)
	}
	return nil
}

func (e *executionStore) rangeCompleteHistoryTasks(ctx context.Context, request *persistence.RangeCompleteHistoryTasksRequest) error {
	infos, err := e.blob.List(ctx, historyTaskPrefix(request.ShardID, request.TaskCategory.ID()))
	if err != nil {
		return fmt.Errorf("list for range complete: %w", err)
	}
	for _, info := range infos {
		fireNs, taskID, ok := parseHistoryTaskKey(info.Key)
		if !ok {
			continue
		}
		fireTime := time.Unix(0, fireNs)
		// In [InclusiveMinTaskKey, ExclusiveMaxTaskKey).
		if fireTime.Before(request.InclusiveMinTaskKey.FireTime) {
			continue
		}
		if fireTime.Equal(request.InclusiveMinTaskKey.FireTime) && taskID < request.InclusiveMinTaskKey.TaskID {
			continue
		}
		if fireTime.After(request.ExclusiveMaxTaskKey.FireTime) {
			continue
		}
		if fireTime.Equal(request.ExclusiveMaxTaskKey.FireTime) && taskID >= request.ExclusiveMaxTaskKey.TaskID {
			continue
		}
		if err := e.blob.Delete(ctx, info.Key, blob.DeleteOptions{}); err != nil {
			return fmt.Errorf("delete task in range: %w", err)
		}
	}
	return nil
}

// --- replication DLQ ---

func (e *executionStore) putReplicationTaskToDLQ(ctx context.Context, request *persistence.PutReplicationTaskToDLQRequest) error {
	if request.TaskInfo == nil {
		return errors.New("objstore: PutReplicationTaskToDLQ: TaskInfo is required")
	}
	infoBlob, err := e.serializer.ReplicationTaskInfoToBlob(request.TaskInfo)
	if err != nil {
		return fmt.Errorf("serialize replication task info: %w", err)
	}
	env := replicationDLQEnv{
		FireTimeNs: 0, // replication tasks don't have a fire time
		TaskID:     request.TaskInfo.GetTaskId(),
		Info:       blobToEnv(infoBlob),
	}
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	key := dlqTaskKey(request.SourceClusterName, request.ShardID, time.Unix(0, 0), request.TaskInfo.GetTaskId())
	_, err = e.blob.Put(ctx, key, body, blob.PutOptions{
		ContentType: "application/json",
	})
	if err != nil {
		return fmt.Errorf("put DLQ task: %w", err)
	}
	return nil
}

func (e *executionStore) getReplicationTasksFromDLQ(ctx context.Context, request *persistence.GetReplicationTasksFromDLQRequest) (*persistence.InternalGetReplicationTasksFromDLQResponse, error) {
	infos, err := e.blob.List(ctx, dlqTaskPrefix(request.SourceClusterName, request.ShardID))
	if err != nil {
		return nil, fmt.Errorf("list DLQ: %w", err)
	}
	type item struct {
		key    string
		taskID int64
	}
	matching := make([]item, 0, len(infos))
	for _, info := range infos {
		_, taskID, ok := parseDLQKey(info.Key)
		if !ok {
			continue
		}
		if taskID < request.InclusiveMinTaskKey.TaskID {
			continue
		}
		if taskID >= request.ExclusiveMaxTaskKey.TaskID {
			continue
		}
		matching = append(matching, item{info.Key, taskID})
	}
	sort.Slice(matching, func(i, j int) bool { return matching[i].taskID < matching[j].taskID })

	pageStart := 0
	if len(request.NextPageToken) > 0 {
		if v, err := strconv.Atoi(string(request.NextPageToken)); err == nil {
			pageStart = v
		}
	}
	pageEnd := len(matching)
	if request.BatchSize > 0 && pageStart+request.BatchSize < pageEnd {
		pageEnd = pageStart + request.BatchSize
	}
	out := make([]persistence.InternalHistoryTask, 0, pageEnd-pageStart)
	for _, m := range matching[pageStart:pageEnd] {
		body, err := readBlobBody(ctx, e.blob, m.key)
		if err != nil {
			return nil, fmt.Errorf("read DLQ task: %w", err)
		}
		var env replicationDLQEnv
		if err := json.Unmarshal(body, &env); err != nil {
			return nil, fmt.Errorf("unmarshal DLQ task: %w", err)
		}
		out = append(out, persistence.InternalHistoryTask{
			Key:  tasks.NewImmediateKey(env.TaskID),
			Blob: envToBlob(env.Info),
		})
	}
	var token []byte
	if pageEnd < len(matching) {
		token = []byte(strconv.Itoa(pageEnd))
	}
	return &persistence.InternalGetHistoryTasksResponse{
		Tasks:         out,
		NextPageToken: token,
	}, nil
}

func (e *executionStore) deleteReplicationTaskFromDLQ(ctx context.Context, request *persistence.DeleteReplicationTaskFromDLQRequest) error {
	key := dlqTaskKey(request.SourceClusterName, request.ShardID, request.TaskKey.FireTime, request.TaskKey.TaskID)
	if err := e.blob.Delete(ctx, key, blob.DeleteOptions{}); err != nil {
		return fmt.Errorf("delete DLQ task: %w", err)
	}
	return nil
}

func (e *executionStore) rangeDeleteReplicationTaskFromDLQ(ctx context.Context, request *persistence.RangeDeleteReplicationTaskFromDLQRequest) error {
	infos, err := e.blob.List(ctx, dlqTaskPrefix(request.SourceClusterName, request.ShardID))
	if err != nil {
		return fmt.Errorf("list DLQ for range delete: %w", err)
	}
	for _, info := range infos {
		_, taskID, ok := parseDLQKey(info.Key)
		if !ok {
			continue
		}
		if taskID < request.InclusiveMinTaskKey.TaskID {
			continue
		}
		if taskID >= request.ExclusiveMaxTaskKey.TaskID {
			continue
		}
		if err := e.blob.Delete(ctx, info.Key, blob.DeleteOptions{}); err != nil {
			return fmt.Errorf("delete DLQ task in range: %w", err)
		}
	}
	return nil
}

func (e *executionStore) isReplicationDLQEmpty(ctx context.Context, request *persistence.GetReplicationTasksFromDLQRequest) (bool, error) {
	infos, err := e.blob.List(ctx, dlqTaskPrefix(request.SourceClusterName, request.ShardID))
	if err != nil {
		return false, fmt.Errorf("list DLQ for empty check: %w", err)
	}
	for _, info := range infos {
		_, taskID, ok := parseDLQKey(info.Key)
		if !ok {
			continue
		}
		if taskID >= request.InclusiveMinTaskKey.TaskID && taskID < request.ExclusiveMaxTaskKey.TaskID {
			return false, nil
		}
	}
	return true, nil
}

// parseHistoryTaskKey extracts (fireTimeNs, taskID) from a history task key.
func parseHistoryTaskKey(key string) (int64, int64, bool) {
	idx := strings.LastIndex(key, "/")
	if idx == -1 {
		return 0, 0, false
	}
	tail := key[idx+1:]
	dash := strings.Index(tail, "-")
	if dash == -1 {
		return 0, 0, false
	}
	fireNs, err := strconv.ParseInt(strings.TrimLeft(tail[:dash], "0"), 10, 64)
	if err != nil && tail[:dash] != strings.Repeat("0", len(tail[:dash])) {
		return 0, 0, false
	}
	taskID, err := strconv.ParseInt(strings.TrimLeft(tail[dash+1:], "0"), 10, 64)
	if err != nil && tail[dash+1:] != strings.Repeat("0", len(tail[dash+1:])) {
		return 0, 0, false
	}
	return fireNs, taskID, true
}

func parseDLQKey(key string) (int64, int64, bool) {
	return parseHistoryTaskKey(key) // same shape — (fireNs, taskID) tail
}
