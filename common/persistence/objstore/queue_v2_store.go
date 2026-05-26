package objstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/objstore/blob"
)

// queueV2Store implements [persistence.QueueV2]:
//
//	queues-v2/{queueType}/{queueName}/meta              → JSON queueMetaEnv (next message ID, last message ID)
//	queues-v2/{queueType}/{queueName}/messages/{id:020} → JSON queueMessageEnv
//
// Each enqueue allocates a fresh ID via CAS on the meta record;
// matches the cassandra approach where the queue table has a
// monotonic message_id column.
type queueV2Store struct {
	blob blob.Store
	mu   sync.Mutex // serialize enqueues per process; CAS keeps cross-process safety
}

func newQueueV2Store(b blob.Store) persistence.QueueV2 {
	return &queueV2Store{blob: b}
}

type queueMetaEnv struct {
	NextID        int64 `json:"next_id"`
	MessageCount  int64 `json:"count"`
	LastMessageID int64 `json:"last"`
}

type queueMessageEnv struct {
	ID   int64    `json:"id"`
	Data *blobEnv `json:"d,omitempty"`
}

func queueMetaKey(qt persistence.QueueV2Type, name string) string {
	return fmt.Sprintf("queues-v2/%d/%s/meta", qt, name)
}

func queueMessagePrefix(qt persistence.QueueV2Type, name string) string {
	return fmt.Sprintf("queues-v2/%d/%s/messages/", qt, name)
}

func queueMessageKey(qt persistence.QueueV2Type, name string, id int64) string {
	return fmt.Sprintf("%s%020d", queueMessagePrefix(qt, name), id)
}

func (q *queueV2Store) CreateQueue(ctx context.Context, request *persistence.InternalCreateQueueRequest) (*persistence.InternalCreateQueueResponse, error) {
	meta := queueMetaEnv{NextID: 1, LastMessageID: -1}
	body, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	_, err = q.blob.Put(ctx, queueMetaKey(request.QueueType, request.QueueName), body, blob.PutOptions{
		ContentType: "application/json",
		IfNoneMatch: "*",
	})
	if errors.Is(err, blob.ErrPreconditionFailed) {
		return nil, &persistence.ConditionFailedError{Msg: fmt.Sprintf("objstore: queue %d/%s already exists", request.QueueType, request.QueueName)}
	}
	if err != nil {
		return nil, fmt.Errorf("objstore: create queue: %w", err)
	}
	return &persistence.InternalCreateQueueResponse{}, nil
}

func (q *queueV2Store) EnqueueMessage(ctx context.Context, request *persistence.InternalEnqueueMessageRequest) (*persistence.InternalEnqueueMessageResponse, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	metaKey := queueMetaKey(request.QueueType, request.QueueName)
	meta, etag, err := q.readQueueMeta(ctx, metaKey)
	if err != nil {
		return nil, err
	}
	id := meta.NextID
	msgEnv := queueMessageEnv{ID: id, Data: blobToEnv(request.Blob)}
	msgBody, err := json.Marshal(msgEnv)
	if err != nil {
		return nil, err
	}
	if _, err := q.blob.Put(ctx, queueMessageKey(request.QueueType, request.QueueName, id), msgBody, blob.PutOptions{
		ContentType: "application/json",
	}); err != nil {
		return nil, fmt.Errorf("objstore: put queue message: %w", err)
	}
	meta.NextID = id + 1
	meta.MessageCount++
	meta.LastMessageID = id
	newBody, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	if _, err := q.blob.Put(ctx, metaKey, newBody, blob.PutOptions{
		ContentType: "application/json",
		IfMatch:     etag,
	}); err != nil {
		return nil, fmt.Errorf("objstore: bump queue meta: %w", err)
	}
	return &persistence.InternalEnqueueMessageResponse{
		Metadata: persistence.MessageMetadata{ID: id},
	}, nil
}

func (q *queueV2Store) ReadMessages(ctx context.Context, request *persistence.InternalReadMessagesRequest) (*persistence.InternalReadMessagesResponse, error) {
	infos, err := q.blob.List(ctx, queueMessagePrefix(request.QueueType, request.QueueName))
	if err != nil {
		return nil, fmt.Errorf("objstore: list queue messages: %w", err)
	}
	startID := int64(0)
	if len(request.NextPageToken) > 0 {
		if v, err := strconv.ParseInt(string(request.NextPageToken), 10, 64); err == nil {
			startID = v
		}
	}
	messages := make([]persistence.QueueV2Message, 0, request.PageSize)
	var nextToken []byte
	for _, info := range infos {
		id, ok := parseQueueMsgID(info.Key)
		if !ok || id < startID {
			continue
		}
		if request.PageSize > 0 && len(messages) >= request.PageSize {
			nextToken = []byte(strconv.FormatInt(id, 10))
			break
		}
		body, err := readBlobBody(ctx, q.blob, info.Key)
		if err != nil {
			return nil, fmt.Errorf("read queue message %s: %w", info.Key, err)
		}
		var msg queueMessageEnv
		if err := json.Unmarshal(body, &msg); err != nil {
			return nil, fmt.Errorf("unmarshal queue message: %w", err)
		}
		messages = append(messages, persistence.QueueV2Message{
			MetaData: persistence.MessageMetadata{ID: msg.ID},
			Data:     envToBlob(msg.Data),
		})
	}
	return &persistence.InternalReadMessagesResponse{
		Messages:      messages,
		NextPageToken: nextToken,
	}, nil
}

func (q *queueV2Store) RangeDeleteMessages(ctx context.Context, request *persistence.InternalRangeDeleteMessagesRequest) (*persistence.InternalRangeDeleteMessagesResponse, error) {
	infos, err := q.blob.List(ctx, queueMessagePrefix(request.QueueType, request.QueueName))
	if err != nil {
		return nil, fmt.Errorf("objstore: list for range delete: %w", err)
	}
	deleted := int64(0)
	for _, info := range infos {
		id, ok := parseQueueMsgID(info.Key)
		if !ok || id > request.InclusiveMaxMessageMetadata.ID {
			continue
		}
		if err := q.blob.Delete(ctx, info.Key, blob.DeleteOptions{}); err != nil {
			return nil, fmt.Errorf("delete %s: %w", info.Key, err)
		}
		deleted++
	}
	return &persistence.InternalRangeDeleteMessagesResponse{
		MessagesDeleted: deleted,
	}, nil
}

func (q *queueV2Store) ListQueues(ctx context.Context, request *persistence.InternalListQueuesRequest) (*persistence.InternalListQueuesResponse, error) {
	prefix := fmt.Sprintf("queues-v2/%d/", request.QueueType)
	infos, err := q.blob.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("objstore: list queues: %w", err)
	}
	// Each queue contributes one meta key + N message keys.
	// Aggregate by queueName from the meta key.
	queues := make([]persistence.QueueInfo, 0)
	for _, info := range infos {
		if !strings.HasSuffix(info.Key, "/meta") {
			continue
		}
		// Key shape: queues-v2/{type}/{name}/meta
		trimmed := strings.TrimPrefix(info.Key, prefix)
		trimmed = strings.TrimSuffix(trimmed, "/meta")
		body, err := readBlobBody(ctx, q.blob, info.Key)
		if err != nil {
			return nil, fmt.Errorf("read queue meta: %w", err)
		}
		var meta queueMetaEnv
		if err := json.Unmarshal(body, &meta); err != nil {
			return nil, fmt.Errorf("unmarshal queue meta: %w", err)
		}
		queues = append(queues, persistence.QueueInfo{
			QueueName:     trimmed,
			MessageCount:  meta.MessageCount,
			LastMessageID: meta.LastMessageID,
		})
	}
	return &persistence.InternalListQueuesResponse{Queues: queues}, nil
}

func (q *queueV2Store) readQueueMeta(ctx context.Context, key string) (*queueMetaEnv, string, error) {
	res, err := q.blob.Get(ctx, key)
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			// Auto-create on first enqueue (matches cassandra
			// behavior of accepting writes without an explicit
			// CreateQueue call).
			return &queueMetaEnv{NextID: 1, LastMessageID: -1}, "", nil
		}
		return nil, "", fmt.Errorf("objstore: read queue meta: %w", err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, "", err
	}
	var meta queueMetaEnv
	if err := json.Unmarshal(body, &meta); err != nil {
		return nil, "", fmt.Errorf("unmarshal queue meta: %w", err)
	}
	return &meta, res.ETag, nil
}

func parseQueueMsgID(key string) (int64, bool) {
	idx := strings.LastIndex(key, "/")
	if idx == -1 {
		return 0, false
	}
	tail := key[idx+1:]
	v, err := strconv.ParseInt(strings.TrimLeft(tail, "0"), 10, 64)
	if err != nil {
		if tail == "00000000000000000000" {
			return 0, true
		}
		return 0, false
	}
	return v, true
}
