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

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/objstore/blob"
)

// queueV1Store implements [persistence.Queue] — the legacy queue
// interface that the NamespaceReplicationQueue still uses. Storage
// layout:
//
//	queues-v1/{type}/messages/{id:020d}   → message + DLQ messages
//	queues-v1/{type}/metadata             → ack levels
//	queues-v1/{type}/dlq-messages/{id:020d}
//	queues-v1/{type}/dlq-metadata
type queueV1Store struct {
	blob      blob.Store
	queueType persistence.QueueType

	mu sync.Mutex // serialize within process; CAS handles cross-process
}

func newQueueV1Store(b blob.Store, qt persistence.QueueType) persistence.Queue {
	return &queueV1Store{blob: b, queueType: qt}
}

func (q *queueV1Store) Close() {}

type queueV1MessageEnv struct {
	ID       int64  `json:"id"`
	Data     []byte `json:"d"`
	Encoding string `json:"e,omitempty"`
}

type queueV1Meta struct {
	NextID   int64                          `json:"next"`
	AckLevel int64                          `json:"ack"`
	Clusters map[string]int64               `json:"clusters,omitempty"`
	Extra    map[string]map[string][]byte   `json:"extra,omitempty"`
	Blob     map[string]*persistedDataBlob  `json:"blob,omitempty"`
}

type persistedDataBlob struct {
	Encoding string `json:"e,omitempty"`
	Data     []byte `json:"d,omitempty"`
}

func qv1Prefix(qt persistence.QueueType) string {
	return fmt.Sprintf("queues-v1/%d/", qt)
}

func qv1MessagesPrefix(qt persistence.QueueType) string {
	return qv1Prefix(qt) + "messages/"
}

func qv1MessageKey(qt persistence.QueueType, id int64) string {
	return fmt.Sprintf("%s%020d", qv1MessagesPrefix(qt), id)
}

func qv1DLQPrefix(qt persistence.QueueType) string {
	return qv1Prefix(qt) + "dlq-messages/"
}

func qv1DLQMessageKey(qt persistence.QueueType, id int64) string {
	return fmt.Sprintf("%s%020d", qv1DLQPrefix(qt), id)
}

func qv1MetaKey(qt persistence.QueueType) string     { return qv1Prefix(qt) + "metadata" }
func qv1DLQMetaKey(qt persistence.QueueType) string  { return qv1Prefix(qt) + "dlq-metadata" }

func (q *queueV1Store) Init(ctx context.Context, blobInfo *commonpb.DataBlob) error {
	// Initialize the metadata blob if it doesn't exist.
	_, _, err := q.readMeta(ctx, qv1MetaKey(q.queueType))
	if err == nil {
		return nil
	}
	if !errors.Is(err, blob.ErrNotFound) {
		return err
	}
	meta := queueV1Meta{NextID: 1}
	body, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	_, err = q.blob.Put(ctx, qv1MetaKey(q.queueType), body, blob.PutOptions{
		ContentType: "application/json",
	})
	return err
}

func (q *queueV1Store) EnqueueMessage(ctx context.Context, message *commonpb.DataBlob) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	meta, etag, err := q.readMeta(ctx, qv1MetaKey(q.queueType))
	if err != nil && !errors.Is(err, blob.ErrNotFound) {
		return err
	}
	if meta == nil {
		meta = &queueV1Meta{NextID: 1}
	}
	id := meta.NextID
	env := queueV1MessageEnv{
		ID:       id,
		Data:     message.Data,
		Encoding: message.EncodingType.String(),
	}
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	if _, err := q.blob.Put(ctx, qv1MessageKey(q.queueType, id), body, blob.PutOptions{
		ContentType: "application/json",
	}); err != nil {
		return err
	}
	meta.NextID = id + 1
	return q.writeMeta(ctx, qv1MetaKey(q.queueType), meta, etag)
}

func (q *queueV1Store) ReadMessages(ctx context.Context, lastMessageID int64, maxCount int) ([]*persistence.QueueMessage, error) {
	infos, err := q.blob.List(ctx, qv1MessagesPrefix(q.queueType))
	if err != nil {
		return nil, err
	}
	out := make([]*persistence.QueueMessage, 0, len(infos))
	for _, info := range infos {
		id, ok := parseQueueV1ID(info.Key)
		if !ok || id <= lastMessageID {
			continue
		}
		if maxCount > 0 && len(out) >= maxCount {
			break
		}
		body, err := readBlobBody(ctx, q.blob, info.Key)
		if err != nil {
			return nil, err
		}
		var env queueV1MessageEnv
		if err := json.Unmarshal(body, &env); err != nil {
			return nil, err
		}
		out = append(out, &persistence.QueueMessage{
			QueueType: q.queueType,
			ID:        env.ID,
			Data:      env.Data,
			Encoding:  env.Encoding,
		})
	}
	return out, nil
}

func (q *queueV1Store) DeleteMessagesBefore(ctx context.Context, messageID int64) error {
	infos, err := q.blob.List(ctx, qv1MessagesPrefix(q.queueType))
	if err != nil {
		return err
	}
	for _, info := range infos {
		id, ok := parseQueueV1ID(info.Key)
		if !ok || id >= messageID {
			continue
		}
		_ = q.blob.Delete(ctx, info.Key, blob.DeleteOptions{})
	}
	return nil
}

func (q *queueV1Store) UpdateAckLevel(ctx context.Context, metadata *persistence.InternalQueueMetadata) error {
	meta, etag, err := q.readMeta(ctx, qv1MetaKey(q.queueType))
	if err != nil && !errors.Is(err, blob.ErrNotFound) {
		return err
	}
	if meta == nil {
		meta = &queueV1Meta{NextID: 1}
	}
	if metadata.Blob != nil {
		if meta.Blob == nil {
			meta.Blob = make(map[string]*persistedDataBlob)
		}
		meta.Blob["ack"] = &persistedDataBlob{
			Encoding: metadata.Blob.EncodingType.String(),
			Data:     metadata.Blob.Data,
		}
	}
	return q.writeMeta(ctx, qv1MetaKey(q.queueType), meta, etag)
}

func (q *queueV1Store) GetAckLevels(ctx context.Context) (*persistence.InternalQueueMetadata, error) {
	meta, _, err := q.readMeta(ctx, qv1MetaKey(q.queueType))
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return &persistence.InternalQueueMetadata{}, nil
		}
		return nil, err
	}
	out := &persistence.InternalQueueMetadata{}
	if meta != nil && meta.Blob != nil && meta.Blob["ack"] != nil {
		enc, _ := enumspb.EncodingTypeFromString(meta.Blob["ack"].Encoding)
		out.Blob = &commonpb.DataBlob{
			EncodingType: enc,
			Data:         meta.Blob["ack"].Data,
		}
	}
	return out, nil
}

// --- DLQ ---

func (q *queueV1Store) EnqueueMessageToDLQ(ctx context.Context, message *commonpb.DataBlob) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	meta, etag, err := q.readMeta(ctx, qv1DLQMetaKey(q.queueType))
	if err != nil && !errors.Is(err, blob.ErrNotFound) {
		return 0, err
	}
	if meta == nil {
		meta = &queueV1Meta{NextID: 1}
	}
	id := meta.NextID
	env := queueV1MessageEnv{
		ID:       id,
		Data:     message.Data,
		Encoding: message.EncodingType.String(),
	}
	body, err := json.Marshal(env)
	if err != nil {
		return 0, err
	}
	if _, err := q.blob.Put(ctx, qv1DLQMessageKey(q.queueType, id), body, blob.PutOptions{
		ContentType: "application/json",
	}); err != nil {
		return 0, err
	}
	meta.NextID = id + 1
	if err := q.writeMeta(ctx, qv1DLQMetaKey(q.queueType), meta, etag); err != nil {
		return 0, err
	}
	return id, nil
}

func (q *queueV1Store) ReadMessagesFromDLQ(ctx context.Context, firstMessageID int64, lastMessageID int64, pageSize int, pageToken []byte) ([]*persistence.QueueMessage, []byte, error) {
	infos, err := q.blob.List(ctx, qv1DLQPrefix(q.queueType))
	if err != nil {
		return nil, nil, err
	}
	startID := firstMessageID
	if len(pageToken) > 0 {
		if v, err := strconv.ParseInt(string(pageToken), 10, 64); err == nil {
			startID = v
		}
	}
	out := make([]*persistence.QueueMessage, 0, pageSize)
	var nextToken []byte
	for _, info := range infos {
		id, ok := parseQueueV1ID(info.Key)
		if !ok || id < startID || id >= lastMessageID {
			continue
		}
		if pageSize > 0 && len(out) >= pageSize {
			nextToken = []byte(strconv.FormatInt(id, 10))
			break
		}
		body, err := readBlobBody(ctx, q.blob, info.Key)
		if err != nil {
			return nil, nil, err
		}
		var env queueV1MessageEnv
		if err := json.Unmarshal(body, &env); err != nil {
			return nil, nil, err
		}
		out = append(out, &persistence.QueueMessage{
			QueueType: -q.queueType,
			ID:        env.ID,
			Data:      env.Data,
			Encoding:  env.Encoding,
		})
	}
	return out, nextToken, nil
}

func (q *queueV1Store) DeleteMessageFromDLQ(ctx context.Context, messageID int64) error {
	return q.blob.Delete(ctx, qv1DLQMessageKey(q.queueType, messageID), blob.DeleteOptions{})
}

func (q *queueV1Store) RangeDeleteMessagesFromDLQ(ctx context.Context, firstMessageID int64, lastMessageID int64) error {
	infos, err := q.blob.List(ctx, qv1DLQPrefix(q.queueType))
	if err != nil {
		return err
	}
	for _, info := range infos {
		id, ok := parseQueueV1ID(info.Key)
		if !ok || id < firstMessageID || id > lastMessageID {
			continue
		}
		_ = q.blob.Delete(ctx, info.Key, blob.DeleteOptions{})
	}
	return nil
}

func (q *queueV1Store) UpdateDLQAckLevel(ctx context.Context, metadata *persistence.InternalQueueMetadata) error {
	meta, etag, err := q.readMeta(ctx, qv1DLQMetaKey(q.queueType))
	if err != nil && !errors.Is(err, blob.ErrNotFound) {
		return err
	}
	if meta == nil {
		meta = &queueV1Meta{NextID: 1}
	}
	if metadata.Blob != nil {
		if meta.Blob == nil {
			meta.Blob = make(map[string]*persistedDataBlob)
		}
		meta.Blob["ack"] = &persistedDataBlob{
			Encoding: metadata.Blob.EncodingType.String(),
			Data:     metadata.Blob.Data,
		}
	}
	return q.writeMeta(ctx, qv1DLQMetaKey(q.queueType), meta, etag)
}

func (q *queueV1Store) GetDLQAckLevels(ctx context.Context) (*persistence.InternalQueueMetadata, error) {
	meta, _, err := q.readMeta(ctx, qv1DLQMetaKey(q.queueType))
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return &persistence.InternalQueueMetadata{}, nil
		}
		return nil, err
	}
	out := &persistence.InternalQueueMetadata{}
	if meta != nil && meta.Blob != nil && meta.Blob["ack"] != nil {
		enc, _ := enumspb.EncodingTypeFromString(meta.Blob["ack"].Encoding)
		out.Blob = &commonpb.DataBlob{
			EncodingType: enc,
			Data:         meta.Blob["ack"].Data,
		}
	}
	return out, nil
}

// --- helpers ---

func (q *queueV1Store) readMeta(ctx context.Context, key string) (*queueV1Meta, string, error) {
	res, err := q.blob.Get(ctx, key)
	if err != nil {
		return nil, "", err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, "", err
	}
	var env queueV1Meta
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, "", err
	}
	return &env, res.ETag, nil
}

func (q *queueV1Store) writeMeta(ctx context.Context, key string, meta *queueV1Meta, etag string) error {
	body, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	opts := blob.PutOptions{ContentType: "application/json"}
	if etag != "" {
		opts.IfMatch = etag
	}
	_, err = q.blob.Put(ctx, key, body, opts)
	return err
}

func parseQueueV1ID(key string) (int64, bool) {
	idx := strings.LastIndex(key, "/")
	if idx == -1 {
		return 0, false
	}
	tail := key[idx+1:]
	trimmed := strings.TrimLeft(tail, "0")
	if trimmed == "" {
		return 0, true
	}
	v, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}
