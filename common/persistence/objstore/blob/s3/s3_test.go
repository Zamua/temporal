package s3_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"go.temporal.io/server/common/persistence/objstore/blob"
	s3adapter "go.temporal.io/server/common/persistence/objstore/blob/s3"
)

// TestErrorTranslation drives the adapter against a tiny scripted
// mock that returns the exact AWS error shapes we care about, and
// asserts the [blob] sentinels come out the other side. The S3
// integration test (against MinIO) lives behind a build tag — see
// TestContractMinIO in s3_minio_test.go.
func TestErrorTranslation(t *testing.T) {
	t.Run("GetReturnsNotFoundForNoSuchKey", func(t *testing.T) {
		store := s3adapter.NewWithClient(&mockS3{
			getErr: &s3types.NoSuchKey{},
		}, "b")
		_, err := store.Get(context.Background(), "k")
		if !errors.Is(err, blob.ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("GetReturnsNotFoundFor404", func(t *testing.T) {
		store := s3adapter.NewWithClient(&mockS3{
			getErr: httpErr(http.StatusNotFound),
		}, "b")
		_, err := store.Get(context.Background(), "k")
		if !errors.Is(err, blob.ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("PutReturnsPreconditionFailedFor412", func(t *testing.T) {
		store := s3adapter.NewWithClient(&mockS3{
			putErr: httpErr(http.StatusPreconditionFailed),
		}, "b")
		_, err := store.Put(context.Background(), "k", []byte("x"), blob.PutOptions{IfMatch: "\"etag\""})
		if !errors.Is(err, blob.ErrPreconditionFailed) {
			t.Fatalf("expected ErrPreconditionFailed, got %v", err)
		}
	})

	t.Run("PutIfNoneMatchStarReturnsPreconditionFailedFor409", func(t *testing.T) {
		// Older S3-compatible backends return 409 instead of 412 for
		// If-None-Match=* collisions; the adapter accepts either.
		store := s3adapter.NewWithClient(&mockS3{
			putErr: httpErr(http.StatusConflict),
		}, "b")
		_, err := store.Put(context.Background(), "k", []byte("x"), blob.PutOptions{IfNoneMatch: "*"})
		if !errors.Is(err, blob.ErrPreconditionFailed) {
			t.Fatalf("expected ErrPreconditionFailed, got %v", err)
		}
	})

	t.Run("DeleteReturnsPreconditionFailedFor412", func(t *testing.T) {
		store := s3adapter.NewWithClient(&mockS3{
			deleteErr: httpErr(http.StatusPreconditionFailed),
		}, "b")
		err := store.Delete(context.Background(), "k", blob.DeleteOptions{IfMatch: "\"etag\""})
		if !errors.Is(err, blob.ErrPreconditionFailed) {
			t.Fatalf("expected ErrPreconditionFailed, got %v", err)
		}
	})

	t.Run("PutSurfacesUnknownErrorsAsWrapped", func(t *testing.T) {
		boom := errors.New("network exploded")
		store := s3adapter.NewWithClient(&mockS3{putErr: boom}, "b")
		_, err := store.Put(context.Background(), "k", []byte("x"), blob.PutOptions{})
		if errors.Is(err, blob.ErrPreconditionFailed) || errors.Is(err, blob.ErrNotFound) {
			t.Fatalf("non-status errors must not get mapped to sentinels, got %v", err)
		}
		if !errors.Is(err, boom) {
			t.Fatalf("expected wrapped underlying error, got %v", err)
		}
	})
}

// TestListPaginationCombinesPages verifies the adapter follows
// continuation tokens transparently — the higher-level stores
// expect List to be single-call.
func TestListPaginationCombinesPages(t *testing.T) {
	page1Truncated := true
	page1 := &s3.ListObjectsV2Output{
		Contents: []s3types.Object{
			{Key: strPtr("k/1"), Size: int64Ptr(1), ETag: strPtr("\"e1\"")},
		},
		IsTruncated:           &page1Truncated,
		NextContinuationToken: strPtr("page2"),
	}
	page2Truncated := false
	page2 := &s3.ListObjectsV2Output{
		Contents: []s3types.Object{
			{Key: strPtr("k/2"), Size: int64Ptr(2), ETag: strPtr("\"e2\"")},
		},
		IsTruncated: &page2Truncated,
	}

	mock := &mockS3{
		listResponses: []*s3.ListObjectsV2Output{page1, page2},
	}
	store := s3adapter.NewWithClient(mock, "b")
	got, err := store.List(context.Background(), "k/")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 2 || got[0].Key != "k/1" || got[1].Key != "k/2" {
		t.Fatalf("expected both pages merged, got %+v", got)
	}
	if mock.listCalls != 2 {
		t.Fatalf("expected 2 list calls (follow continuation), got %d", mock.listCalls)
	}
}

// --- mock ---

type mockS3 struct {
	putErr    error
	getErr    error
	deleteErr error

	listResponses []*s3.ListObjectsV2Output
	listCalls     int
}

func (m *mockS3) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if m.putErr != nil {
		return nil, m.putErr
	}
	etag := "\"new-etag\""
	return &s3.PutObjectOutput{ETag: &etag}, nil
}

func (m *mockS3) GetObject(ctx context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	etag := "\"e\""
	return &s3.GetObjectOutput{
		Body: io.NopCloser(nilReader{}),
		ETag: &etag,
	}, nil
}

func (m *mockS3) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if m.deleteErr != nil {
		return nil, m.deleteErr
	}
	return &s3.DeleteObjectOutput{}, nil
}

func (m *mockS3) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	idx := m.listCalls
	m.listCalls++
	if idx >= len(m.listResponses) {
		return &s3.ListObjectsV2Output{}, nil
	}
	return m.listResponses[idx], nil
}

type nilReader struct{}

func (nilReader) Read(p []byte) (int, error) { return 0, io.EOF }

func httpErr(code int) error {
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{
				StatusCode: code,
				Body:       io.NopCloser(nilReader{}),
			},
		},
		Err: errors.New(http.StatusText(code)),
	}
}

func strPtr(s string) *string { return &s }
func int64Ptr(i int64) *int64 { return &i }
