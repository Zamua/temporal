// Package s3 is the AWS S3 / S3-compatible adapter for [blob.Store].
//
// "S3-compatible" because aws-sdk-go-v2 + path-style addressing
// works against AWS S3, MinIO, Cloudflare R2, Backblaze B2, Wasabi,
// SeaweedFS, and any other API-conformant backend. The choice between
// them is purely a Config matter — the adapter code is identical.
//
// Why we don't share code with common/archiver/s3store: that package
// is the archival (cold-storage) backend for completed workflows.
// This is the primary persistence path, with stricter requirements:
// conditional PUT (CAS) for manifest pointer-swap, conditional DELETE
// for safe cleanup, paginated LIST. Different surface, different
// semantics; one shared API surface (blob.Store) and that's enough.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"go.temporal.io/server/common/persistence/objstore/blob"
)

// Config is everything the adapter needs to talk to an S3-compatible
// backend. Region defaults to "us-east-1" when empty (matches MinIO's
// default). AccessKey/Secret can be empty to let aws-sdk-go-v2 pull
// from the environment / instance profile / shared config.
type Config struct {
	Bucket    string
	Region    string
	Endpoint  string // empty → real AWS S3
	AccessKey string
	Secret    string

	// PathStyle forces path-style addressing (`https://endpoint/bucket/key`)
	// instead of virtual-hosted-style (`https://bucket.endpoint/key`).
	// Required for MinIO and most S3-compatible backends.
	PathStyle bool
}

// s3API is the subset of *s3.Client this adapter calls. Lets us
// substitute a mock in unit tests without depending on the full
// client surface.
type s3API interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

type store struct {
	client s3API
	bucket string
}

// New builds an [blob.Store] backed by AWS S3 (or an S3-compatible
// service). Returns an error if the config is missing required
// fields or if SDK config resolution fails.
func New(ctx context.Context, cfg Config) (blob.Store, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("s3: bucket is required")
	}
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(region),
	}
	if cfg.AccessKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.Secret, ""),
		))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("s3: load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.PathStyle
	})
	return &store{client: client, bucket: cfg.Bucket}, nil
}

// NewWithClient is the test seam — lets the contract test (or any
// mock) supply a custom s3API implementation.
func NewWithClient(client s3API, bucket string) blob.Store {
	return &store{client: client, bucket: bucket}
}

func (s *store) Put(ctx context.Context, key string, body []byte, opts blob.PutOptions) (blob.PutResult, error) {
	in := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	}
	if opts.ContentType != "" {
		in.ContentType = aws.String(opts.ContentType)
	}
	if opts.IfMatch != "" {
		in.IfMatch = aws.String(opts.IfMatch)
	}
	if opts.IfNoneMatch != "" {
		in.IfNoneMatch = aws.String(opts.IfNoneMatch)
	}

	out, err := s.client.PutObject(ctx, in)
	if err != nil {
		return blob.PutResult{}, translatePutError(err)
	}
	return blob.PutResult{ETag: aws.ToString(out.ETag)}, nil
}

func (s *store) Get(ctx context.Context, key string) (blob.GetResult, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return blob.GetResult{}, translateGetError(err)
	}
	return blob.GetResult{
		Body: out.Body,
		ETag: aws.ToString(out.ETag),
	}, nil
}

func (s *store) Delete(ctx context.Context, key string, opts blob.DeleteOptions) error {
	in := &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	if opts.IfMatch != "" {
		in.IfMatch = aws.String(opts.IfMatch)
	}
	_, err := s.client.DeleteObject(ctx, in)
	if err != nil {
		return translateDeleteError(err)
	}
	return nil
}

func (s *store) List(ctx context.Context, prefix string) ([]blob.ObjectInfo, error) {
	var (
		token *string
		out   []blob.ObjectInfo
	)
	for {
		page, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("s3: list %q: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			out = append(out, blob.ObjectInfo{
				Key:  aws.ToString(obj.Key),
				Size: aws.ToInt64(obj.Size),
				ETag: aws.ToString(obj.ETag),
			})
		}
		if aws.ToBool(page.IsTruncated) && page.NextContinuationToken != nil {
			token = page.NextContinuationToken
			continue
		}
		break
	}
	return out, nil
}

// closeBody drains+closes a response body — used in error paths so
// the http connection can be reused.
func closeBody(b io.ReadCloser) {
	if b == nil {
		return
	}
	_, _ = io.Copy(io.Discard, b)
	_ = b.Close()
}

// translatePutError maps S3 error responses to [blob] sentinels.
// S3 returns PreconditionFailed (HTTP 412) when If-Match fails, and
// either PreconditionFailed (412) or — historically — a 409 with code
// PreconditionFailed when If-None-Match=* hits an existing object.
// We treat both as ErrPreconditionFailed.
func translatePutError(err error) error {
	if status := httpStatus(err); status == http.StatusPreconditionFailed || status == http.StatusConflict {
		return blob.ErrPreconditionFailed
	}
	return fmt.Errorf("s3: put: %w", err)
}

func translateGetError(err error) error {
	var noSuchKey *s3types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return blob.ErrNotFound
	}
	if status := httpStatus(err); status == http.StatusNotFound {
		return blob.ErrNotFound
	}
	return fmt.Errorf("s3: get: %w", err)
}

func translateDeleteError(err error) error {
	if status := httpStatus(err); status == http.StatusPreconditionFailed {
		return blob.ErrPreconditionFailed
	}
	return fmt.Errorf("s3: delete: %w", err)
}

// httpStatus pulls the HTTP status code out of a wrapped smithy
// HTTP error response. Returns 0 if the error isn't HTTP-shaped.
func httpStatus(err error) int {
	var rerr *smithyhttp.ResponseError
	if errors.As(err, &rerr) && rerr.Response != nil {
		defer closeBody(rerr.Response.Body)
		return rerr.HTTPStatusCode()
	}
	return 0
}
