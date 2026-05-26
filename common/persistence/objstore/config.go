// Package objstore is the persistence backend that stores all
// Temporal state in object storage (S3, GCS, MinIO, R2, B2, etc.).
//
// The package is wired into Temporal via the
// [client.AbstractDataStoreFactory] extension point. A
// `customDatastore` block in the persistence YAML names this
// backend and supplies the per-backend options:
//
//	persistence:
//	  defaultStore: default
//	  dataStores:
//	    default:
//	      customDatastore:
//	        name: objstore
//	        options:
//	          backend: s3
//	          bucket: temporal-state
//	          region: us-east-1
//	          endpoint: http://minio:9000
//	          accessKey: minio
//	          secret: secret
//	          pathStyle: true
//
// For tests and local dev, `backend: memfs` skips all I/O.
package objstore

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/objstore/blob"
	"go.temporal.io/server/common/persistence/objstore/blob/memfs"
	s3blob "go.temporal.io/server/common/persistence/objstore/blob/s3"
)

// Options is the parsed form of `CustomDatastoreConfig.Options` for
// this backend. Exported so it can be referenced in tests + downstream
// tooling that builds factories without going through the YAML path.
type Options struct {
	// Backend selects the blob.Store implementation. Required.
	// Supported: "s3", "memfs".
	Backend string

	// S3 options — only consulted when Backend == "s3".
	Bucket    string
	Region    string
	Endpoint  string
	AccessKey string
	Secret    string
	PathStyle bool
}

// parseOptions extracts an [Options] from the loosely-typed
// `map[string]any` Temporal hands a custom datastore at startup.
func parseOptions(raw map[string]any) (Options, error) {
	if raw == nil {
		return Options{}, errors.New("objstore: customDatastore.options is required")
	}
	out := Options{
		Backend:   stringOpt(raw, "backend"),
		Bucket:    stringOpt(raw, "bucket"),
		Region:    stringOpt(raw, "region"),
		Endpoint:  stringOpt(raw, "endpoint"),
		AccessKey: stringOpt(raw, "accessKey"),
		Secret:    stringOpt(raw, "secret"),
		PathStyle: boolOpt(raw, "pathStyle"),
	}
	if out.Backend == "" {
		return Options{}, errors.New("objstore: options.backend is required (s3 or memfs)")
	}
	return out, nil
}

func stringOpt(raw map[string]any, key string) string {
	v, ok := raw[key]
	if !ok || v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func boolOpt(raw map[string]any, key string) bool {
	v, ok := raw[key]
	if !ok || v == nil {
		return false
	}
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}

// newBlobStore builds a [blob.Store] from parsed [Options]. Exposed
// to tests; production callers go through the factory.
func newBlobStore(ctx context.Context, opts Options) (blob.Store, error) {
	switch opts.Backend {
	case "memfs":
		return memfs.New(), nil
	case "s3":
		return s3blob.New(ctx, s3blob.Config{
			Bucket:    opts.Bucket,
			Region:    opts.Region,
			Endpoint:  opts.Endpoint,
			AccessKey: opts.AccessKey,
			Secret:    opts.Secret,
			PathStyle: opts.PathStyle,
		})
	default:
		return nil, fmt.Errorf("objstore: unsupported backend %q (want one of: s3, memfs)", opts.Backend)
	}
}

// ConfigFromYAML is a helper for tests + main wiring that bypasses
// CustomDatastoreConfig and builds a config inline.
func ConfigFromYAML(name string, options map[string]any) config.CustomDatastoreConfig {
	return config.CustomDatastoreConfig{
		Name:    name,
		Options: options,
	}
}
