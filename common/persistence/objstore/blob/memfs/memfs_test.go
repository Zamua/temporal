package memfs_test

import (
	"testing"

	"go.temporal.io/server/common/persistence/objstore/blob"
	"go.temporal.io/server/common/persistence/objstore/blob/blobtest"
	"go.temporal.io/server/common/persistence/objstore/blob/memfs"
)

func TestContract(t *testing.T) {
	blobtest.RunContract(t, func(t *testing.T) (blob.Store, func()) {
		return memfs.New(), func() {}
	})
}
