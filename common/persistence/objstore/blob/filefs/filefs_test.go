package filefs_test

import (
	"path/filepath"
	"testing"

	"go.temporal.io/server/common/persistence/objstore/blob"
	"go.temporal.io/server/common/persistence/objstore/blob/blobtest"
	"go.temporal.io/server/common/persistence/objstore/blob/filefs"
)

func TestContract(t *testing.T) {
	blobtest.RunContract(t, func(t *testing.T) (blob.Store, func()) {
		root := filepath.Join(t.TempDir(), "filefs")
		store, err := filefs.New(root)
		if err != nil {
			t.Fatalf("filefs.New: %v", err)
		}
		return store, func() {}
	})
}
