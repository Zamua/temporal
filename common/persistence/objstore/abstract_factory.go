package objstore

import (
	"context"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resolver"
)

// AbstractFactory is the objstore implementation of
// [client.AbstractDataStoreFactory]. Wire it into the dependency
// graph (via fx) so that a `customDatastore` block in the
// persistence YAML routes to objstore.
//
// Typical wiring (in cmd/server or a custom main):
//
//	app := fx.New(
//	  fx.Provide(func() client.AbstractDataStoreFactory {
//	    return objstore.NewAbstractFactory()
//	  }),
//	  ...
//	)
type AbstractFactory struct{}

// NewAbstractFactory constructs the abstract factory. Stateless;
// the per-instance state lives on the [Factory] this returns
// from NewFactory.
func NewAbstractFactory() *AbstractFactory {
	return &AbstractFactory{}
}

// NewFactory implements [client.AbstractDataStoreFactory]. Called
// once at server startup with the parsed `customDatastore` block.
// Returns a [persistence.DataStoreFactory] whose stores are wired
// to the configured blob backend.
func (a *AbstractFactory) NewFactory(
	cfg config.CustomDatastoreConfig,
	r resolver.ServiceResolver,
	clusterName string,
	logger log.Logger,
	metricsHandler metrics.Handler,
	serializer serialization.Serializer,
) persistence.DataStoreFactory {
	_ = r
	_ = metricsHandler

	opts, err := parseOptions(cfg.Options)
	if err != nil {
		logger.Fatal("objstore: invalid customDatastore.options", tag.Error(err))
	}
	store, err := newBlobStore(context.Background(), opts)
	if err != nil {
		logger.Fatal("objstore: failed to construct blob.Store",
			tag.NewStringTag("backend", opts.Backend),
			tag.Error(err))
	}
	logger.Info("objstore: persistence factory initialized",
		tag.NewStringTag("backend", opts.Backend),
		tag.NewStringTag("bucket", opts.Bucket))
	return NewFactoryWithSerializer(store, clusterName, logger, serializer)
}

// Verify at compile-time that AbstractFactory satisfies the
// extension-point interface.
var _ client.AbstractDataStoreFactory = (*AbstractFactory)(nil)
