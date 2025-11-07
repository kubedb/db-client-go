package qdrant

import (
	"context"
	"fmt"

	"github.com/qdrant/go-client/qdrant"
	"kubedb.dev/apimachinery/apis/kubedb"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc  client.Client
	db  *api.Qdrant
	ctx context.Context
}

func NewKubeDBClientBuilder(kc client.Client, db *api.Qdrant) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc: kc,
		db: db,
	}
}

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}

func (o *KubeDBClientBuilder) GetQdrantClient() (*qdrant.Client, error) {
	if o.ctx == nil {
		o.ctx = context.Background()
	}

	config := &qdrant.Config{
		Host:   o.db.ServiceDNS(),
		Port:   kubedb.QdrantGRPCPort,
		APIKey: o.db.GetAPIKey(o.ctx, o.kc),
	}

	qdrantClient, err := qdrant.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Qdrant client: %w", err)
	}

	return qdrantClient, nil
}
