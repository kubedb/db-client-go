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
	kc      client.Client
	db      *api.Qdrant
	url     string
	podName string
	ctx     context.Context
}

func NewKubeDBClientBuilder(kc client.Client, db *api.Qdrant) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc: kc,
		db: db,
	}
}

func (o *KubeDBClientBuilder) WithURL(url string) *KubeDBClientBuilder {
	o.url = url
	return o
}

func (o *KubeDBClientBuilder) WithPod(podName string) *KubeDBClientBuilder {
	o.podName = podName
	return o
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
		Host: "localhost",
		Port: kubedb.QdrantGRPCPort,
	}

	qdrantClient, err := qdrant.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Qdrant client: %w", err)
	}

	return qdrantClient, nil
}

func (o *KubeDBClientBuilder) GetServiceAddress() string {
	if o.url != "" {
		return o.url
	}

	serviceName := o.db.ServiceName()
	namespace := o.db.Namespace
	return fmt.Sprintf("%s.%s.svc.cluster.local", serviceName, namespace)
}
