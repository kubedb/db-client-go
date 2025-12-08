package milvus

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	core "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc  client.Client
	db  *api.Milvus
	ctx context.Context
}

func NewKubeDBClientBuilder(kc client.Client, db *api.Milvus) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc: kc,
		db: db,
	}
}

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}

func (o *KubeDBClientBuilder) GetMilvusClient() (*milvusclient.Client, error) {
	if o.ctx == nil {
		o.ctx = context.Background()
	}

	addr := o.db.ServiceDNS()

	config := &milvusclient.ClientConfig{
		Address: addr,
	}

	if !o.db.Spec.DisableSecurity {
		if o.db.Spec.AuthSecret == nil {
			return nil, fmt.Errorf("auth secret is not specified")
		}

		secretName := o.db.GetAuthSecretName()
		var secret core.Secret
		if err := o.kc.Get(o.ctx, client.ObjectKey{
			Namespace: o.db.Namespace,
			Name:      secretName,
		}, &secret); err != nil {
			return nil, fmt.Errorf("failed to get auth secret %q: %w", secretName, err)
		}

		user, ok := secret.Data[core.BasicAuthUsernameKey]
		if !ok {
			return nil, fmt.Errorf("username is missing in secret %q", secretName)
		}

		pass, ok := secret.Data[core.BasicAuthPasswordKey]
		if !ok {
			return nil, fmt.Errorf("password is missing in secret %q", secretName)
		}

		config.Username = string(user)
		config.Password = string(pass)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(config.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		klog.Warningf("gRPC dial failed: %v", err)
		return nil, err
	}
	defer conn.Close() // nolint:errcheck
	conn.Close()       // nolint:errcheck

	client, err := milvusclient.New(ctx, config)
	if err != nil {
		klog.Warningf("Failed to create Milvus client: %v", err)
		return nil, err
	}

	return client, nil
}
