package milvus

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	var username, password string

	config := &milvusclient.ClientConfig{
		Address: addr,
	}

	// Case 1: Security disabled
	if !o.db.Spec.DisableSecurity {
		if o.db.Spec.AuthSecret != nil && o.db.Spec.AuthSecret.ExternallyManaged {
			// Case 2: Auth externally managed → fetch from Secret
			var err error
			username, err = o.db.GetUsername(o.ctx, o.kc)
			if err != nil {
				return nil, errors.Wrap(err, "failed to read username from auth secret")
			}
			password, err = o.db.GetPassword(o.ctx, o.kc)
			if err != nil {
				return nil, errors.Wrap(err, "failed to read password from auth secret")
			}
			config.Username = username
			config.Password = password
		}
	}
	// store the client outside the retry function
	var milvusClient *milvusclient.Client

	// Step 1: Dial
	conn, err := grpc.Dial(config.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(10*time.Second),
	)
	if err != nil {
		klog.Warningf("gRPC dial failed: %v", err)
		return nil, err
	}
	conn.Close()

	// Step 2: Create client
	ctx, cancel := context.WithTimeout(o.ctx, 30*time.Second)
	defer cancel()

	client, err := milvusclient.New(ctx, config)
	if err != nil {
		klog.Warningf("Failed to create Milvus client: %v", err)
		return nil, err
	}

	milvusClient = client
	return milvusClient, nil
}
