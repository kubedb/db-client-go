package milvus

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
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

	if o.db == nil {
		klog.Errorf("Milvus CR is nil")
		return nil, fmt.Errorf("milvus CR is nil")
	}

	if o.db.Spec.Standalone == nil {
		return nil, fmt.Errorf("standalone spec is nil")
	}

	addr := o.db.GetGRPCAddress()

	var username, password string
	if !o.db.Spec.Standalone.DisableSecurity {
		var err error
		username, err = o.db.GetUsername(o.ctx, o.kc)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read username from auth secret")
		}
		password, err = o.db.GetPassword(o.ctx, o.kc)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read password from auth secret")
		}
	}

	config := &milvusclient.ClientConfig{
		Address:  addr,
		Username: username,
		Password: password,
	}

	// store the client outside the retry function
	var milvusClient *milvusclient.Client

	operation := func() error {
		// Step 1: Dial
		conn, err := grpc.Dial(config.Address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithTimeout(10*time.Second),
		)
		if err != nil {
			klog.Warningf("gRPC dial failed: %v", err)
			return err
		}
		conn.Close()

		// Step 2: Create client
		ctx, cancel := context.WithTimeout(o.ctx, 30*time.Second)
		defer cancel()

		client, err := milvusclient.New(ctx, config)
		if err != nil {
			klog.Warningf("Failed to create Milvus client: %v", err)
			return err
		}

		// Step 3: Test connection
		_, err = client.ListCollections(ctx, milvusclient.NewListCollectionOption())
		if err != nil {
			client.Close(ctx)
			klog.Warningf("Failed to list collections: %v", err)
			return err
		}

		// success
		milvusClient = client
		return nil
	}

	// Exponential backoff: max 5 mins
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 2 * time.Second
	b.MaxInterval = 30 * time.Second
	b.MaxElapsedTime = 5 * time.Minute

	err := backoff.Retry(operation, b)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Milvus after retry: %w", err)
	}

	return milvusClient, nil
}
