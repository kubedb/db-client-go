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
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"kubedb.dev/apimachinery/apis/kubedb"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc      client.Client
	db      *api.Milvus
	url     string
	podName string
	ctx     context.Context
}

func NewKubeDBClientBuilder(kc client.Client, db *api.Milvus) *KubeDBClientBuilder {
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

	grpcPort := int32(19530)
	if o.db.Spec.Standalone.GRPCPort != nil {
		grpcPort = *o.db.Spec.Standalone.GRPCPort
	}

	addr := o.url
	if addr == "" {
		addr = fmt.Sprintf("localhost:%d", grpcPort)
	}

	config := &milvusclient.ClientConfig{
		Address: addr,
	}

	if !o.db.Spec.Standalone.DisableSecurity {
		secret := &core.Secret{}
		err := o.kc.Get(o.ctx, types.NamespacedName{
			Name:      o.db.Spec.Standalone.AuthSecret.Name,
			Namespace: o.db.Namespace,
		}, secret)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get auth secret %s", o.db.Spec.Standalone.AuthSecret.Name)
		}

		username, ok := secret.Data[kubedb.MilvusUsernameKey]
		if !ok || len(username) == 0 {
			return nil, fmt.Errorf("username missing in auth secret %s", o.db.Spec.Standalone.AuthSecret.Name)
		}
		password, ok := secret.Data[kubedb.MilvusPasswordKey]
		if !ok || len(password) == 0 {
			return nil, fmt.Errorf("password missing in auth secret %s", o.db.Spec.Standalone.AuthSecret.Name)
		}
		config.Username = string(username)
		config.Password = string(password)
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
