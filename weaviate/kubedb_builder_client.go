package weaviate

import (
	"context"
	"fmt"

	weaviate "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/auth"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"kubedb.dev/apimachinery/apis/kubedb"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc         client.Client
	db         *api.Weaviate
	url        string
	podName    string
	ctx        context.Context
	authConfig auth.Config
}

func (o *KubeDBClientBuilder) WithAPIKey(apiKey string) *KubeDBClientBuilder {
	o.authConfig = auth.ApiKey{Value: apiKey}
	return o
}

func (o *KubeDBClientBuilder) WithAuth() *KubeDBClientBuilder {
	if o.db.Spec.DisableSecurity {
		return o
	}
	if o.ctx == nil {
		o.ctx = context.Background()
	}
	secret := &v1.Secret{}
	err := o.kc.Get(o.ctx, types.NamespacedName{Name: o.db.GetAuthSecretName(), Namespace: o.db.Namespace}, secret)
	if err != nil {
		klog.Errorf("Failed to get auth secret: %v", err)
		return o // Error handled in caller
	}
	// Fetch the correct key for Weaviate API key auth (matches secret creation)
	apiKey, ok := secret.Data["AUTHENTICATION_APIKEY_ALLOWED_KEYS"]
	if !ok || len(apiKey) == 0 {
		klog.Errorf("Missing or empty AUTHENTICATION_APIKEY_ALLOWED_KEYS in secret")
		return o // Error handled in caller
	}
	o.authConfig = auth.ApiKey{Value: string(apiKey)}
	return o
}

func NewKubeDBClientBuilder(kc client.Client, db *api.Weaviate) *KubeDBClientBuilder {
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

func (o *KubeDBClientBuilder) GetWeaviateClient() (*weaviate.Client, error) {
	if o.ctx == nil {
		o.ctx = context.Background()
	}

	if o.db == nil {
		return nil, fmt.Errorf("weaviate CR is nil")
	}

	addr := o.url
	if addr == "" {
		addr = fmt.Sprintf("localhost:%d", 8082)
	}

	secret := &v1.Secret{}
	if err := o.kc.Get(o.ctx, client.ObjectKey{
		Namespace: o.db.Namespace,
		Name:      o.db.Spec.AuthSecret.Name,
	}, secret); err != nil {
		return nil, fmt.Errorf("failed to get weaviate auth secret: %w", err)
	}

	val, _ := secret.Data[kubedb.WeaviateAPIKey]
	cfg := auth.ApiKey{Value: string(val)}

	config := &weaviate.Config{
		Host:       addr,
		Scheme:     "http", // "https" if TLS
		AuthConfig: cfg,
	}

	// Create client
	weaviateClient, err := weaviate.NewClient(*config)

	if err != nil {
		return nil, fmt.Errorf("failed to test Weaviate client connection: %w", err)
	}

	return weaviateClient, nil
}

func (o *KubeDBClientBuilder) GetServiceAddress() string {
	if o.url != "" {
		return o.url
	}

	serviceName := o.db.ServiceName()
	namespace := o.db.Namespace
	return fmt.Sprintf("%s.%s.svc.cluster.local", serviceName, namespace)
}
