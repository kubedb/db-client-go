/*
Copyright AppsCode Inc. and Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
		return o
	}
	// Fetch the correct key for Weaviate API key auth (matches secret creation)
	apiKey, ok := secret.Data[kubedb.WeaviateAPIKey]
	if !ok || len(apiKey) == 0 {
		klog.Errorf("Missing or empty AUTHENTICATION_APIKEY_ALLOWED_KEYS in secret")
		return o
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
		addr = fmt.Sprintf("%s:%d", o.GetServiceAddress(), kubedb.WeaviateHTTPPort)
	}

	secret := &v1.Secret{}
	if err := o.kc.Get(o.ctx, client.ObjectKey{
		Namespace: o.db.Namespace,
		Name:      o.db.Spec.AuthSecret.Name,
	}, secret); err != nil {
		return nil, fmt.Errorf("failed to get weaviate auth secret: %w", err)
	}

	val, ok := secret.Data[kubedb.WeaviateAPIKey]
	if !ok || len(val) == 0 {
		return nil, fmt.Errorf("weaviate auth secret key %s is missing or empty", kubedb.WeaviateAPIKey)
	}
	cfg := auth.ApiKey{Value: string(val)}
	wvconfig := &weaviate.Config{
		Host:       addr,
		Scheme:     o.db.GetConnectionScheme(),
		AuthConfig: cfg,
	}

	// Create client
	weaviateClient, err := weaviate.NewClient(*wvconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Weaviate client: %w", err)
	}
	return weaviateClient, nil
}

func (o *KubeDBClientBuilder) GetServiceAddress() string {
	if o.url != "" {
		return o.url
	}
	return fmt.Sprintf("%s.%s.svc.cluster.local", o.db.ServiceName(), o.db.GetNamespace())
}
