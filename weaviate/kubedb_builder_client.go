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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"

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
		port := kubedb.WeaviateHTTPPort
		if o.db.Spec.TLS != nil {
			port = kubedb.WeaviateHTTPSPort
		}
		addr = fmt.Sprintf("%s:%d", o.GetServiceAddress(), port)
	}

	wvconfig := &weaviate.Config{
		Host:   addr,
		Scheme: o.db.GetConnectionScheme(),
	}

	apiKey, err := o.getAPIKey()
	if err != nil {
		return nil, err
	}
	if o.db.Spec.TLS != nil {
		httpClient, err := o.getHTTPClient(apiKey)
		if err != nil {
			return nil, err
		}
		wvconfig.ConnectionClient = httpClient
	} else if apiKey != "" {
		wvconfig.AuthConfig = auth.ApiKey{Value: apiKey}
	}

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
	return o.db.ServiceFQDN()
}

func (o *KubeDBClientBuilder) getAPIKey() (string, error) {
	if o.db.Spec.DisableSecurity {
		return "", nil
	}
	if o.authConfig != nil {
		if cfg, ok := o.authConfig.(auth.ApiKey); ok {
			return cfg.Value, nil
		}
	}
	secret := &v1.Secret{}
	if err := o.kc.Get(o.ctx, client.ObjectKey{
		Namespace: o.db.Namespace,
		Name:      o.db.GetAuthSecretName(),
	}, secret); err != nil {
		return "", fmt.Errorf("failed to get weaviate auth secret: %w", err)
	}
	val, ok := secret.Data[kubedb.WeaviateAPIKey]
	if !ok || len(val) == 0 {
		return "", fmt.Errorf("weaviate auth secret key %s is missing or empty", kubedb.WeaviateAPIKey)
	}
	return string(val), nil
}

func (o *KubeDBClientBuilder) getHTTPClient(apiKey string) (*http.Client, error) {
	secret := &v1.Secret{}
	if err := o.kc.Get(o.ctx, client.ObjectKey{
		Namespace: o.db.Namespace,
		Name:      o.db.GetCertSecretName(api.WeaviateServerCert),
	}, secret); err != nil {
		return nil, fmt.Errorf("failed to get weaviate server certificate secret: %w", err)
	}
	ca, ok := secret.Data[kubedb.WeaviateTLSCACert]
	if !ok || len(ca) == 0 {
		return nil, fmt.Errorf("weaviate server certificate secret key %s is missing or empty", kubedb.WeaviateTLSCACert)
	}
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("failed to append weaviate CA certificate")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{
		RootCAs:    certPool,
		ServerName: o.db.ServiceFQDN(),
	}
	if o.db.TLSClientAuthEnabled() {
		clientSecret := &v1.Secret{}
		if err := o.kc.Get(o.ctx, client.ObjectKey{
			Namespace: o.db.Namespace,
			Name:      o.db.GetCertSecretName(api.WeaviateClientCert),
		}, clientSecret); err != nil {
			return nil, fmt.Errorf("failed to get weaviate client certificate secret: %w", err)
		}
		clientCert, ok := clientSecret.Data[kubedb.WeaviateTLSCert]
		if !ok || len(clientCert) == 0 {
			return nil, fmt.Errorf("weaviate client certificate secret key %s is missing or empty", kubedb.WeaviateTLSCert)
		}
		clientKey, ok := clientSecret.Data[kubedb.WeaviateTLSKey]
		if !ok || len(clientKey) == 0 {
			return nil, fmt.Errorf("weaviate client certificate secret key %s is missing or empty", kubedb.WeaviateTLSKey)
		}
		cert, err := tls.X509KeyPair(clientCert, clientKey)
		if err != nil {
			return nil, fmt.Errorf("failed to load weaviate client certificate: %w", err)
		}
		transport.TLSClientConfig.Certificates = []tls.Certificate{cert}
	}
	var rt http.RoundTripper = transport
	if apiKey != "" {
		rt = apiKeyRoundTripper{
			next:   rt,
			apiKey: apiKey,
		}
	}
	return &http.Client{Transport: rt}, nil
}

type apiKeyRoundTripper struct {
	next   http.RoundTripper
	apiKey string
}

func (r apiKeyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.Header.Set("Authorization", "Bearer "+r.apiKey)
	return r.next.RoundTrip(clone)
}
