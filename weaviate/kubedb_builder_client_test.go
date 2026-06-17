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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"kubedb.dev/apimachinery/apis/kubedb"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestGetHTTPClientLoadsServerCAAndClientCertificate(t *testing.T) {
	certPEM, keyPEM := newTestCertificate(t)
	db := newTLSWeaviate()
	kc := newSecretClient(t,
		newSecret(db.Namespace, db.GetCertSecretName(api.WeaviateServerCert), map[string][]byte{
			kubedb.WeaviateTLSCACert: certPEM,
		}),
		newSecret(db.Namespace, db.GetCertSecretName(api.WeaviateClientCert), map[string][]byte{
			kubedb.WeaviateTLSCACert: certPEM,
			kubedb.WeaviateTLSCert:   certPEM,
			kubedb.WeaviateTLSKey:    keyPEM,
		}),
	)

	httpClient, err := NewKubeDBClientBuilder(kc, db).WithContext(context.Background()).getHTTPClient("")
	if err != nil {
		t.Fatalf("getHTTPClient() returned error: %v", err)
	}

	transport, ok := httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport, got %T", httpClient.Transport)
	}
	tlsConfig := transport.TLSClientConfig
	if tlsConfig == nil {
		t.Fatal("expected TLSClientConfig")
	}
	if tlsConfig.RootCAs == nil {
		t.Fatal("expected RootCAs to trust the Weaviate server certificate")
	}
	if got := len(tlsConfig.Certificates); got != 1 {
		t.Fatalf("expected one client certificate, got %d", got)
	}
	if tlsConfig.ServerName != db.ServiceFQDN() {
		t.Fatalf("expected ServerName %q, got %q", db.ServiceFQDN(), tlsConfig.ServerName)
	}
}

func TestGetHTTPClientSupportsServerTLSWithoutClientCertificate(t *testing.T) {
	certPEM, _ := newTestCertificate(t)
	db := newTLSWeaviate()
	clientAuth := false
	db.Spec.TLS.ClientAuth = &clientAuth
	kc := newSecretClient(t,
		newSecret(db.Namespace, db.GetCertSecretName(api.WeaviateServerCert), map[string][]byte{
			kubedb.WeaviateTLSCACert: certPEM,
		}),
	)

	httpClient, err := NewKubeDBClientBuilder(kc, db).WithContext(context.Background()).getHTTPClient("")
	if err != nil {
		t.Fatalf("getHTTPClient() returned error: %v", err)
	}

	transport, ok := httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport, got %T", httpClient.Transport)
	}
	tlsConfig := transport.TLSClientConfig
	if tlsConfig == nil {
		t.Fatal("expected TLSClientConfig")
	}
	if tlsConfig.RootCAs == nil {
		t.Fatal("expected RootCAs to trust the Weaviate server certificate")
	}
	if got := len(tlsConfig.Certificates); got != 0 {
		t.Fatalf("expected no client certificates, got %d", got)
	}
}

func TestGetHTTPClientRequiresClientCertificateSecret(t *testing.T) {
	certPEM, _ := newTestCertificate(t)
	db := newTLSWeaviate()
	kc := newSecretClient(t,
		newSecret(db.Namespace, db.GetCertSecretName(api.WeaviateServerCert), map[string][]byte{
			kubedb.WeaviateTLSCACert: certPEM,
		}),
	)

	_, err := NewKubeDBClientBuilder(kc, db).WithContext(context.Background()).getHTTPClient("")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "weaviate client certificate secret") {
		t.Fatalf("expected client certificate secret error, got %v", err)
	}
}

func newTLSWeaviate() *api.Weaviate {
	return &api.Weaviate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sample",
			Namespace: "demo",
		},
		Spec: api.WeaviateSpec{
			TLS: &api.WeaviateTLSConfig{},
		},
	}
}

type secretClient struct {
	scheme  *runtime.Scheme
	secrets map[client.ObjectKey]*core.Secret
}

func newSecretClient(t *testing.T, secrets ...*core.Secret) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := core.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add core scheme: %v", err)
	}
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add weaviate scheme: %v", err)
	}

	c := &secretClient{
		scheme:  scheme,
		secrets: map[client.ObjectKey]*core.Secret{},
	}
	for _, secret := range secrets {
		c.secrets[client.ObjectKeyFromObject(secret)] = secret.DeepCopy()
	}
	return c
}

func (c *secretClient) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	secret, ok := c.secrets[key]
	if !ok {
		return apierrors.NewNotFound(core.Resource("secrets"), key.Name)
	}
	target, ok := obj.(*core.Secret)
	if !ok {
		return fmt.Errorf("unsupported object %T", obj)
	}
	secret.DeepCopyInto(target)
	return nil
}

func (c *secretClient) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errUnsupportedClientOperation
}

func (c *secretClient) Apply(context.Context, runtime.ApplyConfiguration, ...client.ApplyOption) error {
	return errUnsupportedClientOperation
}

func (c *secretClient) Create(context.Context, client.Object, ...client.CreateOption) error {
	return errUnsupportedClientOperation
}

func (c *secretClient) Delete(context.Context, client.Object, ...client.DeleteOption) error {
	return errUnsupportedClientOperation
}

func (c *secretClient) Update(context.Context, client.Object, ...client.UpdateOption) error {
	return errUnsupportedClientOperation
}

func (c *secretClient) Patch(context.Context, client.Object, client.Patch, ...client.PatchOption) error {
	return errUnsupportedClientOperation
}

func (c *secretClient) DeleteAllOf(context.Context, client.Object, ...client.DeleteAllOfOption) error {
	return errUnsupportedClientOperation
}

func (c *secretClient) Status() client.SubResourceWriter {
	return unsupportedSubResourceClient{}
}

func (c *secretClient) SubResource(string) client.SubResourceClient {
	return unsupportedSubResourceClient{}
}

func (c *secretClient) Scheme() *runtime.Scheme {
	return c.scheme
}

func (c *secretClient) RESTMapper() meta.RESTMapper {
	return nil
}

func (c *secretClient) GroupVersionKindFor(runtime.Object) (schema.GroupVersionKind, error) {
	return schema.GroupVersionKind{}, nil
}

func (c *secretClient) IsObjectNamespaced(runtime.Object) (bool, error) {
	return true, nil
}

type unsupportedSubResourceClient struct{}

func (unsupportedSubResourceClient) Get(context.Context, client.Object, client.Object, ...client.SubResourceGetOption) error {
	return errUnsupportedClientOperation
}

func (unsupportedSubResourceClient) Create(context.Context, client.Object, client.Object, ...client.SubResourceCreateOption) error {
	return errUnsupportedClientOperation
}

func (unsupportedSubResourceClient) Update(context.Context, client.Object, ...client.SubResourceUpdateOption) error {
	return errUnsupportedClientOperation
}

func (unsupportedSubResourceClient) Patch(context.Context, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
	return errUnsupportedClientOperation
}

var errUnsupportedClientOperation = errors.New("unsupported client operation")

func newSecret(namespace, name string, data map[string][]byte) *core.Secret {
	return &core.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: data,
	}
}

func newTestCertificate(t *testing.T) ([]byte, []byte) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "weaviate-test",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return certPEM, keyPEM
}
