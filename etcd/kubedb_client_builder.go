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

package etcd

import (
	"context"
	"crypto/tls"
	"crypto/x509"

	"kubedb.dev/apimachinery/apis/kubedb"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	secret_lib "kubedb.dev/apimachinery/pkg/secret"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc      client.Client
	db      *api.Etcd
	url     string
	podName string
	ctx     context.Context
}

func NewKubeDBClientBuilder(kc client.Client, db *api.Etcd) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc: kc,
		db: db,
	}
}

// WithURL pins the client to an explicit endpoint (i.e. a port-forwarded
// address) instead of the in-cluster member URLs.
func (o *KubeDBClientBuilder) WithURL(url string) *KubeDBClientBuilder {
	o.url = url
	return o
}

// WithPod restricts the client to a single member, which is what the health
// checker and the member-scoped ops (defrag, member status) need.
func (o *KubeDBClientBuilder) WithPod(podName string) *KubeDBClientBuilder {
	o.podName = podName
	return o
}

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}

// GetEtcdClient dials the etcd cluster and returns the wrapped clientv3 client.
// The caller owns the returned client and must Close() it.
func (o *KubeDBClientBuilder) GetEtcdClient() (*Client, error) {
	if o.ctx == nil {
		o.ctx = context.Background()
	}

	// Config.Context is deliberately left unset: it scopes the lifetime of the
	// whole client, and the builder context is only meant to scope the secret
	// lookups done here.
	cfg := clientv3.Config{
		Endpoints:   o.getEndpoints(),
		DialTimeout: DefaultDialTimeout,
	}

	tlsConfig, err := o.getTLSConfig()
	if err != nil {
		return nil, err
	}
	cfg.TLS = tlsConfig

	username, password, err := o.getAuthCredentials()
	if err != nil {
		return nil, err
	}
	// An empty username makes clientv3 skip the Authenticate RPC entirely. That
	// is the anonymous path used before `auth enable` has run.
	cfg.Username = username
	cfg.Password = password

	cl, err := clientv3.New(cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create etcd client for %s/%s", o.db.Namespace, o.db.Name)
	}

	return &Client{
		Client: cl,
		cfg:    cfg,
	}, nil
}

// getEndpoints resolves the client URLs this builder should dial. An explicit
// url wins, then a single pod, then every member of the cluster.
func (o *KubeDBClientBuilder) getEndpoints() []string {
	if o.url != "" {
		return []string{o.url}
	}
	if o.podName != "" {
		return []string{o.db.ClientURL(o.podName)}
	}

	replicas := int32(1)
	if o.db.Spec.Replicas != nil && *o.db.Spec.Replicas > 0 {
		replicas = *o.db.Spec.Replicas
	}
	endpoints := make([]string, 0, replicas)
	for i := 0; i < int(replicas); i++ {
		endpoints = append(endpoints, o.db.ClientURL(o.db.PodName(i)))
	}
	return endpoints
}

// getTLSConfig builds the client side *tls.Config from the client certificate
// secret. It returns nil when the database does not have TLS enabled.
func (o *KubeDBClientBuilder) getTLSConfig() (*tls.Config, error) {
	if o.db.Spec.TLS == nil {
		return nil, nil
	}

	secretName := o.db.GetCertSecretName(api.EtcdClientCert)
	if secretName == "" {
		return nil, errors.Errorf("etcd %s/%s has TLS enabled but the %q certificate alias is not configured", o.db.Namespace, o.db.Name, api.EtcdClientCert)
	}

	var sec core.Secret
	if err := o.kc.Get(o.ctx, client.ObjectKey{Namespace: o.db.Namespace, Name: secretName}, &sec); err != nil {
		return nil, errors.Wrapf(err, "failed to read etcd client cert secret %s/%s", o.db.Namespace, secretName)
	}

	caPEM := sec.Data[kubedb.CACert]
	if len(caPEM) == 0 {
		return nil, errors.Errorf("etcd client cert secret %s/%s must contain %q", o.db.Namespace, secretName, kubedb.CACert)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.Errorf("failed to parse %q of etcd client cert secret %s/%s", kubedb.CACert, o.db.Namespace, secretName)
	}

	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    pool,
	}

	certPEM, certOK := sec.Data[core.TLSCertKey]
	keyPEM, keyOK := sec.Data[core.TLSPrivateKeyKey]
	switch {
	case certOK && keyOK:
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to load client certificate from etcd client cert secret %s/%s", o.db.Namespace, secretName)
		}
		cfg.Certificates = []tls.Certificate{cert}
	case certOK != keyOK:
		return nil, errors.Errorf("etcd client cert secret %s/%s must contain both %q and %q", o.db.Namespace, secretName, core.TLSCertKey, core.TLSPrivateKeyKey)
	}

	return cfg, nil
}

// getAuthCredentials reads the root credentials from the auth secret. A missing
// secret (or a secret without a password) means etcd RBAC has not been enabled
// yet, so the client dials anonymously instead of failing.
func (o *KubeDBClientBuilder) getAuthCredentials() (string, string, error) {
	if o.db.Spec.AuthSecret == nil {
		return "", "", nil
	}

	isVirtual := api.IsVirtualAuthSecretReferred(o.db.Spec.AuthSecret)
	data, err := secret_lib.GetData(o.ctx, o.kc, o.db.Namespace, o.db.GetAuthSecretName(), isVirtual)
	if err != nil {
		if kerr.IsNotFound(err) {
			return "", "", nil
		}
		return "", "", errors.Wrapf(err, "failed to read etcd auth secret %s/%s", o.db.Namespace, o.db.GetAuthSecretName())
	}

	password := string(data[core.BasicAuthPasswordKey])
	if password == "" {
		return "", "", nil
	}
	// etcd requires a user named root before auth can be enabled at all, so
	// that is the fallback when the secret carries no username.
	username := string(data[core.BasicAuthUsernameKey])
	if username == "" {
		username = kubedb.EtcdRootUser
	}
	return username, password, nil
}
