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

package memcached

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"strings"
	"time"

	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"kubedb.dev/apimachinery/apis/kubedb"
	dbapi "kubedb.dev/apimachinery/apis/kubedb/v1"

	"github.com/kubedb/gomemcache/memcache"
	"k8s.io/klog/v2"
	api "kubedb.dev/apimachinery/apis/kubedb/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc       client.Client
	db       *dbapi.Memcached
	podName  string
	url      string
	database int
}

func NewKubeDBClientBuilder(kc client.Client, db *dbapi.Memcached) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc: kc,
		db: db,
	}
}

func (o *KubeDBClientBuilder) WithPod(podName string) *KubeDBClientBuilder {
	o.podName = podName
	return o
}

func (o *KubeDBClientBuilder) WithURL(url string) *KubeDBClientBuilder {
	o.url = url
	return o
}

func (o *KubeDBClientBuilder) WithDatabase(database int) *KubeDBClientBuilder {
	o.database = database
	return o
}

func (o *KubeDBClientBuilder) GetMemcachedClient() (*Client, error) {
	mcClient := memcache.New(o.db.Address())
	if o.db.Spec.TLS != nil {
		// Secret for Memcached Client Certs
		secret, err := o.GetTLSSecret()
		if err != nil {
			klog.Error(err, "Failed to get TLS-secret")
			return nil, err
		}

		if secret.Data["ca.crt"] == nil || secret.Data["tls.crt"] == nil || secret.Data["tls.key"] == nil {
			return nil, errors.New("invalid tls-secret.")
		}

		caCert := secret.Data["ca.crt"]
		clientCert := secret.Data["tls.crt"]
		clientKey := secret.Data["tls.key"]

		caCertPool := x509.NewCertPool()
		if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
			klog.Errorf("Failed to append CA certificate to the pool")
		}

		// Load client certificate
		clientCertificate, err := tls.X509KeyPair(clientCert, clientKey)
		if err != nil {
			klog.Errorf("Failed to load client certificate: %v", err)
		}
		// Create TLS configuration
		tlsConfig := &tls.Config{
			Certificates:       []tls.Certificate{clientCertificate},
			RootCAs:            caCertPool,
			InsecureSkipVerify: false, // Ensure server's cert is verified
		}
		// Override the dialer to use TLS by setting the DialContext function
		mcClient.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return tls.DialWithDialer(&net.Dialer{
				Timeout: 10 * time.Second,
			}, "tcp", o.db.Address(), tlsConfig)
		}
	}

	return &Client{
		mcClient,
	}, nil
}

func (o *KubeDBClientBuilder) SetAuth(mcClient *Client) error {
	secret, err := o.GetSecret()
	if err != nil {
		klog.Error(err, "Failed to get auth-secret")
		return errors.New("secret is not found")
	}

	authData := string(secret.Data[kubedb.AuthDataKey])
	separatePairs := strings.Split(authData, "\n")
	firstUsernamePassPair := separatePairs[0]

	splitUsernamePassword := strings.Split(firstUsernamePassPair, ":")
	memcachedUserName, memcachedPassword := strings.TrimSpace(splitUsernamePassword[0]), strings.TrimSpace(splitUsernamePassword[1])

	err = mcClient.SetAuth(&memcache.Item{
		Key: kubedb.MemcachedHealthKey, Flags: 0, Expiration: 0, User: memcachedUserName, Pass: memcachedPassword,
	})
	if err != nil {
		klog.Errorf("Authentication Error: %v", err.Error())
	} else {
		klog.V(5).Infof("Authentication Done Successfully !!...")
	}
	return nil
}

func (o *KubeDBClientBuilder) GetSecret() (*core.Secret, error) {
	var authSecret core.Secret
	err := o.kc.Get(context.TODO(), types.NamespacedName{
		Name:      o.db.GetMemcachedAuthSecretName(),
		Namespace: o.db.Namespace,
	}, &authSecret)
	if err != nil {
		return nil, err
	}
	return &authSecret, nil
}

func (o *KubeDBClientBuilder) GetTLSSecret() (*core.Secret, error) {
	var tlsSecret core.Secret
	err := o.kc.Get(context.TODO(), types.NamespacedName{
		Name:      o.db.GetCertSecretName(api.MemcachedClientCert),
		Namespace: o.db.Namespace,
	}, &tlsSecret)
	if err != nil {
		return nil, err
	}
	return &tlsSecret, nil
}
