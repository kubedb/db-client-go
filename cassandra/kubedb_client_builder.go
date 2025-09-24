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

package cassandra

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"

	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"kubedb.dev/apimachinery/apis/kubedb"

	"github.com/gocql/gocql"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc   client.Client
	db   *api.Cassandra
	url  string
	port *int
	ctx  context.Context
}

func NewKubeDBClientBuilder(kc client.Client, db *api.Cassandra) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc: kc,
		db: db,
	}
}

func (o *KubeDBClientBuilder) WithURL(url string) *KubeDBClientBuilder {
	o.url = url
	return o
}

func (o *KubeDBClientBuilder) WithPort(port *int) *KubeDBClientBuilder {
	o.port = port
	return o
}

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}
func (o *KubeDBClientBuilder) GetCassandraClient() (*Client, error) {
	host := o.url
	cluster := gocql.NewCluster(host)
	cluster.Port = kubedb.CassandraNativeTcpPort
	cluster.Keyspace = "system"
	if o.db.Spec.Topology == nil {
		cluster.Consistency = gocql.One
	} else {
		cluster.Consistency = gocql.Quorum
	}
	if !o.db.Spec.DisableSecurity {

		authSecret := &core.Secret{}
		err := o.kc.Get(o.ctx, types.NamespacedName{
			Namespace: o.db.Namespace,
			Name:      o.db.GetAuthSecretName(),
		}, authSecret)
		if err != nil {
			if kerr.IsNotFound(err) {
				klog.Error(err, "AuthSecret not found")
				return nil, errors.New("auth-secret not found")
			}
			return nil, err
		}
		userName := string(authSecret.Data[core.BasicAuthUsernameKey])
		password := string(authSecret.Data[core.BasicAuthPasswordKey])
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: userName,
			Password: password,
		}
	}
	if o.db.Spec.TLS != nil {
		tlsConfig, err := o.GetTLSConfig()
		if err != nil {
			return nil, err
		}
		cluster.SslOpts = &gocql.SslOptions{
			Config: tlsConfig,
		}
	}
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("unable to connect to Cassandra cluster: %v", err)
	}

	return &Client{session}, nil
}

func (o *KubeDBClientBuilder) GetTLSConfig() (*tls.Config, error) {
	var certSecret core.Secret
	err := o.kc.Get(o.ctx, types.NamespacedName{
		Namespace: o.db.Namespace,
		Name:      o.db.GetCertSecretName(api.CassandraClientCert),
	}, &certSecret)
	if err != nil {
		klog.Error(err, "failed to get clientCert secret")
		return nil, err
	}

	// get tls cert, clientCA and rootCA for tls config
	// use server cert ca for rootca as issuer ref is not taken into account
	clientCA := x509.NewCertPool()
	rootCA := x509.NewCertPool()

	crt, err := tls.X509KeyPair(certSecret.Data[core.TLSCertKey], certSecret.Data[core.TLSPrivateKeyKey])
	if err != nil {
		klog.Error(err, "failed to create certificate for TLS config")
		return nil, err
	}
	clientCA.AppendCertsFromPEM(certSecret.Data[kubedb.CACert])
	rootCA.AppendCertsFromPEM(certSecret.Data[kubedb.CACert])

	tlsConfig := &tls.Config{
		ServerName:   o.db.ServiceName(),
		Certificates: []tls.Certificate{crt},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCA,
		RootCAs:      rootCA,
		MaxVersion:   tls.VersionTLS13,
	}
	return tlsConfig, nil
}
