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

package neo4j

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"time"

	"kubedb.dev/apimachinery/apis/kubedb"
	v1api "kubedb.dev/apimachinery/apis/kubedb/v1"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	apiutils "kubedb.dev/apimachinery/pkg/utils"

	"github.com/go-logr/logr"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type credential struct {
	username string
	password string
}

type Neo4jClientBuilder struct {
	kc      client.Client
	db      *api.Neo4j
	log     logr.Logger
	url     string
	podName string
	ctx     context.Context
}

func NewNeo4jClientBuilder(kc client.Client, db *api.Neo4j) *Neo4jClientBuilder {
	return &Neo4jClientBuilder{
		kc: kc,
		db: db,
	}
}

func (o *Neo4jClientBuilder) WithPodName(podName string) *Neo4jClientBuilder {
	o.podName = podName
	return o
}

func (o *Neo4jClientBuilder) WithContext(ctx context.Context) *Neo4jClientBuilder {
	o.ctx = ctx
	return o
}

func (o *Neo4jClientBuilder) WithLog(log logr.Logger) *Neo4jClientBuilder {
	o.log = log
	return o
}

func (o *Neo4jClientBuilder) GetNeo4jClient() (*Client, error) {
	// Construct URL - use default service if podName not provided
	o.url = o.buildConnectionURL()

	klog.V(3).Infof("Attempting to connect to Neo4j at: %s", o.url)

	authSecret := &core.Secret{}
	var cred credential

	if !o.db.Spec.DisableSecurity {
		err := o.kc.Get(o.ctx, types.NamespacedName{
			Namespace: o.db.Namespace,
			Name:      o.db.GetAuthSecretName(),
		}, authSecret)
		if err != nil {
			if kerr.IsNotFound(err) {
				klog.Error(err, "Auth-secret not found")
				return nil, errors.New("auth-secret is not found")
			}
			klog.Error(err, "Failed to get auth-secret")
			return nil, err
		}
		cred.username = string(authSecret.Data[core.BasicAuthUsernameKey])
		cred.password = string(authSecret.Data[core.BasicAuthPasswordKey])
	} else {
		klog.Info("Security is disabled for Neo4j, no credentials will be used.")
	}

	tlsConfig := &tls.Config{}

	if o.db.Spec.TLS != nil && o.db.Spec.TLS.Bolt.Mode != api.TLSModeDisabled {
		certSecret := &core.Secret{}
		err := o.kc.Get(o.ctx, types.NamespacedName{
			Namespace: o.db.Namespace,
			Name:      o.db.GetCertSecretName(api.Neo4jCertificateTypeClient),
		}, certSecret)
		if err != nil {
			if kerr.IsNotFound(err) {
				klog.Error(err, "Client certificate secret not found")
				return nil, errors.New("client certificate secret is not found")
			}
			klog.Error(err, "Failed to get client certificate Secret")
			return nil, err
		}

		// get tls cert, clientCA and rootCA for tls config
		clientCA := x509.NewCertPool()
		rootCA := x509.NewCertPool()

		crt, err := tls.X509KeyPair(certSecret.Data[core.TLSCertKey], certSecret.Data[core.TLSPrivateKeyKey])
		if err != nil {
			klog.Error(err, "Failed to parse private key pair")
			return nil, err
		}
		clientCA.AppendCertsFromPEM(certSecret.Data[v1api.TLSCACertFileName])
		rootCA.AppendCertsFromPEM(certSecret.Data[v1api.TLSCACertFileName])
		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{crt},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    clientCA,
			RootCAs:      rootCA,
			MaxVersion:   tls.VersionTLS13,
		}
	}

	// Create driver and check for errors immediately
	driver, err := neo4j.NewDriverWithContext(o.url, neo4j.BasicAuth(cred.username, cred.password, ""), func(c *config.Config) {
		c.SocketConnectTimeout = 15 * time.Second
		c.ConnectionAcquisitionTimeout = 30 * time.Second
		c.MaxTransactionRetryTime = 30 * time.Second
		c.MaxConnectionLifetime = 30 * time.Minute
		c.MaxConnectionPoolSize = 20
		c.SocketKeepalive = true
		if o.db.Spec.TLS != nil {
			c.TlsConfig = tlsConfig
		}
	})
	if o.db.Spec.DisableSecurity {
		driver, err = neo4j.NewDriverWithContext(o.url, neo4j.NoAuth())
	}
	if err != nil {
		klog.Error(err, "Failed to create Neo4j driver")
		return nil, err
	}

	// Now verify connectivity on the successfully created driver
	if err = driver.VerifyConnectivity(o.ctx); err != nil {
		klog.Error(err, "Failed to connect to Neo4j")
		// Close driver on verification failure
		_ = driver.Close(o.ctx)
		return nil, err
	}

	log := ctrl.Log.WithValues(api.ResourceSingularNeo4j, types.NamespacedName{
		Namespace: o.db.Namespace,
		Name:      o.db.OffshootName(),
	})

	log.V(2).Info("Connection established successfully to Neo4j at: %s", o.url)

	return &Client{
		DriverWithContext: driver,
	}, nil
}

func (o *Neo4jClientBuilder) buildConnectionURL() string {
	scheme := "neo4j"

	if o.db.Spec.TLS != nil && o.db.Spec.TLS.Bolt.Mode != api.TLSModeDisabled {
		scheme = "neo4j+s"
	}

	if o.podName != "" {
		return fmt.Sprintf("%s://%s.%s.svc.%s:%d", scheme, o.podName, o.db.Namespace, apiutils.FindDomain(), kubedb.Neo4jBoltPort)
	}

	return fmt.Sprintf("%s://%s.%s.svc.%s:%d", scheme, o.db.ServiceName(), o.db.Namespace, apiutils.FindDomain(), kubedb.Neo4jBoltPort)
}
