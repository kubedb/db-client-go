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
	"strings"
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

type KubeDBClientBuilder struct {
	kc      client.Client
	db      *api.Neo4j
	log     logr.Logger
	url     string
	podName string
	ctx     context.Context
}

func NewKubeDBClientBuilder(kc client.Client, db *api.Neo4j) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc: kc,
		db: db,
	}
}

func (o *KubeDBClientBuilder) WithPodName(podName string) *KubeDBClientBuilder {
	o.podName = podName
	return o
}

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}

func (o *KubeDBClientBuilder) WithLog(log logr.Logger) *KubeDBClientBuilder {
	o.log = log
	return o
}

func (o *KubeDBClientBuilder) GetNeo4jClient() (*Client, error) {
	// Get domain and fallback to cluster.local if not found
	domain := apiutils.FindDomain()
	if domain == "" {
		domain = "cluster.local"
	}

	// Construct URL - use default service if podName not provided
	o.url = o.buildConnectionURL()

	klog.V(3).Infof("Attempting to connect to Neo4j at: %s", o.url)

	var dbUser, dbPassword string
	authSecret := &core.Secret{}

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

		// Extract NEO4J_AUTH from Secret.Data
		auth, ok := authSecret.Data["NEO4J_AUTH"]
		if !ok {
			return nil, fmt.Errorf("secret %s/%s does not contain key NEO4J_AUTH", o.db.Namespace, o.db.GetAuthSecretName())
		}
		authStr := strings.TrimSpace(string(auth))

		// Expect "username/password", split safely
		parts := strings.SplitN(authStr, "/", 2)
		if len(parts) != 2 || parts[0] == "" {
			return nil, fmt.Errorf("invalid NEO4J_AUTH format in secret %s/%s. Expected \"username/password\"", o.db.Namespace, o.db.GetAuthSecretName())
		}
		dbUser = parts[0]
		dbPassword = parts[1]
	} else {
		klog.Info("Security is disabled for Neo4j, no credentials will be used.")
	}

	tlsConfig := &tls.Config{}

	if o.db.Spec.TLS != nil && o.db.Spec.TLS.IssuerRef != nil {
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
	driver, err := neo4j.NewDriverWithContext(o.url, neo4j.BasicAuth(dbUser, dbPassword, ""), func(c *config.Config) {
		c.SocketConnectTimeout = 60 * time.Second
		c.ConnectionAcquisitionTimeout = 60 * time.Second
		c.MaxTransactionRetryTime = 60 * time.Second
		c.MaxConnectionLifetime = 30 * time.Minute
		c.MaxConnectionPoolSize = 20
		if o.db.Spec.TLS != nil && o.db.Spec.TLS.IssuerRef != nil {
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

func (c *Client) ExecuteQuery(ctx context.Context, query string, params map[string]any, dbName string) (*neo4j.EagerResult, error) {
	return neo4j.ExecuteQuery(ctx, c, query, params, neo4j.EagerResultTransformer,
		neo4j.ExecuteQueryWithDatabase(dbName))
}

func (o *KubeDBClientBuilder) buildConnectionURL() string {
	scheme := "neo4j"

	if o.db.Spec.TLS != nil && o.db.Spec.TLS.IssuerRef != nil {
		scheme = "neo4j+s"
	}

	if o.podName != "" {
		return fmt.Sprintf("%s://%s.%s.%s.svc.%s:%d", scheme, o.podName, o.db.OffshootName(), o.db.Namespace, apiutils.FindDomain(), kubedb.Neo4jBoltPort)
	}

	return fmt.Sprintf("%s://%s.%s.svc.%s:%d", scheme, o.db.ServiceName(), o.db.Namespace, apiutils.FindDomain(), kubedb.Neo4jBoltPort)
}
