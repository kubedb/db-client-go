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

type KubeDBClientBuilder struct {
	kc      client.Client
	db      *api.Neo4j
	log     logr.Logger
	url     string
	podName string
	ctx     context.Context
	auth    credential
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

func (o *KubeDBClientBuilder) WithAuth(username, password string) *KubeDBClientBuilder {
	o.auth = credential{
		username: username,
		password: password,
	}
	return o
}

func (o *KubeDBClientBuilder) GetNeo4jClient() (*Client, error) {
	// Construct URL - use default service if podName not provided
	o.url = o.buildConnectionURL()

	klog.V(3).Infof("Attempting to connect to Neo4j at: %s", o.url)

	authSecret := &core.Secret{}

	if !o.db.Spec.DisableSecurity && o.auth.username == "" && o.auth.password == "" {
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
		o.auth.username = string(authSecret.Data[core.BasicAuthUsernameKey])
		o.auth.password = string(authSecret.Data[core.BasicAuthPasswordKey])
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
	driver, err := neo4j.NewDriverWithContext(o.url, neo4j.BasicAuth(o.auth.username, o.auth.password, ""), func(c *config.Config) {
		c.SocketConnectTimeout = 60 * time.Second
		c.ConnectionAcquisitionTimeout = 60 * time.Second
		c.MaxTransactionRetryTime = 60 * time.Second
		c.MaxConnectionLifetime = 30 * time.Minute
		c.MaxConnectionPoolSize = 20
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

func (c *Client) ExecuteQuery(ctx context.Context, query string, params map[string]any, dbName string) (*neo4j.EagerResult, error) {
	return neo4j.ExecuteQuery(ctx, c, query, params, neo4j.EagerResultTransformer,
		neo4j.ExecuteQueryWithDatabase(dbName))
}

func (c *Client) ReloadTLS(ctx context.Context) error {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	_, err := session.Run(timeoutCtx, `
	CALL dbms.security.reloadTLS()
	`, nil)
	if err != nil {
		return fmt.Errorf("failed to execute TLS reload procedure: %w", err)
	}

	return nil
}

func (c *Client) CreateUser(ctx context.Context, username, password string) error {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	query := fmt.Sprintf("CREATE USER `%s` IF NOT EXISTS SET PASSWORD $password CHANGE NOT REQUIRED", username)

	params := map[string]any{
		"password": password,
	}

	_, err := session.Run(ctx, query, params)
	if err != nil {
		return fmt.Errorf("failed to create user %s: %w", username, err)
	}

	return nil
}

func (c *Client) DropUser(ctx context.Context, username string) error {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	query := fmt.Sprintf("DROP USER `%s` IF EXISTS", username)

	_, err := session.Run(ctx, query, nil)
	if err != nil {
		return fmt.Errorf("failed to drop user %s: %w", username, err)
	}

	return nil
}

func (c *Client) GrantRoleToUser(ctx context.Context, username, role string) error {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	query := fmt.Sprintf("GRANT ROLE `%s` TO `%s`", role, username)

	_, err := session.Run(ctx, query, nil)
	if err != nil {
		return fmt.Errorf("failed to grant role %s to user %s: %w", role, username, err)
	}
	return nil
}

func (c *Client) RenameUser(ctx context.Context, oldUsername, newUsername string) error {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	query := fmt.Sprintf("RENAME USER `%s` to `%s`", oldUsername, newUsername)

	_, err := session.Run(ctx, query, nil)
	if err != nil {
		return fmt.Errorf("failed to rename user from %s to %s: %w", oldUsername, newUsername, err)
	}
	return nil
}

func (c *Client) UpdateUserPassword(ctx context.Context, username, newPassword string) error {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	query := fmt.Sprintf("ALTER USER `%s` SET PASSWORD $password", username)

	params := map[string]any{
		"password": newPassword,
	}

	_, err := session.Run(ctx, query, params)
	if err != nil {
		return fmt.Errorf("failed to update password for user %s, pass %s: %w", username, newPassword, err)
	}
	return nil
}

func (c *Client) SetConfigValue(ctx context.Context, key, value string) error {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	query := fmt.Sprintf("CALL dbms.setConfigValue('%s', '%s')", key, value)

	_, err := session.Run(ctx, query, nil)
	if err != nil {
		return fmt.Errorf("failed to set config value for key %s to %s: %w", key, value, err)
	}
	return nil
}

func (o *KubeDBClientBuilder) buildConnectionURL() string {
	scheme := "neo4j"

	if o.db.Spec.TLS != nil && o.db.Spec.TLS.Bolt.Mode != api.TLSModeDisabled {
		scheme = "neo4j+s"
	}

	if o.podName != "" {
		return fmt.Sprintf("%s://%s.%s.svc.%s:%d", scheme, o.podName, o.db.Namespace, apiutils.FindDomain(), kubedb.Neo4jBoltPort)
	}

	return fmt.Sprintf("%s://%s.%s.svc.%s:%d", scheme, o.db.ServiceName(), o.db.Namespace, apiutils.FindDomain(), kubedb.Neo4jBoltPort)
}
