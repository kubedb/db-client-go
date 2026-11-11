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

package yugabytedb

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"

	"kubedb.dev/apimachinery/apis/kubedb"
	dbapi "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	dbsecret "kubedb.dev/apimachinery/pkg/secret"

	"github.com/gocql/gocql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	core "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	DefaultTimeout = 10 * time.Second

	// Keys of the client certificate Secret that cert-manager writes.
	CACertKey = "ca.crt"
)

// KubeDBClientBuilder builds YSQL and YCQL clients for a YugabyteDB.
//
// By default a client connects to the client Service of the database with the credentials of the
// auth Secret, and, when client-to-node TLS is on, with the client certificate Secret. Every
// default can be overridden, so the operator can also reach one pod before it is admitted to the
// client Service, log in with the default or the previous password, or trust a CA bundle.
type KubeDBClientBuilder struct {
	kc       client.Client
	db       *dbapi.YugabyteDB
	url      string
	podName  string
	port     *int
	database string
	ctx      context.Context

	user, password string
	credentialsSet bool

	tlsConfig *tls.Config
	tlsSet    bool

	timeout time.Duration
}

func NewKubeDBClientBuilder(kc client.Client, db *dbapi.YugabyteDB) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc: kc,
		db: db,
	}
}

// WithPod connects to one pod through the governing Service.
func (o *KubeDBClientBuilder) WithPod(podName string) *KubeDBClientBuilder {
	o.podName = podName
	return o
}

// WithURL connects to the given host name.
func (o *KubeDBClientBuilder) WithURL(url string) *KubeDBClientBuilder {
	o.url = url
	return o
}

func (o *KubeDBClientBuilder) WithPort(port int) *KubeDBClientBuilder {
	o.port = &port
	return o
}

// WithDatabase selects the YSQL database. The default is yugabyte.
func (o *KubeDBClientBuilder) WithDatabase(database string) *KubeDBClientBuilder {
	o.database = database
	return o
}

// WithCredentials logs in with user and password instead of the credentials of the auth Secret.
func (o *KubeDBClientBuilder) WithCredentials(user, password string) *KubeDBClientBuilder {
	o.user, o.password = user, password
	o.credentialsSet = true
	return o
}

// WithTLSConfig uses cfg instead of the client certificate Secret. A nil cfg connects in plain text.
func (o *KubeDBClientBuilder) WithTLSConfig(cfg *tls.Config) *KubeDBClientBuilder {
	o.tlsConfig = cfg
	o.tlsSet = true
	return o
}

func (o *KubeDBClientBuilder) WithTimeout(timeout time.Duration) *KubeDBClientBuilder {
	o.timeout = timeout
	return o
}

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}

// GetYSQLClient opens a YSQL connection pool and pings it.
func (o *KubeDBClientBuilder) GetYSQLClient() (*YSQLClient, error) {
	cfg, err := o.ysqlConfig()
	if err != nil {
		return nil, err
	}
	db := stdlib.OpenDB(*cfg)
	if err := db.PingContext(o.ctx); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			klog.Errorf("Failed to close client. error: %v", closeErr)
		}
		return nil, err
	}
	return &YSQLClient{db}, nil
}

// GetYCQLClient opens a YCQL session to the one host. The session never discovers the other
// nodes: system.peers lists pod IPs that a client outside the pod network cannot reach.
func (o *KubeDBClientBuilder) GetYCQLClient() (*YCQLClient, error) {
	o.setDefaults()
	host, err := o.host()
	if err != nil {
		return nil, err
	}
	user, password, err := o.credentials()
	if err != nil {
		return nil, err
	}
	tlsCfg, err := o.GetTLSConfig()
	if err != nil {
		return nil, err
	}

	cluster := gocql.NewCluster(host)
	cluster.Port = o.portOr(kubedb.YugabyteDBYCQLPort)
	cluster.ProtoVersion = 4
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: user, Password: password}
	cluster.Consistency = gocql.One
	cluster.ConnectTimeout = o.timeout
	cluster.Timeout = o.timeout
	cluster.NumConns = 1
	cluster.DisableInitialHostLookup = true
	cluster.HostFilter = gocql.WhiteListHostFilter(host)
	cluster.Events.DisableTopologyEvents = true
	cluster.Events.DisableNodeStatusEvents = true
	cluster.Events.DisableSchemaEvents = true
	cluster.DisableSkipMetadata = true
	if tlsCfg != nil {
		cluster.SslOpts = &gocql.SslOptions{Config: tlsCfg, EnableHostVerification: true}
	}
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &YCQLClient{session}, nil
}

// GetTLSConfig returns the TLS configuration of the connection, nil for plain text. By default it
// is built from the client certificate Secret when client-to-node TLS is on, and verifies the server
// certificate against the dialled host name.
func (o *KubeDBClientBuilder) GetTLSConfig() (*tls.Config, error) {
	o.setDefaults()
	if o.tlsSet {
		return o.tlsConfig, nil
	}
	if o.db == nil {
		return nil, errors.New("no YugabyteDB is given to read the client certificate from")
	}
	if !o.db.IsClientToNodeTLS() {
		return nil, nil
	}
	host, err := o.host()
	if err != nil {
		return nil, err
	}
	name := o.db.GetCertSecretName(dbapi.YugabyteDBCertificateTypeClient)
	var secret core.Secret
	if err := o.kc.Get(o.ctx, client.ObjectKey{Namespace: o.db.Namespace, Name: name}, &secret); err != nil {
		return nil, fmt.Errorf("failed to read the client certificate Secret %s/%s: %w", o.db.Namespace, name, err)
	}
	return TLSConfigFromPEM(host, secret.Data[CACertKey], secret.Data[core.TLSCertKey], secret.Data[core.TLSPrivateKeyKey])
}

// TLSConfigFromPEM builds a TLS configuration that trusts ca and verifies the server certificate
// against serverName. A client certificate is presented when cert and key are both given.
func TLSConfigFromPEM(serverName string, ca, cert, key []byte) (*tls.Config, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("the CA bundle holds no certificate")
	}
	cfg := &tls.Config{RootCAs: pool, ServerName: serverName, MinVersion: tls.VersionTLS12}
	if len(cert) > 0 && len(key) > 0 {
		pair, err := tls.X509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("invalid client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg, nil
}

func (o *KubeDBClientBuilder) setDefaults() {
	if o.ctx == nil {
		o.ctx = context.Background()
	}
	if o.timeout == 0 {
		o.timeout = DefaultTimeout
	}
}

func (o *KubeDBClientBuilder) ysqlConfig() (*pgx.ConnConfig, error) {
	o.setDefaults()
	host, err := o.host()
	if err != nil {
		return nil, err
	}
	user, password, err := o.credentials()
	if err != nil {
		return nil, err
	}
	tlsCfg, err := o.GetTLSConfig()
	if err != nil {
		return nil, err
	}
	database := o.database
	if database == "" {
		database = kubedb.YugabyteDBDefaultDatabase
	}

	// The credentials are set on the parsed config, so that no special character in them has to be
	// escaped in a connection string.
	u := url.URL{
		Scheme:   "postgres",
		Host:     net.JoinHostPort(host, strconv.Itoa(o.portOr(kubedb.YugabyteDBYSQLPort))),
		Path:     "/" + database,
		RawQuery: "sslmode=disable&connect_timeout=" + strconv.Itoa(int(o.timeout/time.Second)),
	}
	cfg, err := pgx.ParseConfig(u.String())
	if err != nil {
		return nil, err
	}
	cfg.User, cfg.Password = user, password
	if tlsCfg != nil {
		cfg.TLSConfig = tlsCfg
		// Never fall back to plain text.
		cfg.Fallbacks = nil
	}
	return cfg, nil
}

func (o *KubeDBClientBuilder) host() (string, error) {
	switch {
	case o.podName != "":
		if o.db == nil {
			return "", errors.New("no YugabyteDB is given to address the pod through")
		}
		return fmt.Sprintf("%s.%s.%s.svc", o.podName, o.db.GoverningServiceName(), o.db.Namespace), nil
	case o.url != "":
		return o.url, nil
	case o.db != nil:
		return fmt.Sprintf("%s.%s.svc", o.db.ServiceName(), o.db.Namespace), nil
	}
	return "", errors.New("no host: give a YugabyteDB, a pod or a URL")
}

func (o *KubeDBClientBuilder) portOr(def int) int {
	if o.port != nil {
		return *o.port
	}
	return def
}

func (o *KubeDBClientBuilder) credentials() (string, string, error) {
	if o.credentialsSet {
		return o.user, o.password, nil
	}
	if o.db == nil {
		return "", "", errors.New("no YugabyteDB is given to read the auth Secret from")
	}
	data, err := dbsecret.GetData(o.ctx, o.kc, o.db.Namespace, o.db.GetAuthSecretName(), dbapi.IsVirtualAuthSecretReferred(o.db.Spec.AuthSecret))
	if err != nil {
		return "", "", fmt.Errorf("failed to read the auth Secret %s/%s: %w", o.db.Namespace, o.db.GetAuthSecretName(), err)
	}
	user, password := string(data[core.BasicAuthUsernameKey]), string(data[core.BasicAuthPasswordKey])
	if user == "" || password == "" {
		return "", "", fmt.Errorf("the auth Secret %s/%s has no username or password", o.db.Namespace, o.db.GetAuthSecretName())
	}
	return user, password, nil
}
