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

package mariadb

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"os"

	"kubedb.dev/apimachinery/apis/kubedb"
	dbapi "kubedb.dev/apimachinery/apis/kubedb/v1"
	secret_lib "kubedb.dev/apimachinery/pkg/secret"

	sql_driver "github.com/go-sql-driver/mysql"
	core "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"xorm.io/xorm"
)

const (
	rootCAKey = "ca.crt"

	defaultMariaDBPort = 3306
	// reservedConnectTimeout bounds the attempt on the reserved port, so a
	// server without it configured falls back to the regular port quickly.
	reservedConnectTimeout = "5s"
)

type KubeDBClientBuilder struct {
	kc      client.Client
	db      *dbapi.MariaDB
	url     string
	podName string
	ctx     context.Context
	// reservedPort, when set together with WithPod, is tried before the
	// regular port. It is MariaDB's `extra_port` listener, which is not
	// counted against max_connections.
	reservedPort int
}

func NewKubeDBClientBuilder(kc client.Client, db *dbapi.MariaDB) *KubeDBClientBuilder {
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

// WithReservedPort makes the builder prefer MariaDB's reserved listener
// (server variable `extra_port`) on the pod selected with WithPod, and fall
// back to the regular port when it is not available. Connections on the
// reserved port are not counted against max_connections, so callers such as
// health checks keep working while regular clients are rejected.
func (o *KubeDBClientBuilder) WithReservedPort(port int) *KubeDBClientBuilder {
	o.reservedPort = port
	return o
}

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}

func (o *KubeDBClientBuilder) useReservedPort() bool {
	return o.reservedPort > 0 && o.podName != ""
}

func (o *KubeDBClientBuilder) GetMariaDBClient() (*Client, error) {
	if o.ctx == nil {
		o.ctx = context.Background()
	}
	if o.useReservedPort() {
		c, err := o.getMariaDBClient(o.reservedPort, reservedConnectTimeout)
		if err == nil {
			return c, nil
		}
		klog.V(4).Infof("reserved port %d unavailable on pod %s, using port %d: %v", o.reservedPort, o.podName, defaultMariaDBPort, err)
	}
	return o.getMariaDBClient(defaultMariaDBPort, "")
}

func (o *KubeDBClientBuilder) getMariaDBClient(port int, timeout string) (*Client, error) {
	connector, err := o.getConnectionString(port, timeout)
	if err != nil {
		return nil, err
	}

	// connect to database
	db, err := sql.Open("mysql", connector)
	if err != nil {
		return nil, err
	}

	// ping to database to check the connection
	if err := db.PingContext(o.ctx); err != nil {
		closeErr := db.Close() // nolint:errcheck
		if closeErr != nil {
			klog.Errorf("Failed to close client. error: %v", closeErr)
		}
		return nil, err
	}

	return &Client{db}, nil
}

func (o *KubeDBClientBuilder) GetMariaDBXormClient() (*XormClient, error) {
	if o.ctx == nil {
		o.ctx = context.Background()
	}
	if o.useReservedPort() {
		c, err := o.getMariaDBXormClient(o.reservedPort, reservedConnectTimeout)
		if err == nil {
			return c, nil
		}
		klog.V(4).Infof("reserved port %d unavailable on pod %s, using port %d: %v", o.reservedPort, o.podName, defaultMariaDBPort, err)
	}
	return o.getMariaDBXormClient(defaultMariaDBPort, "")
}

func (o *KubeDBClientBuilder) getMariaDBXormClient(port int, timeout string) (*XormClient, error) {
	connector, err := o.getConnectionString(port, timeout)
	if err != nil {
		return nil, err
	}
	engine, err := xorm.NewEngine("mysql", connector)
	if err != nil {
		return nil, err
	}
	_, err = engine.Query("SELECT 1")
	if err != nil {
		// do not leak the connection pool of a client that never worked
		if closeErr := engine.Close(); closeErr != nil {
			klog.Errorf("Failed to close client. error: %v", closeErr)
		}
		return nil, err
	}
	engine.SetDefaultContext(o.ctx)
	return &XormClient{
		engine,
	}, nil
}

func (o *KubeDBClientBuilder) getMariaDBBasicAuth() (string, string, error) {
	var secretName string
	if o.db.Spec.AuthSecret != nil {
		secretName = o.db.GetAuthSecretName()
	}
	isVirtual := dbapi.IsVirtualAuthSecretReferred(o.db.Spec.AuthSecret)
	data, err := secret_lib.GetData(o.ctx, o.kc, o.db.Namespace, secretName, isVirtual)
	if err != nil {
		return "", "", err
	}
	user, ok := data[core.BasicAuthUsernameKey]
	if !ok {
		return "", "", fmt.Errorf("DB root user is not set")
	}
	pass, ok := data[core.BasicAuthPasswordKey]
	if !ok {
		return "", "", fmt.Errorf("DB root password is not set")
	}
	return string(user), string(pass), nil
}

func (o *KubeDBClientBuilder) SSLEnabledMariaDB() bool {
	if o.db == nil {
		return os.Getenv("REQUIRE_SSL") == "TRUE" || os.Getenv("REQUIRE_SSL") == "ON"
	}
	return o.db.Spec.TLS != nil && o.db.Spec.RequireSSL
}

func (o *KubeDBClientBuilder) getURL() string {
	if o.db == nil {
		if o.url != "" {
			return o.url
		} else {
			return os.Getenv("GOVERNING_SERVICE_NAME")
		}
	} else {
		if o.db.Spec.Distributed {
			return fmt.Sprintf("%s.%s.%s.svc.%s", o.podName, o.db.GoverningServiceName(), o.db.Namespace, kubedb.KubeSliceDomainSuffix)
		} else {
			return fmt.Sprintf("%s.%s.%s.svc", o.podName, o.db.GoverningServiceName(), o.db.Namespace)
		}
	}
}

func (o *KubeDBClientBuilder) getConnectionString(port int, timeout string) (string, error) {
	var user, pass string
	var err error
	if o.db == nil {
		user, pass = getMariaDBBasicAuth()
	} else {
		user, pass, err = o.getMariaDBBasicAuth()
		if err != nil {
			return "", err
		}
	}

	if o.podName != "" {
		o.url = o.getURL()
	}

	tlsConfig := ""
	if o.SSLEnabledMariaDB() {
		// get client-secret

		var secretName string
		if o.db == nil {
			secretName = os.Getenv("TLS_SECRET_NAME")
		} else {
			secretName = o.db.GetCertSecretName(dbapi.MariaDBClientCert)
		}

		var clientSecret core.Secret
		err := o.kc.Get(o.ctx, client.ObjectKey{Namespace: o.db.Namespace, Name: secretName}, &clientSecret)
		if err != nil {
			return "", err
		}

		value, exists := clientSecret.Data[rootCAKey]
		if !exists {
			return "", fmt.Errorf("%v in not present in client secret", rootCAKey)
		}

		cacrt := value
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(cacrt)

		value, exists = clientSecret.Data[core.TLSCertKey]
		if !exists {
			return "", fmt.Errorf("%v in not present in client secret", core.TLSCertKey)
		}
		crt := value

		value, exists = clientSecret.Data[core.TLSPrivateKeyKey]
		if !exists {
			return "", fmt.Errorf("%v in not present in client secret", core.TLSPrivateKeyKey)
		}
		key := value

		cert, err := tls.X509KeyPair(crt, key)
		if err != nil {
			return "", err
		}
		var clientCert []tls.Certificate
		clientCert = append(clientCert, cert)
		err = sql_driver.RegisterTLSConfig(kubedb.MariaDBTLSConfigCustom, &tls.Config{
			RootCAs:      certPool,
			Certificates: clientCert,
		})
		if err != nil {
			return "", err
		}
		tlsConfig = fmt.Sprintf("tls=%s", kubedb.MariaDBTLSConfigCustom)
	}
	params := tlsConfig
	if timeout != "" {
		if params != "" {
			params += "&"
		}
		params += "timeout=" + timeout
	}
	connector := fmt.Sprintf("%v:%v@tcp(%s:%d)/%s?%s", user, pass, o.url, port, "mysql", params)
	return connector, nil
}

func getMariaDBBasicAuth() (string, string) {
	return os.Getenv("MYSQL_ROOT_USERNAME"), os.Getenv("MYSQL_ROOT_PASSWORD")
}
