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

	"kubedb.dev/apimachinery/apis/kubedb"
	dbapi "kubedb.dev/apimachinery/apis/kubedb/v1"

	sql_driver "github.com/go-sql-driver/mysql"
	core "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"xorm.io/xorm"
)

const (
	rootCAKey = "ca.crt"
)

type KubeDBClientBuilder struct {
	kc      client.Client
	db      *dbapi.MariaDB
	url     string
	podName string
	ctx     context.Context
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

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}

func (o *KubeDBClientBuilder) GetMariaDBClient() (*Client, error) {
	if o.ctx == nil {
		o.ctx = context.Background()
	}

	connector, err := o.getConnectionString()
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
		closeErr := db.Close()
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

	connector, err := o.getConnectionString()
	if err != nil {
		return nil, err
	}
	engine, err := xorm.NewEngine("mysql", connector)
	if err != nil {
		return nil, err
	}
	_, err = engine.Query("SELECT 1")
	if err != nil {
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
	var secret core.Secret
	err := o.kc.Get(o.ctx, client.ObjectKey{Namespace: o.db.Namespace, Name: secretName}, &secret)
	if err != nil {
		return "", "", err
	}
	user, ok := secret.Data[core.BasicAuthUsernameKey]
	if !ok {
		return "", "", fmt.Errorf("DB root user is not set")
	}
	pass, ok := secret.Data[core.BasicAuthPasswordKey]
	if !ok {
		return "", "", fmt.Errorf("DB root password is not set")
	}
	return string(user), string(pass), nil
}

func (o *KubeDBClientBuilder) SSLEnabledMariaDB() bool {
	return o.db.Spec.TLS != nil && o.db.Spec.RequireSSL
}

func (o *KubeDBClientBuilder) getURL() string {
	return fmt.Sprintf("%s.%s.%s.svc.cluster.local", o.podName, o.db.GoverningServiceName(), o.db.Namespace)
}

func (o *KubeDBClientBuilder) getConnectionString() (string, error) {
	user, pass, err := o.getMariaDBBasicAuth()
	if err != nil {
		return "", err
	}

	if o.podName != "" {
		o.url = o.getURL()
	}

	tlsConfig := ""
	if o.SSLEnabledMariaDB() {
		// get client-secret
		var clientSecret core.Secret
		err := o.kc.Get(o.ctx, client.ObjectKey{Namespace: o.db.Namespace, Name: o.db.GetCertSecretName(dbapi.MariaDBClientCert)}, &clientSecret)
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
	connector := fmt.Sprintf("%v:%v@tcp(%s:%d)/%s?%s", user, pass, o.url, 3306, "mysql", tlsConfig)
	return connector, nil
}
