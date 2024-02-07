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

package mssql

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/microsoft/go-mssqldb"
	core "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	mapi "kubedb.dev/mssql/api/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"xorm.io/xorm"
)

type KubeDBClientBuilder struct {
	kc      client.Client
	db      *mapi.MsSQL
	url     string
	podName string
	ctx     context.Context
}

func NewKubeDBClientBuilder(kc client.Client, db *mapi.MsSQL) *KubeDBClientBuilder {
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

func (o *KubeDBClientBuilder) GetMsSQLClient() (*Client, error) {
	if o.ctx == nil {
		o.ctx = context.Background()
	}

	connector, err := o.getConnectionString()
	if err != nil {
		return nil, err
	}

	// connect to database
	db, err := sql.Open("mssql", connector)
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

func (o *KubeDBClientBuilder) GetMsSQLXormClient() (*XormClient, error) {
	if o.ctx == nil {
		o.ctx = context.Background()
	}
	connector, err := o.getConnectionString()
	if err != nil {
		return nil, err
	}
	engine, err := xorm.NewEngine("mssql", connector)
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

func (o *KubeDBClientBuilder) getURL() string {
	return fmt.Sprintf("%s.%s.%s.svc", o.podName, o.db.GoverningServiceName(), o.db.Namespace)
}

func (o *KubeDBClientBuilder) getMsSQLSACredentials() (string, string, error) {
	db := o.db
	var secretName string
	if db.Spec.AuthSecret != nil {
		secretName = db.GetAuthSecretName()
	}
	var secret core.Secret
	err := o.kc.Get(o.ctx, client.ObjectKey{Namespace: db.Namespace, Name: secretName}, &secret)
	if err != nil {
		return "", "", err
	}
	user, ok := secret.Data[core.BasicAuthUsernameKey]
	if !ok {
		return "", "", fmt.Errorf("DB sa user is not found in secret")
	}
	pass, ok := secret.Data[core.BasicAuthPasswordKey]
	if !ok {
		return "", "", fmt.Errorf("DB SA password is not set in secret")
	}
	return string(user), string(pass), nil
}

func (o *KubeDBClientBuilder) getConnectionString() (string, error) {
	user, pass, err := o.getMsSQLSACredentials()
	if err != nil {
		return "", err
	}

	if o.podName != "" {
		o.url = o.getURL()
	}

	tlsConfig := ""
	//if o.db.Spec.RequireSSL && o.db.Spec.TLS != nil {
	//	// get client-secret
	//	var clientSecret core.Secret
	//	err := o.kc.Get(o.ctx, client.ObjectKey{Namespace: o.db.GetNamespace(), Name: o.db.GetCertSecretName(api.MsSQLClientCert)}, &clientSecret)
	//	if err != nil {
	//		return "", err
	//	}
	//	cacrt := clientSecret.Data["ca.crt"]
	//	certPool := x509.NewCertPool()
	//	certPool.AppendCertsFromPEM(cacrt)
	//
	//	crt := clientSecret.Data["tls.crt"]
	//	key := clientSecret.Data["tls.key"]
	//	cert, err := tls.X509KeyPair(crt, key)
	//	if err != nil {
	//		return "", err
	//	}
	//	var clientCert []tls.Certificate
	//	clientCert = append(clientCert, cert)
	//
	//	// tls custom setup
	//	if o.db.Spec.RequireSSL {
	//		mssql_driver.RegisterTLSConfig(api.MsSQLTLSConfigCustom, &tls.Config{
	//			RootCAs:      certPool,
	//			Certificates: clientCert,
	//		})
	//		tlsConfig = fmt.Sprintf("tls=%s", api.MsSQLTLSConfigCustom)
	//	} else {
	//		tlsConfig = fmt.Sprintf("tls=%s", api.MsSQLTLSConfigSkipVerify)
	//	}
	//}

	connector := fmt.Sprintf("sqlserver://%s:%s@%s:%d?%s", user, pass, o.url, 1433, tlsConfig)
	return connector, nil
}
