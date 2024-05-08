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

	"fmt"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	_ "github.com/microsoft/go-mssqldb"
	core "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"xorm.io/xorm"
)

type KubeDBClientBuilder struct {
	kc      client.Client
	db      *api.MSSQLServer
	url     string
	podName string
	ctx     context.Context
}

func NewKubeDBClientBuilder(kc client.Client, db *api.MSSQLServer) *KubeDBClientBuilder {
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

func (o *KubeDBClientBuilder) GetMSSQLXormClient() (*XormClient, error) {
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

func (o *KubeDBClientBuilder) getMSSQLSACredentials() (string, string, error) {
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
		return "", "", fmt.Errorf("DB SA user is not found in secret")
	}
	pass, ok := secret.Data[core.BasicAuthPasswordKey]
	if !ok {
		return "", "", fmt.Errorf("DB  password is not set in secret")
	}
	return string(user), string(pass), nil
}

func (o *KubeDBClientBuilder) getConnectionString() (string, error) {
	user, pass, err := o.getMSSQLSACredentials()
	if err != nil {
		return "", err
	}

	if o.podName != "" {
		o.url = o.getURL()
	}

	// TODO: Add tlsConfig
	tlsConfig := ""
	//if o.db.Spec.RequireSSL && o.db.Spec.TLS != nil {
	//	// get client-secret
	//	var clientSecret core.Secret
	//	err := o.kc.Get(o.ctx, client.ObjectKey{Namespace: o.db.GetNamespace(), Name: o.db.GetCertSecretName(api.MSSQLClientCert)}, &clientSecret)
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
	//		mssql_driver.RegisterTLSConfig(api.MSSQLTLSConfigCustom, &tls.Config{
	//			RootCAs:      certPool,
	//			Certificates: clientCert,
	//		})
	//		tlsConfig = fmt.Sprintf("tls=%s", api.MSSQLTLSConfigCustom)
	//	} else {
	//		tlsConfig = fmt.Sprintf("tls=%s", api.MSSQLTLSConfigSkipVerify)
	//	}
	//}

	connectionString := fmt.Sprintf("server=%s;user id=%s;password=%s;database=master;%s", o.url, user, pass, tlsConfig)
	return connectionString, nil
}
