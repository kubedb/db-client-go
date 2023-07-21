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

package pgbouncer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	_ "github.com/lib/pq"
	core "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"kmodules.xyz/client-go/tools/certholder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"xorm.io/xorm"
)

const (
	DefaultPgBouncerDB = "pgbouncer"
)

type KubeDBClientBuilder struct {
	kc          client.Client
	db          *api.PgBouncer
	url         string
	podName     string
	pgBouncerDB string
	ctx         context.Context
}

func NewKubeDBClientBuilder(kc client.Client, db *api.PgBouncer) *KubeDBClientBuilder {
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

func (o *KubeDBClientBuilder) WithPgBouncerDB(pgDB string) *KubeDBClientBuilder {
	o.pgBouncerDB = pgDB
	return o
}

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}

func (o *KubeDBClientBuilder) GetPgbouncerXormClient(ctx context.Context) (*XormClient, error) {
	connector, err := o.getConnectionString(ctx)
	if err != nil {
		return nil, err
	}

	engine, err := xorm.NewEngine("postgres", connector)
	if err != nil {
		return nil, fmt.Errorf("failed to generate pgbouncer client using connection string: %v", err)
	}
	_, err = engine.Query("SHOW HELP")
	if err != nil {
		return nil, fmt.Errorf("failed to run query: %v", err)
	}
	return &XormClient{engine}, nil
}

func (o *KubeDBClientBuilder) getURL() string {
	return fmt.Sprintf("%s.%s.%s.svc", o.podName, o.db.GoverningServiceName(), o.db.Namespace)
}

func (o *KubeDBClientBuilder) getPgBouncerAuthCredentials(ctx context.Context) (string, string, error) {
	if o.db.Spec.AuthSecret == nil {
		return "", "", errors.New("no database secret")
	}
	var secret core.Secret
	err := o.kc.Get(ctx, client.ObjectKey{Namespace: o.db.Namespace, Name: o.db.Spec.AuthSecret.Name}, &secret)
	if err != nil {
		return "", "", err
	}
	return string(secret.Data[core.BasicAuthUsernameKey]), string(secret.Data[core.BasicAuthPasswordKey]), nil
}

func (o *KubeDBClientBuilder) GetPgBouncerClient(ctx context.Context) (*Client, error) {
	connector, err := o.getConnectionString(ctx)
	if err != nil {
		return nil, err
	}
	// connect to database
	db, err := sql.Open("postgres", connector)
	if err != nil {
		return nil, err
	}

	// ping to database to check the connection
	if _, err := db.QueryContext(ctx, "SHOW HELP;"); err != nil {
		closeErr := db.Close()
		if closeErr != nil {
			klog.Errorf("Failed to close client. error: %v", closeErr)
		}
		return nil, err
	}

	return &Client{db}, nil
}

func (o *KubeDBClientBuilder) getConnectionString(ctx context.Context) (string, error) {
	if o.podName != "" {
		o.url = o.getURL()
	}
	dnsName := o.url
	port := api.PgBouncerDatabasePort

	if o.pgBouncerDB == "" {
		o.pgBouncerDB = DefaultPgBouncerDB
	}

	user, pass, err := o.getPgBouncerAuthCredentials(ctx)
	if err != nil {
		return "", fmt.Errorf("DB basic auth is not found for PgBouncer %v/%v", o.db.Namespace, o.db.Name)
	}
	cnnstr := ""
	sslMode := o.db.Spec.SSLMode
	if sslMode == "" {
		sslMode = api.PgBouncerSSLModeDisable
	}

	if o.db.Spec.TLS != nil {
		paths, err := o.getTLSConfig(ctx)
		if err != nil {
			return "", err
		}
		if o.db.Spec.ConnectionPool.AuthType == api.PgBouncerClientAuthModeCert || o.db.Spec.SSLMode == api.PgBouncerSSLModeVerifyCA || o.db.Spec.SSLMode == api.PgBouncerSSLModeVerifyFull {
			cnnstr = fmt.Sprintf("user=%s password=%s host=%s port=%d connect_timeout=15 dbname=%s sslmode=%s sslrootcert=%s sslcert=%s sslkey=%s", user, pass, dnsName, port, o.pgBouncerDB, sslMode, paths.CACert, paths.Cert, paths.Key)
		} else {
			cnnstr = fmt.Sprintf("user=%s password=%s host=%s port=%d connect_timeout=15 dbname=%s sslmode=%s sslrootcert=%s", user, pass, dnsName, port, o.pgBouncerDB, sslMode, paths.CACert)
		}
	} else {
		cnnstr = fmt.Sprintf("user=%s password=%s host=%s port=%d connect_timeout=15 dbname=%s sslmode=%s", user, pass, dnsName, port, o.pgBouncerDB, sslMode)
	}
	return cnnstr, nil
}

func (o *KubeDBClientBuilder) getTLSConfig(ctx context.Context) (*certholder.Paths, error) {
	secretName := o.db.GetCertSecretName(api.PgBouncerClientCert)

	var certSecret core.Secret
	err := o.kc.Get(ctx, client.ObjectKey{Namespace: o.db.Namespace, Name: secretName}, &certSecret)
	if err != nil {
		klog.Error(err, "failed to get certificate secret.", secretName)
		return nil, err
	}

	certs, _ := certholder.DefaultHolder.ForResource(api.SchemeGroupVersion.WithResource(api.ResourcePluralPgBouncer), o.db.ObjectMeta)
	paths, err := certs.Save(&certSecret)
	if err != nil {
		klog.Error(err, "failed to save certificate")
		return nil, err
	}
	return paths, nil
}
