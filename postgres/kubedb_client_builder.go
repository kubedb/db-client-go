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

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	core "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"kmodules.xyz/client-go/tools/certholder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"xorm.io/xorm"
)

const (
	DefaultPostgresDB = "postgres"
)

type KubeDBClientBuilder struct {
	kc         client.Client
	db         *api.Postgres
	url        string
	podName    string
	postgresDB string
	ctx        context.Context
}

func NewKubeDBClientBuilder(kc client.Client, db *api.Postgres) *KubeDBClientBuilder {
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

func (o *KubeDBClientBuilder) WithPostgresDB(pgDB string) *KubeDBClientBuilder {
	o.postgresDB = pgDB
	return o
}

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}

func (o *KubeDBClientBuilder) GetPostgresXormClient() (*XormClient, error) {
	if o.ctx == nil {
		o.ctx = context.Background()
	}
	connector, err := o.getConnectionString()
	if err != nil {
		return nil, err
	}

	engine, err := xorm.NewEngine("postgres", connector)
	if err != nil {
		return nil, fmt.Errorf("failed to generate postgres client using connection string: %v", err)
	}
	_, err = engine.Query("SELECT 1")
	if err != nil {
		return nil, fmt.Errorf("failed to run query: %v", err)
	}
	engine.SetDefaultContext(o.ctx)
	return &XormClient{engine}, nil
}

func (o *KubeDBClientBuilder) getURL() string {
	return fmt.Sprintf("%s.%s.%s.svc", o.podName, o.db.GoverningServiceName(), o.db.Namespace)
}

func (o *KubeDBClientBuilder) getPostgresAuthCredentials() (string, string, error) {
	if o.db.Spec.AuthSecret == nil {
		return "", "", errors.New("no database secret")
	}
	var secret core.Secret
	err := o.kc.Get(o.ctx, client.ObjectKey{Namespace: o.db.Namespace, Name: o.db.Spec.AuthSecret.Name}, &secret)
	if err != nil {
		return "", "", err
	}
	return string(secret.Data[core.BasicAuthUsernameKey]), string(secret.Data[core.BasicAuthPasswordKey]), nil
}

func (o *KubeDBClientBuilder) GetPostgresClient() (*Client, error) {
	if o.ctx == nil {
		o.ctx = context.Background()
	}
	connector, err := o.getConnectionString()
	if err != nil {
		return nil, err
	}
	// connect to database
	db, err := sql.Open("postgres", connector)
	if err != nil {
		return nil, err
	}

	// ping to database to check the connection
	// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	// defer cancel()
	if err := db.PingContext(o.ctx); err != nil {
		closeErr := db.Close()
		if closeErr != nil {
			klog.Errorf("Failed to close client. error: %v", closeErr)
		}
		return nil, err
	}

	return &Client{db}, nil
}

func (o *KubeDBClientBuilder) getConnectionString() (string, error) {
	if o.podName != "" {
		o.url = o.getURL()
	}
	dnsName := o.url
	port := 5432

	if o.postgresDB == "" {
		o.postgresDB = DefaultPostgresDB
	}

	user, pass, err := o.getPostgresAuthCredentials()
	if err != nil {
		return "", fmt.Errorf("DB basic auth is not found for PostgreSQL %v/%v", o.db.Namespace, o.db.Name)
	}
	cnnstr := ""
	sslMode := o.db.Spec.SSLMode

	//  sslMode == "prefer" and sslMode == "allow"  don't have support for github.com/lib/pq postgres client. as we are using
	// github.com/lib/pq postgres client utils for connecting our server we need to access with  any of require , verify-ca, verify-full or disable.
	// here we have chosen "require" sslmode to connect postgres as a client
	if sslMode == "prefer" || sslMode == "allow" {
		sslMode = "require"
	}
	if o.db.Spec.TLS != nil {
		secretName := o.db.GetCertSecretName(api.PostgresClientCert)

		var certSecret core.Secret
		err := o.kc.Get(o.ctx, client.ObjectKey{Namespace: o.db.Namespace, Name: secretName}, &certSecret)
		if err != nil {
			klog.Error(err, "failed to get certificate secret.", secretName)
			return "", err
		}

		certs, _ := certholder.DefaultHolder.ForResource(api.SchemeGroupVersion.WithResource(api.ResourcePluralPostgres), o.db.ObjectMeta)
		paths, err := certs.Save(&certSecret)
		if err != nil {
			klog.Error(err, "failed to save certificate")
			return "", err
		}
		if o.db.Spec.ClientAuthMode == api.ClientAuthModeCert {
			cnnstr = fmt.Sprintf("user=%s password=%s host=%s port=%d connect_timeout=10 dbname=%s sslmode=%s sslrootcert=%s sslcert=%s sslkey=%s", user, pass, dnsName, port, o.postgresDB, sslMode, paths.CACert, paths.Cert, paths.Key)
		} else {
			cnnstr = fmt.Sprintf("user=%s password=%s host=%s port=%d connect_timeout=10 dbname=%s sslmode=%s sslrootcert=%s", user, pass, dnsName, port, o.postgresDB, sslMode, paths.CACert)
		}
	} else {
		cnnstr = fmt.Sprintf("user=%s password=%s host=%s port=%d connect_timeout=10 dbname=%s sslmode=%s", user, pass, dnsName, port, o.postgresDB, sslMode)
	}
	return cnnstr, nil
}
