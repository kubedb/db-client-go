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

package ignite

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"time"

	ignite "github.com/amsokol/ignite-go-client/binary/v1"
	_ "github.com/amsokol/ignite-go-client/sql"
	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc      client.Client
	db      *api.Ignite
	log     logr.Logger
	url     string
	podName string
	ctx     context.Context
}

func NewKubeDBClientBuilder(kc client.Client, db *api.Ignite) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc: kc,
		db: db,
	}
}

func (o *KubeDBClientBuilder) WithPod(podName string) *KubeDBClientBuilder {
	o.podName = podName
	return o
}

func (o *KubeDBClientBuilder) WithLog(log logr.Logger) *KubeDBClientBuilder {
	o.log = log
	return o
}

func (o *KubeDBClientBuilder) WithURL(url string) *KubeDBClientBuilder {
	o.url = url
	return o
}

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}

func (o *KubeDBClientBuilder) GetIgniteClient() (*Client, error) {

	igniteConnectionInfo := ignite.ConnInfo{
		Network: "tcp",
		Host:    o.Address(),
		Port:    10800,
		Major:   1,
		Minor:   1,
		Patch:   0,
		Dialer: net.Dialer{
			Timeout: 10 * time.Second,
		},
	}
	if !o.db.Spec.DisableSecurity {
		err, username, password := o.getUsernamePassword()
		if err != nil {
			return nil, err
		}

		igniteConnectionInfo.Username = username
		igniteConnectionInfo.Password = password
	}

	igclient, err := ignite.Connect(igniteConnectionInfo)
	if err != nil {
		o.log.Error(err, "failed connect to server: %v")
		return &Client{
			igclient,
		}, err
	}

	return &Client{
		igclient,
	}, nil
}

func (o *KubeDBClientBuilder) GetIgniteSqlClient() (*sql.DB, error) {
	dataSource := "tcp://localhost:10800/PUBLIC?" + "version=1.1.0" +
		// Don't set "tls=yes" if your Ignite server
		// isn't configured with any TLS certificates.
		"&tls-insecure-skip-verify=yes" + "&page-size=10000" + "&timeout=5000"

	if !o.db.Spec.DisableSecurity {
		err, username, password := o.getUsernamePassword()
		if err != nil {
			return nil, nil
		}
		// Credentials are only needed if they're configured in your Ignite server.
		dataSource += fmt.Sprintf("&username=%s", username) + fmt.Sprintf("&password=%s", password)
	}

	db, err := sql.Open("ignite", dataSource)
	if err != nil {
		fmt.Printf("failed to open connection: %v", err)
	}

	return db, nil
}

func (o *KubeDBClientBuilder) getUsernamePassword() (error, string, string) {
	authSecret := &core.Secret{}

	err := o.kc.Get(o.ctx, types.NamespacedName{
		Namespace: o.db.Namespace,
		Name:      o.db.GetAuthSecretName(),
	}, authSecret)
	if err != nil {
		o.log.Error(err, "Failed to get auth-secret")
		return errors.New("auth-secret is not found"), "", ""
	}

	return nil, string(authSecret.Data[core.BasicAuthUsernameKey]), string(authSecret.Data[core.BasicAuthPasswordKey])
}

func (o *KubeDBClientBuilder) CreateCache(cacheName string) error {
	// create cache
	db, err := NewKubeDBClientBuilder(o.kc, o.db).GetIgniteClient()
	if err != nil {
		o.log.Error(err, "Failed to get ignite client: %v")
		return err
	}
	if err := db.CacheCreateWithName(cacheName); err != nil {
		o.log.Error(err, "failed to create cache: %v")
		return err
	}
	return nil
}

func (o *KubeDBClientBuilder) Ping(sqlClient *sql.DB) error {
	if err := sqlClient.PingContext(o.ctx); err != nil {
		o.log.Error(err, "ping failed: %v")
		return err
	}
	return nil
}

func (o *KubeDBClientBuilder) AlterUserPassword(sqlClient *sql.DB, password string) error {
	_, err := sqlClient.ExecContext(o.ctx, fmt.Sprintf(`ALTER USER "ignite" WITH PASSWORD '%s'`, password))
	if err != nil {
		o.log.Error(err, "failed sql execute: %v")
		return err
	}
	return nil
}

func (o *KubeDBClientBuilder) Address() string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", o.db.GoverningServiceName(), o.db.Namespace)
}
