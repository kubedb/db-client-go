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
	"fmt"

	olddbapi "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	apiutils "kubedb.dev/apimachinery/pkg/utils"

	"github.com/pkg/errors"
	_ "github.com/sijms/go-ora/v2" // Oracle driver
	core "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type OracleClientBuilder struct {
	kc      client.Client
	db      *olddbapi.Oracle
	url     string
	port    int32
	service string
	ctx     context.Context
}

func NewOracleClientBuilder(kc client.Client, db *olddbapi.Oracle) *OracleClientBuilder {
	return &OracleClientBuilder{
		kc: kc,
		db: db,
	}
}

func (o *OracleClientBuilder) WithURL(url string) *OracleClientBuilder {
	o.url = url
	return o
}

func (o *OracleClientBuilder) WithPort(port int32) *OracleClientBuilder {
	o.port = port
	return o
}

func (o *OracleClientBuilder) WithService(svc string) *OracleClientBuilder {
	o.service = svc
	return o
}

func (o *OracleClientBuilder) WithContext(ctx context.Context) *OracleClientBuilder {
	o.ctx = ctx
	return o
}

func (o *OracleClientBuilder) GetOracleClient() (*sql.DB, error) {
	if o.ctx == nil {
		o.ctx = context.Background()
	}

	connStr, err := o.getConnectionString()
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("oracle", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open Oracle connection: %v", err)
	}

	if err := db.PingContext(o.ctx); err != nil {
		cerr := db.Close()
		if cerr != nil {
			err = errors.Wrapf(err, "failed to close Oracle connection: %v", cerr)
		}
		return nil, fmt.Errorf("failed to ping Oracle database: %v", err)
	}

	return db, nil
}

func (o *OracleClientBuilder) getConnectionString() (string, error) {
	// Get authentication credentials
	user, pass, err := o.getOracleAuthCredentials()
	if err != nil {
		return "", fmt.Errorf("failed to get auth credentials for Oracle %s/%s: %v", o.db.Namespace, o.db.Name, err)
	}

	url := o.url
	if url == "" {
		url = PrimaryServiceDNS(o.db)
	}
	// Use the provided URL (e.g., service DNS)
	host := fmt.Sprintf("%v:%v/%v", url, o.port, o.service)

	// Construct basic connection string
	connStr := fmt.Sprintf("oracle://%s:%s@%s", user, pass, host)

	return connStr, nil
}

func (o *OracleClientBuilder) getOracleAuthCredentials() (string, string, error) {
	if o.db.Spec.AuthSecret == nil {
		return "", "", errors.New("no database secret provided")
	}

	var secret core.Secret
	err := o.kc.Get(o.ctx, client.ObjectKey{Namespace: o.db.Namespace, Name: o.db.GetAuthSecretName()}, &secret)
	if err != nil {
		return "", "", err
	}
	username := string(secret.Data[core.BasicAuthUsernameKey])
	password := string(secret.Data[core.BasicAuthPasswordKey])

	if username == "" || password == "" {
		return "", "", errors.New("username or password missing in secret")
	}
	return username, password, nil
}

// PrimaryServiceDNS make primary host dns with require template
func PrimaryServiceDNS(db *olddbapi.Oracle) string {
	return fmt.Sprintf("%v.%v.svc.%s", db.ServiceName(), db.Namespace, apiutils.FindDomain())
}
