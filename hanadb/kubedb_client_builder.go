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

package hanadb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"

	_ "github.com/SAP/go-hdb/driver"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// TenantDatabaseName is the name of the KubeDB managed tenant database
	TenantDatabaseName = "KUBEDB"
	// TenantSystemPassword is the system user password for the tenant database
	TenantSystemPassword = "Tenant1_Strong#2025"
	// SystemDBPort is the default port for SYSTEMDB
	SystemDBPort = "39017"
)

type KubeDBClientBuilder struct {
	kc           client.Client
	db           *api.HanaDB
	url          string
	podName      string
	ctx          context.Context
	databaseName string // Allows specifying SYSTEMDB or tenant database
}

func NewKubeDBClientBuilder(kc client.Client, db *api.HanaDB) *KubeDBClientBuilder {
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

func (o *KubeDBClientBuilder) WithDatabase(databaseName string) *KubeDBClientBuilder {
	o.databaseName = databaseName
	return o
}

func (o *KubeDBClientBuilder) GetHanaDBClient() (*SqlClient, error) {
	if o.ctx == nil {
		o.ctx = context.Background()
	}
	connectionString, err := o.getConnectionString()
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("hdb", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to hanadb: %v", err)
	}

	if err := db.PingContext(o.ctx); err != nil {
		if cerr := db.Close(); cerr != nil {
			err = errors.Wrapf(err, "failed to close hanadb: %v", cerr)
		}
		return nil, fmt.Errorf("failed to ping hanadb database: %v", err)
	}

	return &SqlClient{
		DB: db,
	}, nil
}

func (o *KubeDBClientBuilder) getURL() string {
	return fmt.Sprintf("%s.%s.%s.svc", o.podName, o.db.GoverningServiceName(), o.db.GetNamespace())
}

func (o *KubeDBClientBuilder) getConnectionString() (string, error) {
	var user, pass string
	var err error

	// For tenant database, use tenant credentials if connecting to KUBEDB
	// For SYSTEMDB or default, use the auth secret credentials
	if o.databaseName == TenantDatabaseName {
		user = "SYSTEM"
		pass = TenantSystemPassword
	} else {
		// Authentication credentials from secret
		user, pass, err = o.getHanaDBAuthCredentials()
		if err != nil {
			return "", fmt.Errorf("unable to get hanaDB auth credentials: %s/%s: %v", o.db.Namespace, o.db.Name, err)
		}
	}

	var host string

	if o.url != "" {
		host = o.url // Use provided URL if set via WithURL
	} else {
		if o.podName != "" {
			// For specific pod, use headless service (governing service)
			host = fmt.Sprintf("%s.%s.%s.svc.cluster.local", o.podName, o.db.GoverningServiceName(), o.db.Namespace)
		} else {
			// Default to regular service DNS
			host = fmt.Sprintf("%s.%s.svc.cluster.local", o.db.Name, o.db.Namespace)
		}
	}

	// Port is always 39017 for both SYSTEMDB and tenant databases (local access)
	port := SystemDBPort

	// Build connection string with database parameter if specified
	dbParam := ""
	if o.databaseName != "" {
		dbParam = fmt.Sprintf("?databaseName=%s", o.databaseName)
	}

	// URL-encode username and password to handle special characters like #, @, etc.
	encodedUser := url.QueryEscape(user)
	encodedPass := url.QueryEscape(pass)

	connectionString := fmt.Sprintf("hdb://%s:%s@%s:%s%s%s", encodedUser, encodedPass, host, port, dbParam, tlsConfig)
	return connectionString, nil
}

func (o *KubeDBClientBuilder) getHanaDBAuthCredentials() (string, string, error) {
	var secret core.Secret
	if err := o.kc.Get(o.ctx, client.ObjectKey{Namespace: o.db.Namespace, Name: o.db.GetAuthSecretName()}, &secret); err != nil {
		return "", "", err
	}

	if username, password, ok := extractBasicAuth(secret.Data); ok {
		return username, password, nil
	}

	if username, password, err := extractPasswordJSON(secret.Data); err == nil {
		return username, password, nil
	} else if err != errPasswordJSONNotFound {
		return "", "", err
	}

	return "", "", errors.New("secret does not contain recognizable auth credentials")
}

func extractBasicAuth(data map[string][]byte) (string, string, bool) {
	usernameBytes, uok := data[core.BasicAuthUsernameKey]
	passwordBytes, pok := data[core.BasicAuthPasswordKey]
	if !uok || !pok {
		return "", "", false
	}
	username := string(usernameBytes)
	password := string(passwordBytes)
	if username == "" || password == "" {
		return "", "", false
	}
	return username, password, true
}

var errPasswordJSONNotFound = errors.New("password.json key not found in secret")

func extractPasswordJSON(data map[string][]byte) (string, string, error) {
	passwordJSON, ok := data["password.json"]
	if !ok {
		return "", "", errPasswordJSONNotFound
	}

	var passwordData struct {
		MasterPassword string `json:"master_password"`
	}
	if err := json.Unmarshal(passwordJSON, &passwordData); err != nil {
		return "", "", fmt.Errorf("failed to parse password.json: %v", err)
	}

	username := "SYSTEM" // Default for HANA system DB; make configurable if needed
	password := passwordData.MasterPassword
	if username == "" || password == "" {
		return "", "", errors.New("username or password not specified")
	}

	return username, password, nil
}
