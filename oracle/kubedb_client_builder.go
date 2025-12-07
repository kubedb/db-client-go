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
	"net/url"
	"os"
	"path/filepath"

	olddbapi "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	apiutils "kubedb.dev/apimachinery/pkg/utils"

	"github.com/pkg/errors"
	go_ora "github.com/sijms/go-ora/v2"
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
	wallet  string
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

func (o *OracleClientBuilder) WithWallet(wallet string) *OracleClientBuilder {
	o.wallet = wallet
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

	// Fallback to standard connection (with wallet if configured)
	db, err := sql.Open("oracle", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open Oracle connection: %v", err)
	}

	if err := db.PingContext(o.ctx); err != nil {
		cerr := db.Close() // nolint:errcheck
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

	serverURL := o.url
	if serverURL == "" {
		serverURL = PrimaryServiceDNS(o.db)
	}
	// Use the provided URL (e.g., service DNS)
	host := fmt.Sprintf("%v:%v/%v", serverURL, o.port, o.service)

	// Construct basic connection string
	connStr := ""

	if o.db.Spec.TCPSConfig != nil && o.db.Spec.TCPSConfig.TLS != nil {
		// Constract connection string with wallet
		dbname := o.db.Name
		dstDir := o.wallet
		if dstDir == "" {
			dstDir = fmt.Sprintf("/tmp/%s/.tls-wallet", dbname)

			if err := os.MkdirAll(dstDir, 0o755); err != nil {
				fmt.Printf("[ERROR] Failed to create wallet directory: %v\n", err)
			}

			// Read the TLS secret from Kubernetes
			var tlsSecret core.Secret
			secretName := o.db.Name + "-tls-wallet"
			if err := o.kc.Get(o.ctx, client.ObjectKey{Namespace: o.db.Namespace, Name: secretName}, &tlsSecret); err != nil {
				return "", fmt.Errorf("failed to get TLS secret %s: %v", secretName, err)
			}

			// Extract and save all files in the secret data
			for filename, data := range tlsSecret.Data {
				filePath := filepath.Join(dstDir, filename)
				if err := os.WriteFile(filePath, data, 0o600); err != nil {
					return "", fmt.Errorf("failed to write wallet file %s: %v", filename, err)
				}
			}

		}

		// Get service name from database spec
		service := "ORCL"
		if o.db.Spec.Listener != nil && o.db.Spec.Listener.Service != nil {
			service = *o.db.Spec.Listener.Service
		}

		// Build connection string with SSL enabled
		baseURL := go_ora.BuildUrl(serverURL, int(o.port), service, user, pass, nil)

		// Add SSL parameters with proper URL encoding
		params := url.Values{}
		params.Add("SSL", "true")
		params.Add("SSL VERIFY", "false")
		params.Add("WALLET", dstDir)
		params.Add("WALLET PASSWORD", pass)

		// Build final connection string with parameters
		connStr = baseURL + "?" + params.Encode()
		for _, fname := range []string{"cwallet.sso", "ewallet.p12", "server.p12"} {
			filepath.Join(dstDir, fname)
		}
	} else {
		// Construct basic connection string without wallet
		connStr = fmt.Sprintf("oracle://%s:%s@%s", user, pass, host)
	}
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
