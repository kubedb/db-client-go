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
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"strconv"

	hdbdriver "github.com/SAP/go-hdb/driver"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	"kubedb.dev/apimachinery/apis/kubedb"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	connector, err := o.getConnector()
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)

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

func (o *KubeDBClientBuilder) getConnector() (*hdbdriver.Connector, error) {
	var user, pass string
	var err error

	// Authentication credentials from secret
	user, pass, err = o.getHanaDBAuthCredentials()
	if err != nil {
		return nil, fmt.Errorf("unable to get hanaDB auth credentials: %s/%s: %v", o.db.Namespace, o.db.Name, err)
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

	// Port is always 39017 for both SYSTEMDB and tenant databases
	port := kubedb.HanaDBSystemDBSQLPort

	connector := hdbdriver.NewBasicAuthConnector(net.JoinHostPort(host, strconv.Itoa(port)), user, pass)
	if o.databaseName != "" {
		connector = connector.WithDatabase(o.databaseName)
	}

	tlsConfig, err := o.getTLSConfig(host)
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		connector.SetTLSConfig(tlsConfig)
	}

	return connector, nil
}

func (o *KubeDBClientBuilder) getTLSConfig(host string) (*tls.Config, error) {
	if o.db.Spec.TLS == nil || o.db.Spec.TLS.ClientTLS == nil || !*o.db.Spec.TLS.ClientTLS {
		return nil, nil
	}

	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	if o.db.Spec.TLS.ServerName != "" {
		cfg.ServerName = o.db.Spec.TLS.ServerName
	} else if net.ParseIP(host) != nil {
		cfg.ServerName = host
	} else if o.podName != "" {
		cfg.ServerName = fmt.Sprintf("%s.%s.%s.svc.cluster.local", o.podName, o.db.GoverningServiceName(), o.db.Namespace)
	} else {
		cfg.ServerName = host
	}
	cfg.InsecureSkipVerify = o.db.Spec.TLS.InsecureSkipVerify //nolint:gosec

	clientSecretName := o.db.GetCertSecretName(api.HanaDBClientCert)
	if clientSecretName == "" {
		return nil, fmt.Errorf("HanaDB client TLS is enabled but %q certificate alias is not configured", api.HanaDBClientCert)
	}

	var secret core.Secret
	if err := o.kc.Get(o.ctx, client.ObjectKey{Namespace: o.db.Namespace, Name: clientSecretName}, &secret); err != nil {
		return nil, fmt.Errorf("failed to read HanaDB TLS secret %s/%s: %v", o.db.Namespace, clientSecretName, err)
	}

	if caPEM, ok := secret.Data["ca.crt"]; ok && len(caPEM) > 0 {
		rootCAs, err := x509.SystemCertPool()
		if err != nil || rootCAs == nil {
			rootCAs = x509.NewCertPool()
		}
		if !rootCAs.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("failed to parse %q from HanaDB TLS secret %s/%s", "ca.crt", o.db.Namespace, clientSecretName)
		}
		cfg.RootCAs = rootCAs
	} else if !cfg.InsecureSkipVerify {
		return nil, fmt.Errorf("HanaDB TLS secret %s/%s must contain %q unless insecureSkipVerify is enabled", o.db.Namespace, clientSecretName, "ca.crt")
	}

	certPEM, certOK := secret.Data[core.TLSCertKey]
	keyPEM, keyOK := secret.Data[core.TLSPrivateKeyKey]
	switch {
	case certOK && keyOK:
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate from HanaDB TLS secret %s/%s: %v", o.db.Namespace, clientSecretName, err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	case certOK != keyOK:
		return nil, fmt.Errorf("HanaDB TLS secret %s/%s must contain both %q and %q when configuring client certificates", o.db.Namespace, clientSecretName, core.TLSCertKey, core.TLSPrivateKeyKey)
	}

	return cfg, nil
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

	username := "SYSTEM" // Default for HANA system DB;
	password := passwordData.MasterPassword
	if username == "" || password == "" {
		return "", "", errors.New("password.json does not contain master_password")
	}

	return username, password, nil
}
