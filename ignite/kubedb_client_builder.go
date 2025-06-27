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
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"time"

	ignite "github.com/amsokol/ignite-go-client/binary/v1"
	_ "github.com/amsokol/ignite-go-client/sql"
	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"kubedb.dev/apimachinery/apis/kubedb"
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
	timeout time.Duration
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

func (o *KubeDBClientBuilder) WithTimeout(d time.Duration) *KubeDBClientBuilder {
	o.timeout = d
	return o
}

func (o *KubeDBClientBuilder) GetIgniteBinaryClient() (*BinaryClient, error) {
	igniteConnectionInfo := ignite.ConnInfo{
		Network: "tcp",
		Host:    o.Address(),
		Port:    kubedb.IgniteThinPort,
		Major:   1,
		Minor:   1,
		Patch:   0,
		Dialer: net.Dialer{
			Timeout: o.timeout,
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

	if o.db.Spec.TLS != nil {
		igniteTLSConfig, err := o.GetTLSConfig()
		if err != nil {
			return nil, err
		}

		igniteConnectionInfo.TLSConfig = igniteTLSConfig
	}

	igBinClient, err := ignite.Connect(igniteConnectionInfo)
	if err != nil {
		o.log.Error(err, "failed connect to server: %v")
		return &BinaryClient{
			igBinClient,
		}, err
	}

	return &BinaryClient{
		igBinClient,
	}, nil
}

func (o *KubeDBClientBuilder) GetIgniteSqlClient() (*SqlClient, error) {
	dataSource := fmt.Sprintf(
		"tcp://%s:%d/PUBLIC?version=1.1.0"+
			"&timeout=%d",
		o.Address(), kubedb.IgniteThinPort, o.timeout)

	if !o.db.Spec.DisableSecurity {
		err, username, password := o.getUsernamePassword()
		if err != nil {
			return nil, nil
		}
		// Credentials are only needed if they're configured in your Ignite server.
		dataSource += fmt.Sprintf("&username=%s", username) + fmt.Sprintf("&password=%s", password)
	}

	if o.db.Spec.TLS != nil {
		dataSource += fmt.Sprintf("&tls=yes") + fmt.Sprintf("&tls-insecure-skip-verify=yes")
	}

	db, err := sql.Open("ignite", dataSource)
	if err != nil {
		klog.Errorf("failed to open connection: %v", err)
		return &SqlClient{
			db,
		}, err
	}

	return &SqlClient{
		db,
	}, nil
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

	username := strings.TrimSpace(string(authSecret.Data["username"]))
	password := strings.TrimSpace(string(authSecret.Data["password"]))
	return nil, username, password
}

func (igBin *BinaryClient) CreateCache(cacheName string) error {
	// create cache
	if err := igBin.CacheCreateWithName(cacheName); err != nil {
		klog.Error(err, "failed to create cache: %v")
		return err
	}
	return nil
}

func (igBin *BinaryClient) DeleteCache(cacheName string) error {
	// delete cache
	if err := igBin.CacheDestroy(cacheName); err != nil {
		klog.Error(err, "failed to create cache: %v")
		return err
	}
	return nil
}

func (igSql *SqlClient) Ping() error {
	if err := igSql.PingContext(context.TODO()); err != nil {
		klog.Error(err, "Ping failed: %v")
		return err
	}
	return nil
}

func (o *KubeDBClientBuilder) Address() string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", o.db.ServiceName(), o.db.Namespace)
}

func (o *KubeDBClientBuilder) GetCertSecret() (*core.Secret, error) {
	var certSecret core.Secret
	err := o.kc.Get(context.TODO(), types.NamespacedName{
		Name:      o.db.GetIgniteCertSecretName(api.IgniteClientCert),
		Namespace: o.db.Namespace,
	}, &certSecret)
	if err != nil {
		return nil, err
	}
	return &certSecret, nil
}

func (o *KubeDBClientBuilder) GetTLSConfig() (*tls.Config, error) {
	// Secret for Ignite Client Certs
	certSecret, err := o.GetCertSecret()
	if err != nil {
		klog.Error(err, "Failed to get TLS-secret")
		return nil, err
	}

	if certSecret.Data["ca.crt"] == nil || certSecret.Data["tls.crt"] == nil || certSecret.Data["tls.key"] == nil {
		return nil, errors.New("invalid cert-secret.")
	}

	// get tls cert, clientCA and rootCA for tls config
	// use server cert ca for root ca as issuer ref is not taken into account
	clientCA := x509.NewCertPool()
	rootCA := x509.NewCertPool()

	crt, err := tls.X509KeyPair(certSecret.Data[core.TLSCertKey], certSecret.Data[core.TLSPrivateKeyKey])
	if err != nil {
		klog.Error(err, "failed to create certificate for TLS config")
		return nil, err
	}
	clientCA.AppendCertsFromPEM(certSecret.Data[kubedb.CACert])
	rootCA.AppendCertsFromPEM(certSecret.Data[kubedb.CACert])

	tlsConfig := &tls.Config{
		ServerName:   o.db.ServiceName(),
		Certificates: []tls.Certificate{crt},
		ClientCAs:    clientCA,
		RootCAs:      rootCA,
	}
	return tlsConfig, nil
}
