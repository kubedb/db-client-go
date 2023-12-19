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

package connect

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/go-resty/resty/v2"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"net"
	"net/http"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc         client.Client
	dbConnect  *api.KafkaConnectCluster
	url        string
	podName    string
	postgresDB string
	ctx        context.Context
}

func NewKubeDBClientBuilder(kc client.Client, dbConnect *api.KafkaConnectCluster) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc:        kc,
		dbConnect: dbConnect,
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

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}

func (o *KubeDBClientBuilder) GetKafkaConnectClusterClient() (*Client, error) {
	config := Config{
		host: o.getHostPath(),
		api:  "/",
		transport: &http.Transport{
			IdleConnTimeout: time.Second * 3,
			DialContext: (&net.Dialer{
				Timeout: time.Second * 30,
			}).DialContext,
		},
		connectionScheme: o.getConnectionScheme(),
	}

	// If EnableSSL is true set tls config,
	// provide client certs and root CA
	if o.dbConnect.Spec.EnableSSL {
		var certSecret core.Secret
		err := o.kc.Get(o.ctx, types.NamespacedName{
			Namespace: o.dbConnect.Namespace,
			Name:      o.dbConnect.GetCertSecretName(api.KafkaConnectClusterClientCert),
		}, &certSecret)
		if err != nil {
			config.log.Error(err, "failed to get kafka connect cluster client secret")
			return nil, err
		}

		// get tls cert, clientCA and rootCA for tls config
		// use server cert ca for rootca as issuer ref is not taken into account
		clientCA := x509.NewCertPool()
		rootCA := x509.NewCertPool()

		crt, err := tls.X509KeyPair(certSecret.Data[core.TLSCertKey], certSecret.Data[core.TLSPrivateKeyKey])
		if err != nil {
			config.log.Error(err, "failed to create certificate for TLS config")
			return nil, err
		}
		clientCA.AppendCertsFromPEM(certSecret.Data[api.CACert])
		rootCA.AppendCertsFromPEM(certSecret.Data[api.CACert])

		config.transport.TLSClientConfig = &tls.Config{
			Certificates: []tls.Certificate{crt},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    clientCA,
			RootCAs:      rootCA,
			MaxVersion:   tls.VersionTLS13,
		}
	}

	var username, password string

	// if security is enabled set database credentials in clientConfig
	if !o.dbConnect.Spec.DisableSecurity {
		secret := &core.Secret{}
		err := o.kc.Get(o.ctx, types.NamespacedName{
			Name:      o.dbConnect.Spec.AuthSecret.Name,
			Namespace: o.dbConnect.GetNamespace(),
		}, secret)
		if err != nil {
			return nil, err
		}

		if value, ok := secret.Data[core.BasicAuthUsernameKey]; ok {
			username = string(value)
		} else {
			config.log.Info(fmt.Sprintf("Failed for secret: %s/%s, username is missing", secret.Namespace, secret.Name))
			return nil, errors.New("username is missing")
		}

		if value, ok := secret.Data[core.BasicAuthPasswordKey]; ok {
			password = string(value)
		} else {
			config.log.Info(fmt.Sprintf("Failed for secret: %s/%s, password is missing", secret.Namespace, secret.Name))
			return nil, errors.New("password is missing")
		}

		config.username = username
		config.password = password
	}

	newClient := resty.New()
	newClient.SetTransport(config.transport).SetScheme(config.connectionScheme).SetBaseURL(config.host)
	newClient.SetHeader("Accept", "application/json")
	newClient.SetBasicAuth(config.username, config.password)
	newClient.SetTimeout(time.Second * 30)
	newClient.SetDisableWarn(true)

	return &Client{
		Client: newClient,
		Config: &config,
	}, nil
}

// return host path in
// format https://svc_name.namespace.svc:8083/
func (o *KubeDBClientBuilder) getHostPath() string {
	return fmt.Sprintf("%v://%s.%s.svc:%d", o.getConnectionScheme(), o.dbConnect.ServiceName(), o.dbConnect.GetNamespace(), 8083)
}

// will remove later
func (o *KubeDBClientBuilder) getConnectionScheme() string {
	if o.dbConnect.Spec.EnableSSL && o.dbConnect.Spec.TLS != nil {
		return "https"
	}
	return "http"
}