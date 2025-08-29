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

package solr

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	apiutils "kubedb.dev/apimachinery/pkg/utils"
	"net"
	"net/http"
	"time"

	"k8s.io/klog/v2"

	"github.com/Masterminds/semver/v3"
	gerr "github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"kubedb.dev/apimachinery/apis/kubedb"

	"fmt"

	"github.com/go-logr/logr"
	"github.com/go-resty/resty/v2"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc      client.Client
	db      *api.Solr
	url     string
	podName string
	ctx     context.Context
	log     logr.Logger
}

func NewKubeDBClientBuilder(kc client.Client, db *api.Solr) *KubeDBClientBuilder {
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

func (o *KubeDBClientBuilder) WithLog(log logr.Logger) *KubeDBClientBuilder {
	o.log = log
	return o
}

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}

func (o *KubeDBClientBuilder) GetSolrClient() (*Client, error) {
	if o.podName != "" {
		o.url = fmt.Sprintf("%v://%s.%s.%s.svc.%s:%d", o.db.GetConnectionScheme(), o.podName, o.db.GoverningServiceName(), o.db.GetNamespace(), apiutils.FindDomain(), kubedb.SolrRestPort)
	}
	if o.url == "" {
		o.url = o.GetHostPath(o.db)
	}
	if o.db == nil {
		return nil, errors.New("db is empty")
	}
	config := Config{
		host: o.url,
		transport: &http.Transport{
			IdleConnTimeout: time.Second * 10,
			DialContext: (&net.Dialer{
				Timeout:   time.Second * 30,
				KeepAlive: time.Second * 30,
			}).DialContext,
			TLSHandshakeTimeout:   time.Second * 20,
			ResponseHeaderTimeout: time.Second * 20,
			ExpectContinueTimeout: time.Second * 20,
		},
		connectionScheme: o.db.GetConnectionScheme(),
		log:              o.log,
	}

	// If EnableSSL is true set tls config,
	// provide client certs and root CA
	if o.db.Spec.EnableSSL {
		var certSecret core.Secret
		err := o.kc.Get(o.ctx, types.NamespacedName{
			Namespace: o.db.Namespace,
			Name:      o.db.GetCertSecretName(api.SolrClientCert),
		}, &certSecret)
		if err != nil {
			klog.Error(err, "failed to get serverCert secret")
			return nil, err
		}

		// get tls cert, clientCA and rootCA for tls config
		// use server cert ca for rootca as issuer ref is not taken into account
		clientCA := x509.NewCertPool()
		rootCA := x509.NewCertPool()

		crt, err := tls.X509KeyPair(certSecret.Data[core.TLSCertKey], certSecret.Data[core.TLSPrivateKeyKey])
		if err != nil {
			klog.Error(err, "failed to create certificate for TLS config")
			return nil, err
		}
		clientCA.AppendCertsFromPEM(certSecret.Data[kubedb.CACert])
		rootCA.AppendCertsFromPEM(certSecret.Data[kubedb.CACert])

		config.transport.TLSClientConfig = &tls.Config{
			Certificates: []tls.Certificate{crt},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    clientCA,
			RootCAs:      rootCA,
			MaxVersion:   tls.VersionTLS13,
		}
	}

	var authSecret core.Secret
	if !o.db.Spec.DisableSecurity {
		err := o.kc.Get(o.ctx, types.NamespacedName{
			Name:      o.db.GetAuthSecretName(),
			Namespace: o.db.Namespace,
		}, &authSecret)
		if err != nil {
			config.log.Error(err, "failed to get auth secret to get solr client")
			return nil, err
		}
	}
	version, err := semver.NewVersion(o.db.Spec.Version)
	if err != nil {
		return nil, gerr.Wrap(err, "failed to parse version")
	}

	switch {
	case version.Major() >= 9:
		newClient := resty.New()
		newClient.SetScheme(config.connectionScheme).SetBaseURL(config.host).SetTransport(config.transport)
		newClient.SetTimeout(time.Second * 30)
		newClient.SetHeader("Accept", "application/json")
		newClient.SetDisableWarn(true)
		newClient.SetBasicAuth(string(authSecret.Data[core.BasicAuthUsernameKey]), string(authSecret.Data[core.BasicAuthPasswordKey]))
		return &Client{
			&SLClientV9{
				Client: newClient,
				Config: &config,
			},
		}, nil
	case version.Major() == 8:
		newClient := resty.New()
		newClient.SetScheme(config.connectionScheme).SetBaseURL(config.host).SetTransport(config.transport)
		newClient.SetTimeout(time.Second * 30)
		newClient.SetHeader("Accept", "application/json")
		newClient.SetDisableWarn(true)
		newClient.SetBasicAuth(string(authSecret.Data[core.BasicAuthUsernameKey]), string(authSecret.Data[core.BasicAuthPasswordKey]))
		return &Client{
			&SLClientV8{
				Client: newClient,
				Config: &config,
			},
		}, nil
	}

	return nil, fmt.Errorf("unknown version: %s", o.db.Spec.Version)

}

func (o *KubeDBClientBuilder) GetHostPath(db *api.Solr) string {
	return fmt.Sprintf("%v://%s.%s.svc.%s:%d", db.GetConnectionScheme(), db.ServiceName(), db.GetNamespace(), apiutils.FindDomain(), kubedb.SolrRestPort)
}
