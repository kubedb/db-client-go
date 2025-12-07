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

package druid

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"

	dbapi "kubedb.dev/apimachinery/apis/kubedb/v1"
	olddbapi "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	apiutils "kubedb.dev/apimachinery/pkg/utils"

	druidgo "github.com/grafadruid/go-druid"
	_ "github.com/lib/pq"
	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc       client.Client
	db       *olddbapi.Druid
	url      string
	podName  string
	nodeRole olddbapi.DruidNodeRoleType
	password string
	ctx      context.Context
}

func NewKubeDBClientBuilder(kc client.Client, druid *olddbapi.Druid) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc: kc,
		db: druid,
	}
}

// WithURL must be called after initializing NodeRole
// by calling WithNodeRole function
func (o *KubeDBClientBuilder) WithURL(url string) *KubeDBClientBuilder {
	if url == "" {
		url = o.GetNodesAddress()
	}
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

func (o *KubeDBClientBuilder) WithNodeRole(nodeRole olddbapi.DruidNodeRoleType) *KubeDBClientBuilder {
	o.nodeRole = nodeRole
	return o
}

func (o *KubeDBClientBuilder) WithPassword(password string) *KubeDBClientBuilder {
	o.password = password
	return o
}

func (o *KubeDBClientBuilder) GetDruidClient() (*Client, error) {
	var druidOpts []druidgo.ClientOption
	// Add druid auth credential to the client
	if !o.db.Spec.DisableSecurity {
		authOpts, err := o.getClientAuthOpts()
		if err != nil {
			klog.Error(err, "failed to get client auth options")
			return nil, err
		}
		druidOpts = append(druidOpts, *authOpts)
	}

	// Add druid ssl configs to the client
	if o.db.Spec.EnableSSL {
		sslOpts, err := o.getClientSSLConfig()
		if err != nil {
			klog.Error(err, "failed to get client ssl options")
			return nil, err
		}
		druidOpts = append(druidOpts, *sslOpts)
	}

	druidClient, err := druidgo.NewClient(o.url, druidOpts...)
	if err != nil {
		return nil, err
	}
	return &Client{
		Client: druidClient,
	}, nil
}

func (o *KubeDBClientBuilder) getClientAuthOpts() (*druidgo.ClientOption, error) {
	authSecret := &core.Secret{}
	err := o.kc.Get(o.ctx, types.NamespacedName{
		Namespace: o.db.Namespace,
		Name:      o.db.GetAuthSecretName(),
	}, authSecret)
	if err != nil {
		if kerr.IsNotFound(err) {
			klog.Error(err, "AuthSecret not found")
			return nil, errors.New("auth-secret not found")
		}
		return nil, err
	}

	var password string
	userName := string(authSecret.Data[core.BasicAuthUsernameKey])
	if o.password != "" {
		password = o.password
	} else {
		password = string(authSecret.Data[core.BasicAuthPasswordKey])
	}

	druidAuthOpts := druidgo.WithBasicAuth(userName, password)
	return &druidAuthOpts, nil
}

func (o *KubeDBClientBuilder) getClientSSLConfig() (*druidgo.ClientOption, error) {
	certSecret := &core.Secret{}
	err := o.kc.Get(o.ctx, types.NamespacedName{
		Namespace: o.db.Namespace,
		Name:      o.db.GetCertSecretName(olddbapi.DruidClientCert),
	}, certSecret)
	if err != nil {
		if kerr.IsNotFound(err) {
			klog.Error(err, "Client certificate secret not found")
			return nil, errors.New("client certificate secret is not found")
		}
		klog.Error(err, "Failed to get client certificate Secret")
		return nil, err
	}

	// get tls cert, clientCA and rootCA for tls config
	clientCA := x509.NewCertPool()
	rootCA := x509.NewCertPool()

	crt, err := tls.X509KeyPair(certSecret.Data[core.TLSCertKey], certSecret.Data[core.TLSPrivateKeyKey])
	if err != nil {
		klog.Error(err, "Failed to parse private key pair")
		return nil, err
	}
	clientCA.AppendCertsFromPEM(certSecret.Data[dbapi.TLSCACertFileName])
	rootCA.AppendCertsFromPEM(certSecret.Data[dbapi.TLSCACertFileName])

	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				Certificates: []tls.Certificate{crt},
				ClientAuth:   tls.RequireAndVerifyClientCert,
				ClientCAs:    clientCA,
				RootCAs:      rootCA,
				MaxVersion:   tls.VersionTLS12,
			},
		},
	}
	tlsOpts := druidgo.WithHTTPClient(httpClient)
	return &tlsOpts, nil
}

// GetNodesAddress returns DNS for the nodes based on type of the node
func (o *KubeDBClientBuilder) GetNodesAddress() string {
	var scheme string
	if o.db.Spec.EnableSSL {
		scheme = "https"
	} else {
		scheme = "http"
	}

	baseUrl := fmt.Sprintf("%s://%s-0.%s.%s.svc.%s:%d", scheme, o.db.PetSetName(o.nodeRole), o.db.GoverningServiceName(), o.db.Namespace, apiutils.FindDomain(), o.db.DruidNodeContainerPort(o.nodeRole))
	return baseUrl
}
