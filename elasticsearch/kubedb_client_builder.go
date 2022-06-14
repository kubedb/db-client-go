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

package elasticsearch

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	catalog "kubedb.dev/apimachinery/apis/catalog/v1alpha1"
	"kubedb.dev/apimachinery/apis/dashboard/v1alpha1"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	"github.com/Masterminds/semver/v3"
	esv5 "github.com/elastic/go-elasticsearch/v5"
	esv6 "github.com/elastic/go-elasticsearch/v6"
	esv7 "github.com/elastic/go-elasticsearch/v7"
	esv8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

type KubeDBClientBuilder struct {
	kc      kubernetes.Interface
	db      *api.Elasticsearch
	url     string
	podName string
	ctx     context.Context
}

func NewKubeDBClientBuilder(kc kubernetes.Interface, db *api.Elasticsearch) *KubeDBClientBuilder {
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

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}

func (o *KubeDBClientBuilder) GetElasticClient(opt ClientOptions) (*Client, error) {
	if o.podName != "" {
		o.url = o.ServiceURL()
	}
	if o.url == "" {
		o.url = o.ServiceURL()
	}
	if o.db == nil || opt.ESVersion == nil {
		return nil, errors.New("db or esVersion is empty")
	}

	var username, password string

	if opt.Secret != nil {
		if value, ok := opt.Secret.Data[core.BasicAuthUsernameKey]; ok {
			username = string(value)
		} else {
			klog.Errorf("Failed for secret: %s/%s, username is missing", opt.Secret.Namespace, opt.Secret.Name)
			return nil, errors.New("username is missing")
		}

		if value, ok := opt.Secret.Data[core.BasicAuthPasswordKey]; ok {
			password = string(value)
		} else {
			klog.Errorf("Failed for secret: %s/%s, password is missing", opt.Secret.Namespace, opt.Secret.Name)
			return nil, errors.New("password is missing")
		}
	}

	// parse version
	version, err := semver.NewVersion(opt.ESVersion.Spec.Version)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse version")
	}

	switch {
	case opt.ESVersion.Spec.AuthPlugin == catalog.ElasticsearchAuthPluginXpack ||
		opt.ESVersion.Spec.AuthPlugin == catalog.ElasticsearchAuthPluginSearchGuard ||
		opt.ESVersion.Spec.AuthPlugin == catalog.ElasticsearchAuthPluginOpenDistro:
		switch {
		// For Elasticsearch 5.x.x
		case version.Major() == 5:
			esClient, err := esv5.NewClient(esv5.Config{
				Addresses: []string{o.url},
				Username:  username,
				Password:  password,
				Transport: &http.Transport{
					IdleConnTimeout: 3 * time.Second,
					DialContext: (&net.Dialer{
						Timeout: 30 * time.Second,
					}).DialContext,
					TLSClientConfig: &tls.Config{
						InsecureSkipVerify: true,
						MaxVersion:         tls.VersionTLS12,
					},
				},
			})
			if err != nil {
				klog.Errorf("Failed to create HTTP client for Elasticsearch: %s/%s with: %s", o.db.Namespace, o.db.Name, err.Error())
				return nil, err
			}
			// do a manual health check to test client
			res, err := esClient.Cluster.Health(
				esClient.Cluster.Health.WithPretty(),
			)
			if err != nil {
				return nil, err
			}
			defer func(Body io.ReadCloser) {
				err := Body.Close()
				if err != nil {
					klog.Errorf("failed to close response body", err)
				}
			}(res.Body)

			if res.IsError() {
				return nil, fmt.Errorf("health check failed with status code: %d", res.StatusCode)
			}
			return &Client{
				&ESClientV5{client: esClient},
			}, nil

		// for Elasticsearch 6.x.x
		case version.Major() == 6:
			defaultTLSConfig, err := o.getDefaultTLSConfig()
			if err != nil {
				klog.Errorf("Failed get default TLS configuration")
				return nil, err

			}

			esClient, err := esv6.NewClient(esv6.Config{
				Addresses:         []string{o.url},
				Username:          username,
				Password:          password,
				EnableDebugLogger: true,
				DisableRetry:      true,
				Transport: &http.Transport{
					IdleConnTimeout: 3 * time.Second,
					DialContext: (&net.Dialer{
						Timeout: 30 * time.Second,
					}).DialContext,
					TLSClientConfig: defaultTLSConfig,
				},
			})
			if err != nil {
				klog.Errorf("Failed to create HTTP client for Elasticsearch: %s/%s with: %s", o.db.Namespace, o.db.Name, err.Error())
				return nil, err
			}
			res, err := esapi.PingRequest{}.Do(o.ctx, esClient.Transport)
			if err != nil {
				return nil, err
			}

			defer func(Body io.ReadCloser) {
				err = Body.Close()
				if err != nil {
					klog.Errorf("failed to close response body", err)
				}
			}(res.Body)

			if res.IsError() {
				return nil, fmt.Errorf("cluster ping request failed with status code: %d", res.StatusCode)
			}
			return &Client{
				&ESClientV6{client: esClient},
			}, nil

		// for Elasticsearch 7.x.x
		case version.Major() == 7:

			defaultTLSConfig, err := o.getDefaultTLSConfig()
			if err != nil {
				klog.Errorf("Failed get default TLS configuration")
				return nil, err

			}

			esClient, err := esv7.NewClient(esv7.Config{
				Addresses:         []string{o.url},
				Username:          username,
				Password:          password,
				EnableDebugLogger: true,
				DisableRetry:      true,
				Transport: &http.Transport{
					IdleConnTimeout: 3 * time.Second,
					DialContext: (&net.Dialer{
						Timeout: 30 * time.Second,
					}).DialContext,
					TLSClientConfig: defaultTLSConfig,
				},
			})
			if err != nil {
				klog.Errorf("Failed to create HTTP client for Elasticsearch: %s/%s with: %s", o.db.Namespace, o.db.Name, err.Error())
				return nil, err
			}

			res, err := esapi.PingRequest{}.Do(o.ctx, esClient.Transport)
			if err != nil {
				return nil, err
			}

			defer func(Body io.ReadCloser) {
				err = Body.Close()
				if err != nil {
					klog.Errorf("failed to close response body", err)
				}
			}(res.Body)

			if res.IsError() {
				return nil, fmt.Errorf("cluster ping request failed with status code: %d", res.StatusCode)
			}
			return &Client{
				&ESClientV7{client: esClient},
			}, nil

			// for Elasticsearch 8.x.x

		// for Elasticsearch 8.x.x
		case version.Major() == 8:
			defaultTLSConfig, err := o.getDefaultTLSConfig()
			if err != nil {
				klog.Errorf("Failed get default TLS configuration")
				return nil, err

			}

			esClient, err := esv8.NewClient(esv8.Config{
				Addresses:         []string{o.url},
				Username:          username,
				Password:          password,
				EnableDebugLogger: true,
				DisableRetry:      true,
				Transport: &http.Transport{
					IdleConnTimeout: 3 * time.Second,
					DialContext: (&net.Dialer{
						Timeout: 30 * time.Second,
					}).DialContext,
					TLSClientConfig: defaultTLSConfig,
				},
			})
			if err != nil {
				klog.Errorf("Failed to create HTTP client for Elasticsearch: %s/%s with: %s", o.db.Namespace, o.db.Name, err.Error())
				return nil, err
			}

			res, err := esapi.PingRequest{}.Do(o.ctx, esClient.Transport)
			if err != nil {
				return nil, err
			}

			defer func(Body io.ReadCloser) {
				err = Body.Close()
				if err != nil {
					klog.Errorf("failed to close response body", err)
				}
			}(res.Body)

			if res.IsError() {
				return nil, fmt.Errorf("cluster ping request failed with status code: %d", res.StatusCode)
			}

			return &Client{
				&ESClientV8{client: esClient},
			}, nil
		}

	case opt.ESVersion.Spec.AuthPlugin == catalog.ElasticsearchAuthPluginOpenSearch:
		switch {
		case version.Major() == 1:
			defaultTLSConfig, err := o.getDefaultTLSConfig()
			if err != nil {
				klog.Errorf("Failed get default TLS configuration")
				return nil, err

			}

			osClient, err := opensearch.NewClient(opensearch.Config{
				Addresses:         []string{o.url},
				Username:          username,
				Password:          password,
				EnableDebugLogger: true,
				DisableRetry:      true,
				Transport: &http.Transport{
					IdleConnTimeout: 3 * time.Second,
					DialContext: (&net.Dialer{
						Timeout: 30 * time.Second,
					}).DialContext,
					TLSClientConfig: defaultTLSConfig,
				},
			})
			if err != nil {
				klog.Errorf("Failed to create HTTP client for Elasticsearch: %s/%s with: %s", o.db.Namespace, o.db.Name, err.Error())
				return nil, err
			}

			res, err := opensearchapi.PingRequest{}.Do(o.ctx, osClient.Transport)
			if err != nil {
				return nil, err
			}

			defer func(Body io.ReadCloser) {
				err = Body.Close()
				if err != nil {
					klog.Errorf("failed to close response body", err)
				}
			}(res.Body)

			if res.IsError() {
				return nil, fmt.Errorf("cluster ping request failed with status code: %d", res.StatusCode)
			}
			return &Client{
				&OSClientV1{client: osClient},
			}, nil
		}
	}

	return nil, fmt.Errorf("unknown database version: %s", o.db.Spec.Version)
}

func (o *KubeDBClientBuilder) getDefaultTLSConfig() (*tls.Config, error) {
	var crt tls.Certificate
	var clientCA, rootCA *x509.CertPool

	if o.db.Spec.EnableSSL {
		certSecret, err := o.kc.CoreV1().Secrets(o.db.Namespace).Get(o.ctx, o.db.GetCertSecretName(api.ElasticsearchClientCert), v1.GetOptions{})
		if err != nil {
			klog.Errorf("Failed to get client-cert for tls configurations")
			return nil, err
		}

		crt, err = tls.X509KeyPair(certSecret.Data[core.TLSCertKey], certSecret.Data[core.TLSPrivateKeyKey])
		if err != nil {
			klog.Errorf("failed to create certificate for TLS config", err)
			return nil, err
		}

		// get tls cert, clientCA and rootCA for tls config
		// use server cert ca for rootca as issuer ref is not taken into account
		clientCA = x509.NewCertPool()
		rootCA = x509.NewCertPool()

		clientCA.AppendCertsFromPEM(certSecret.Data[v1alpha1.CaCertKey])
		rootCA.AppendCertsFromPEM(certSecret.Data[v1alpha1.CaCertKey])
	}

	defaultTLSConfig := &tls.Config{
		Certificates: []tls.Certificate{crt},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCA,
		RootCAs:      rootCA,
		MaxVersion:   tls.VersionTLS13,
	}

	return defaultTLSConfig, nil
}

func (o *KubeDBClientBuilder) ServiceURL() string {
	return fmt.Sprintf("%v://%s.%s.svc:%d", o.db.GetConnectionScheme(), o.db.ServiceName(), o.db.GetNamespace(), api.ElasticsearchRestPort)
}
