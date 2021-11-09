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
	"fmt"
	"net"
	"net/http"
	"time"

	"kubedb.dev/apimachinery/apis/catalog/v1alpha1"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	"github.com/Masterminds/semver/v3"
	esv6 "github.com/elastic/go-elasticsearch/v6"
	esv7 "github.com/elastic/go-elasticsearch/v7"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc      client.Client
	db      *api.Elasticsearch
	url     string
	podName string
}

func NewKubeDBClientBuilder(kc client.Client, db *api.Elasticsearch) *KubeDBClientBuilder {
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

func (o *KubeDBClientBuilder) GetElasticClient() (*Client, error) {
	if o.podName != "" {
		o.url = o.getURL()
	}
	var username, password string
	if !o.db.Spec.DisableSecurity && o.db.Spec.AuthSecret != nil {
		var secret core.Secret
		err := o.kc.Get(context.TODO(), client.ObjectKey{Namespace: o.db.Namespace, Name: o.db.Spec.AuthSecret.Name}, &secret)
		if err != nil {
			klog.Errorf("Failed to get secret: %s for Elasticsearch: %s/%s with: %s", o.db.Spec.AuthSecret.Name, o.db.Namespace, o.db.Name, err.Error())
			return nil, errors.Wrap(err, "failed to get the secret")
		}

		if value, ok := secret.Data[core.BasicAuthUsernameKey]; ok {
			username = string(value)
		} else {
			klog.Errorf("Failed for secret: %s/%s, username is missing", secret.Namespace, secret.Name)
			return nil, errors.New("username is missing")
		}

		if value, ok := secret.Data[core.BasicAuthPasswordKey]; ok {
			password = string(value)
		} else {
			klog.Errorf("Failed for secret: %s/%s, password is missing", secret.Namespace, secret.Name)
			return nil, errors.New("password is missing")
		}
	}

	// get Elasticsearch version from Elasticsearch version objects
	var esVer v1alpha1.ElasticsearchVersion
	err := o.kc.Get(context.Background(), client.ObjectKey{Name: o.db.Spec.Version}, &esVer)
	if err != nil {
		return nil, fmt.Errorf("unable to get elasticsearch version: %v", err)
	}
	esVersion, err := semver.NewVersion(esVer.Spec.Version)
	if err != nil {
		return nil, err
	}

	switch {
	// for Elasticsearch 6.x.x
	case esVersion.Major() == 6:
		client, err := esv6.NewClient(esv6.Config{
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
		res, err := client.Cluster.Health(
			client.Cluster.Health.WithPretty(),
		)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()

		if res.IsError() {
			return nil, fmt.Errorf("health check failed with status code: %d", res.StatusCode)
		}
		return &Client{
			&ESClientV6{client: client},
		}, nil

	// for Elasticsearch 7.x.x
	case esVersion.Major() == 7:
		client, err := esv7.NewClient(esv7.Config{
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
		res, err := client.Cluster.Health(
			client.Cluster.Health.WithPretty(),
		)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()

		if res.IsError() {
			return nil, fmt.Errorf("health check failed with status code: %d", res.StatusCode)
		}
		return &Client{
			&ESClientV7{client: client},
		}, nil
	}

	return nil, fmt.Errorf("unknown database verseion: %s", o.db.Spec.Version)
}

func (o *KubeDBClientBuilder) getURL() string {
	return fmt.Sprintf("%v://%s.%s.%s.svc:%d", o.db.GetConnectionScheme(), o.podName, o.db.ServiceName(), o.db.GetNamespace(), api.ElasticsearchRestPort)
}
