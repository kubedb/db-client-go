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

package redis

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"time"

	"github.com/Masterminds/semver/v3"
	rd "github.com/go-redis/redis/v8"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
)

type KubeDBClientBuilder struct {
	kubeClient    kubernetes.Interface
	dynamicClient dynamic.Interface
	db            *api.Redis
	podName       string
	url           string
}

func NewKubeDBClientBuilder(db *api.Redis, kubeClient kubernetes.Interface, dClient dynamic.Interface) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kubeClient:    kubeClient,
		dynamicClient: dClient,
		db:            db,
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

func (o *KubeDBClientBuilder) GetRedisClient() (*Client, error) {
	if o.podName != "" {
		o.url = o.getURL()
	}
	if o.db.Spec.AuthSecret == nil {
		return nil, errors.New("no database secret")
	}
	gvr := schema.GroupVersionResource{
		Group:    "catalog.kubedb.com",
		Version:  "v1alpha1",
		Resource: "redisversions",
	}
	redisVersionObj, err := o.dynamicClient.Resource(gvr).Get(context.Background(), o.db.Spec.Version, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get RedisVersion object: %v", err)
	}
	redisVersion, found, err := unstructured.NestedFieldNoCopy(redisVersionObj.Object, "spec", "version")
	if err != nil || !found {
		return nil, fmt.Errorf("failed to get version field from RedisVersion object: %v", err)
	}

	curVersion, err := semver.NewVersion(redisVersion.(string))
	if err != nil {
		return nil, fmt.Errorf("can't get the version from RedisVersion spec")
	}

	rdOpts := &rd.Options{
		DialTimeout: 15 * time.Second,
		IdleTimeout: 3 * time.Second,
		PoolSize:    1,
		Addr:        o.url,
	}

	if curVersion.Major() > 4 {
		authSecret, err := o.kubeClient.CoreV1().Secrets(o.db.Namespace).Get(context.TODO(), o.db.Spec.AuthSecret.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		rdOpts.Password = string(authSecret.Data[core.BasicAuthPasswordKey])
	}

	if o.db.Spec.TLS != nil {
		sec, err := o.kubeClient.CoreV1().Secrets(o.db.Namespace).Get(context.TODO(), o.db.CertificateName(api.RedisClientCert), metav1.GetOptions{})
		if err != nil {
			klog.Error(err, "error in getting the secret")
			return nil, err
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(sec.Data["ca.crt"])
		cert, err := tls.X509KeyPair(sec.Data["tls.crt"], sec.Data["tls.key"])
		if err != nil {
			klog.Error(err, "error in making certificate")
			return nil, err
		}
		rdOpts.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{
				cert,
			},
			ClientCAs: pool,
			RootCAs:   pool,
		}
	}
	rdClient := rd.NewClient(rdOpts)
	_, err = rdClient.Ping(context.Background()).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}
	return &Client{
		rdClient,
	}, nil
}

func (o *KubeDBClientBuilder) getURL() string {
	return fmt.Sprintf("%v.%v", o.podName, o.db.Address())
}
