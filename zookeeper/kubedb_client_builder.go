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

package zookeeper

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"github.com/Shopify/zk"
	"kubedb.dev/apimachinery/apis/kubedb"
	dbapi "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	defaultZooKeeperTimeout = time.Second * 3
)

type KubeDBClientBuilder struct {
	kc                client.Client
	db                *dbapi.ZooKeeper
	ctx               context.Context
	podName           string
	url               string
	enableHTTPClient  bool
	disableAMQPClient bool
}

func NewKubeDBClientBuilder(kc client.Client, db *dbapi.ZooKeeper) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc: kc,
		db: db,
	}
}

// NewKubeDBClientBuilderForHTTP returns a KubeDB client builder only for http client
func NewKubeDBClientBuilderForHTTP(kc client.Client, db *dbapi.ZooKeeper) *KubeDBClientBuilder {
	return NewKubeDBClientBuilder(kc, db).
		WithContext(context.TODO()).
		WithAMQPClientDisabled().
		WithHTTPClientEnabled()
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

func (o *KubeDBClientBuilder) WithHTTPClientEnabled() *KubeDBClientBuilder {
	o.enableHTTPClient = true
	return o
}

func (o *KubeDBClientBuilder) WithAMQPClientDisabled() *KubeDBClientBuilder {
	o.disableAMQPClient = true
	return o
}

func (o *KubeDBClientBuilder) GetZooKeeperClient() (*Client, error) {
	var err error
	if o.podName != "" {
		o.url = o.getPodURL()
	}
	if o.url == "" {
		o.url = o.db.Address()
	}
	zkConn, session, err := zk.Connect([]string{o.url}, defaultZooKeeperTimeout, zk.WithLogInfo(false))
	if err != nil {
		return nil, err
	}
	for event := range session {
		if event.State == zk.StateConnected {
			break
		}
	}

	if !o.db.Spec.DisableAuth {
		if o.db.Spec.AuthSecret == nil {
			klog.Info("Auth-secret not set")
			return nil, errors.New("auth-secret is not set")
		}

		authSecret := core.Secret{}
		err := o.kc.Get(o.ctx, types.NamespacedName{
			Namespace: o.db.Namespace,
			Name:      o.db.Spec.AuthSecret.Name,
		}, &authSecret)
		if err != nil {
			if kerr.IsNotFound(err) {
				klog.Error(err, "Auth-secret not found")
				return nil, errors.New("auth-secret is not found")
			}
			klog.Error(err, "Failed to get auth-secret")
			return nil, err
		}

		//clientConfig.Net.SASL.Enable = true
		username := string(authSecret.Data[core.BasicAuthUsernameKey])
		password := string(authSecret.Data[core.BasicAuthPasswordKey])

		// Correct the format for the username:password string
		authString := fmt.Sprintf("%s:%s", username, password)

		// Add authentication using the properly formatted authString
		err = zkConn.AddAuth("digest", []byte(authString))
		if err != nil {
			log.Fatalf("Failed to add authentication: %v", err)
		}
	}

	return &Client{
		zkConn,
	}, nil
}

func (o *KubeDBClientBuilder) getPodURL() string {
	return fmt.Sprintf("%v.%v.%v.svc:%d", o.podName, o.db.GoverningServiceName(), o.db.Namespace, kubedb.ZooKeeperClientPort)
}
