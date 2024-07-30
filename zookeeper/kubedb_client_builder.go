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
	"fmt"
	"time"

	"github.com/Shopify/zk"
	"kubedb.dev/apimachinery/apis/kubedb"
	dbapi "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	defaultZooKeeperTimeout = time.Second * 3
)

type KubeDBClientBuilder struct {
	kc      client.Client
	db      *dbapi.ZooKeeper
	podName string
	url     string
}

func NewKubeDBClientBuilder(kc client.Client, db *dbapi.ZooKeeper) *KubeDBClientBuilder {
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

func (o *KubeDBClientBuilder) GetZooKeeperClient(ctx context.Context) (*Client, error) {
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
	return &Client{
		zkConn,
	}, nil
}

func (o *KubeDBClientBuilder) getPodURL() string {
	return fmt.Sprintf("%v.%v.%v.svc:%d", o.podName, o.db.GoverningServiceName(), o.db.Namespace, kubedb.ZooKeeperClientPort)
}
