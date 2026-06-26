/*
Copyright AppsCode Inc. and Contributors

Licensed under the AppsCode Community License 1.0.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://github.com/appscode/licenses/raw/1.0.0/AppsCode-Community-1.0.0.md

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package aerospike

import (
	"context"
	"errors"
	"fmt"
	"time"

	"kubedb.dev/apimachinery/apis/kubedb"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	as "github.com/aerospike/aerospike-client-go/v8"
	core "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	DefaultTimeout = 15 * time.Second
	// AerospikeTestNamespace is the default namespace used for write health checks.
	AerospikeTestNamespace = "test"
)

type KubeDBClientBuilder struct {
	kc      client.Client
	db      *api.Aerospike
	podName string
	url     string
}

func NewKubeDBClientBuilder(kc client.Client, db *api.Aerospike) *KubeDBClientBuilder {
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

func (o *KubeDBClientBuilder) GetAerospikeClient(ctx context.Context) (*Client, error) {
	if o.podName != "" {
		o.url = o.getPodURL()
	}
	if o.url == "" {
		o.url = o.getServiceURL()
	}

	policy := as.NewClientPolicy()
	policy.Timeout = DefaultTimeout
	policy.FailIfNotConnected = true

	if !o.db.Spec.DisableAuth {
		user, password, err := o.getClientCredentials(ctx)
		if err != nil {
			return nil, err
		}
		policy.User = user
		policy.Password = password
	}

	host := as.NewHost(o.url, kubedb.AerospikeDatabasePort)
	asClient, err := as.NewClientWithPolicyAndHost(policy, host)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}
	return &Client{
		Client: asClient,
	}, nil
}

func (o *KubeDBClientBuilder) getClientCredentials(ctx context.Context) (string, string, error) {
	if o.db.Spec.AuthSecret == nil || o.db.Spec.AuthSecret.Name == "" {
		return "", "", errors.New("no database secret")
	}

	var secret core.Secret
	err := o.kc.Get(ctx, client.ObjectKey{Namespace: o.db.Namespace, Name: o.db.Spec.AuthSecret.Name}, &secret)
	if err != nil {
		return "", "", err
	}

	user := string(secret.Data[core.BasicAuthUsernameKey])
	password := string(secret.Data[core.BasicAuthPasswordKey])
	if user == "" {
		klog.Warningf("username not found in auth secret %s/%s, connecting without auth", o.db.Namespace, o.db.Spec.AuthSecret.Name)
	}
	return user, password, nil
}

func (o *KubeDBClientBuilder) getServiceURL() string {
	return fmt.Sprintf("%s.%s.svc", o.db.GoverningServiceName(), o.db.Namespace)
}

func (o *KubeDBClientBuilder) getPodURL() string {
	return fmt.Sprintf("%s.%s.%s.svc", o.podName, o.db.GoverningServiceName(), o.db.Namespace)
}
