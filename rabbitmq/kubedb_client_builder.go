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

package rabbitmq

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc      client.Client
	db      *api.RabbitMQ
	url     string
	podName string
	ctx     context.Context
}

func NewKubeDBClientBuilder(kc client.Client, db *api.RabbitMQ) *KubeDBClientBuilder {
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

func (o *KubeDBClientBuilder) GetAMQPconnURL(username string, password string) string {
	return fmt.Sprintf("amqp://%s:%s@%s.%s.svc.cluster.local:%d/", username, password, o.db.OffshootName(), o.db.Namespace, api.RabbitMQAMQPPort)
}

func (o *KubeDBClientBuilder) GetRabbitMQClient() (*Client, error) {
	authSecret := &core.Secret{}
	if !o.db.Spec.DisableSecurity {
		if o.db.Spec.AuthSecret == nil {
			klog.Info("Auth-secret not set")
			return nil, errors.New("auth-secret is not set")
		}
		err := o.kc.Get(o.ctx, types.NamespacedName{
			Namespace: o.db.Namespace,
			Name:      o.db.Spec.AuthSecret.Name,
		}, authSecret)
		if err != nil {
			if kerr.IsNotFound(err) {
				klog.Error(err, "Auth-secret not found")
				return nil, errors.New("auth-secret is not found")
			}
			klog.Error(err, "Failed to get auth-secret")
			return nil, err
		}
	}
	dbConnURL := o.GetAMQPconnURL(string(authSecret.Data[core.BasicAuthUsernameKey]), string(authSecret.Data[core.BasicAuthPasswordKey]))

	rabbitConnection, err := amqp.DialConfig(dbConnURL, amqp.Config{
		Vhost:  "/",
		Locale: "en_US",
	})
	if err != nil {
		klog.Error(err, "Failed to connect to rabbitmq")
		return nil, err
	}
	klog.Info("Successfully created client for db")

	return &Client{
		Connection: rabbitConnection,
	}, nil
}
