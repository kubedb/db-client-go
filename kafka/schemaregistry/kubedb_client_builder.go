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

package schemaregistry

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/go-resty/resty/v2"
	kapi "kubedb.dev/apimachinery/apis/kafka/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc       client.Client
	dbSchema *kapi.SchemaRegistry
	url      string
	path     string
	podName  string
	ctx      context.Context
}

func NewKubeDBClientBuilder(kc client.Client, dbSchema *kapi.SchemaRegistry) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc:       kc,
		dbSchema: dbSchema,
		url:      GetConnectionURL(dbSchema),
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

func (o *KubeDBClientBuilder) WithPath(path string) *KubeDBClientBuilder {
	o.path = path
	return o
}

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}

func (o *KubeDBClientBuilder) GetSchemaRegistryClient() (*Client, error) {
	config := Config{
		host: o.url,
		api:  o.path,
		transport: &http.Transport{
			IdleConnTimeout: time.Second * 3,
			DialContext: (&net.Dialer{
				Timeout: time.Second * 30,
			}).DialContext,
		},
		connectionScheme: o.dbSchema.GetConnectionScheme(),
	}

	newClient := resty.New()
	newClient.SetTransport(config.transport).SetScheme(config.connectionScheme).SetBaseURL(config.host)
	newClient.SetHeader("Accept", "application/json")
	newClient.SetTimeout(time.Second * 30)
	newClient.SetDisableWarn(true)

	return &Client{
		Client: newClient,
		Config: &config,
	}, nil
}

func GetConnectionURL(registry *kapi.SchemaRegistry) string {
	return fmt.Sprintf("%s://%s.%s.svc:%d", registry.GetConnectionScheme(), registry.ServiceName(), registry.Namespace, kapi.ApicurioRegistryRESTPort)

}
