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

package elasticsearchdashboard

import (
	"context"
	"io"
	"net/http"

	catalog "kubedb.dev/apimachinery/apis/catalog/v1alpha1"
	dapi "kubedb.dev/apimachinery/apis/dashboard/v1alpha1"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	core "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Client struct {
	EDClient
}

type ClientOptions struct {
	KClient   client.Client
	Dashboard *dapi.ElasticsearchDashboard
	ESVersion *catalog.ElasticsearchVersion
	DB        *api.Elasticsearch
	Ctx       context.Context
	Secret    *core.Secret
}

type Config struct {
	host             string
	api              string
	username         string
	password         string
	connectionScheme string
	transport        *http.Transport
}

type Health struct {
	ConnectionResponse Response
	OverallState       string
	StateFailedReason  map[string]string
}

type Response struct {
	Code   int
	header http.Header
	body   io.ReadCloser
}

type ResponseBody struct {
	Name    string                 `json:"name"`
	UUID    string                 `json:"uuid"`
	Version map[string]interface{} `json:"version"`
	Status  map[string]interface{} `json:"status"`
	Metrics map[string]interface{} `json:"metrics"`
}
