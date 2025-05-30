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

package solr

import (
	"context"
	"io"
	"net/http"

	"github.com/go-logr/logr"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Client struct {
	SLClient
}

type ClientOptions struct {
	KBClient client.Client
	DB       *api.Solr
	Ctx      context.Context
	Log      logr.Logger
}

type Config struct {
	host             string
	connectionScheme string
	transport        *http.Transport
	log              logr.Logger
}

type Response struct {
	Code   int
	header http.Header
	body   io.ReadCloser
}

type Doc struct {
	Id int    `json:"id,omitempty" yaml:"id,omitempty"`
	DB string `json:"db,omitempty" yaml:"db,omitempty"`
}

type Data struct {
	CommitWithin int  `json:"commitWithin,omitempty" yaml:"commitWithin,omitempty"`
	Overwrite    bool `json:"overwrite,omitempty" yaml:"overwrite,omitempty"`
	Doc          *Doc `json:"doc,omitempty" yaml:"doc,omitempty"`
}

type ADD struct {
	Add *Data `json:"add,omitempty" yaml:"add,omitempty"`
}

type QueryParams struct {
	Query string `json:"query,omitempty" yaml:"query,omitempty"`
	Limit int    `json:"limit,omitempty" yaml:"limit,omitempty"`
}

type CreateParams struct {
	Name              string `json:"name,omitempty" yaml:"name,omitempty"`
	Config            string `json:"config,omitempty" yaml:"config,omitempty"`
	NumShards         int    `json:"numShards,omitempty" yaml:"numShards,omitempty"`
	ReplicationFactor int    `json:"replicationFactor,omitempty" yaml:"replicationFactor,omitempty"`
}

type MoveReplicaInfo struct {
	Replica    string `json:"replica,omitempty" yaml:"replica,omitempty"`
	TargetNode string `json:"targetNode,omitempty" yaml:"targetNode,omitempty"`
	Async      string `json:"async,omitempty" yaml:"async,omitempty"`
}

type MoveReplicaParams struct {
	MoveReplica MoveReplicaInfo `json:"move-replica,omitempty" yaml:"move-replica,omitempty"`
}

type BalanceReplica struct {
	WaitForFinalState bool   `json:"waitForFinalState,omitempty" yaml:"waitForFinalState,omitempty"`
	Async             string `json:"async,omitempty" yaml:"async,omitempty"`
}

type CoreList struct {
	coreName   string
	collection string
}

type UpdateList struct {
	target     string
	replica    string
	collection string
}

type MetricsInfo struct {
	Metrics Metrics `json:"metrics,omitempty" yaml:"metrics,omitempty"`
}

type Metrics struct {
	Jetty Jetty `json:"jetty,omitempty" yaml:"jetty,omitempty"`
	JVM   JVM   `json:"jvm,omitempty" yaml:"jvm,omitempty"`
}

type Jetty struct {
	Jobs        float32
	Size        float32
	Utilization float32
}

type JVM struct {
	BuffersDirectCount         float32 `json:"buffers.direct.count,omitempty" yaml:"buffers.direct.count,omitempty"`
	BuffersDirectMemoryUsed    float32 `json:"buffers.direct.MemoryUsed,omitempty" yaml:"buffers.direct.MemoryUsed,omitempty"`
	BuffersDirectTotalCapacity float32 `json:"buffers.direct.TotalCapacity,omitempty" yaml:"buffers.direct.TotalCapacity,omitempty"`

	BuffersMappedCount         float32 `json:"buffers.mapped.count,omitempty" yaml:"buffers.mapped.count,omitempty"`
	BuffersMappedMemoryUsed    float32 `json:"buffers.mapped.MemoryUsed,omitempty" yaml:"buffers.mapped.MemoryUsed,omitempty"`
	BuffersMappedTotalCapacity float32 `json:"buffers.mapped.TotalCapacity,omitempty" yaml:"buffers.mapped.TotalCapacity,omitempty"`

	MemoryHeapMax   float32
	MemoryHeapUsed  float32
	MemoryHeapUsage float32

	ThreadsCount         float32
	ThreadsPeakCount     float32
	ThreadsRunnableCount float32
}
