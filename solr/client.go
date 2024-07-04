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

type BackupParams struct {
	Location   string `json:"location,omitempty" yaml:"location,omitempty"`
	Repository string `json:"repository,omitempty" yaml:"repository,omitempty"`
	Async      string `json:"async,omitempty" yaml:"async,omitempty"`
}

type BackupRestoreParams struct {
	Location   string `json:"location,omitempty" yaml:"location,omitempty"`
	Repository string `json:"repository,omitempty" yaml:"repository,omitempty"`
	Collection string `json:"collection,omitempty" yaml:"collection,omitempty"`
	Async      string `json:"async,omitempty" yaml:"async,omitempty"`
	BackupId   int    `json:"backupId,omitempty" yaml:"backupId,omitempty"`
}

type CreateParams struct {
	Name              string `json:"name,omitempty" yaml:"name,omitempty"`
	Config            string `json:"config,omitempty" yaml:"config,omitempty"`
	NumShards         int    `json:"numShards,omitempty" yaml:"numShards,omitempty"`
	ReplicationFactor int    `json:"replicationFactor,omitempty" yaml:"replicationFactor,omitempty"`
}
