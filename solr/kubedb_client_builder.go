package solr

import (
	"context"
	"errors"
	"github.com/Masterminds/semver/v3"
	gerr "github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"net"
	"net/http"
	"time"

	"fmt"

	"github.com/go-logr/logr"
	"github.com/go-resty/resty/v2"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc      client.Client
	db      *api.Solr
	url     string
	podName string
	ctx     context.Context
	log     logr.Logger
}

func NewKubeDBClientBuilder(kc client.Client, db *api.Solr) *KubeDBClientBuilder {
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

func (o *KubeDBClientBuilder) WithLog(log logr.Logger) *KubeDBClientBuilder {
	o.log = log
	return o
}

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}

func (o *KubeDBClientBuilder) GetSolrClient() (*Client, error) {
	if o.podName != "" {
		o.url = o.GetHostPath(o.db)
	}
	if o.url == "" {
		o.url = o.GetHostPath(o.db)
	}
	if o.db == nil {
		return nil, errors.New("db is empty")
	}
	config := Config{
		host: o.url,
		transport: &http.Transport{
			IdleConnTimeout: time.Minute * 6,
			DialContext: (&net.Dialer{
				Timeout:   time.Minute * 6,
				KeepAlive: time.Minute * 6,
			}).DialContext,
			TLSHandshakeTimeout:   time.Minute * 6,
			ResponseHeaderTimeout: time.Minute * 6,
			ExpectContinueTimeout: time.Minute * 6,
		},
		connectionScheme: o.db.GetConnectionScheme(),
		log:              o.log,
	}

	var authSecret core.Secret
	if !o.db.Spec.DisableSecurity {
		err := o.kc.Get(o.ctx, types.NamespacedName{
			Name:      o.db.Spec.AuthSecret.Name,
			Namespace: o.db.Namespace,
		}, &authSecret)
		if err != nil {
			config.log.Error(err, "failed to get auth secret to get solr client")
			return nil, err
		}
	}
	version, err := semver.NewVersion(o.db.Spec.Version)
	if err != nil {
		return nil, gerr.Wrap(err, "failed to parse version")
	}

	switch {
	case version.Major() == 9:
		newClient := resty.New()
		newClient.SetScheme(config.connectionScheme).SetBaseURL(config.host).SetTransport(config.transport)
		newClient.SetTimeout(6 * time.Minute)
		newClient.SetHeader("Accept", "application/json")
		newClient.SetDisableWarn(true)
		newClient.SetBasicAuth(string(authSecret.Data[core.BasicAuthUsernameKey]), string(authSecret.Data[core.BasicAuthPasswordKey]))
		return &Client{
			&SLClientV9{
				Client: newClient,
				log:    config.log,
				Config: &config,
			},
		}, nil
	}

	return nil, fmt.Errorf("unknown version: %s", o.db.Spec.Version)

}

func (o *KubeDBClientBuilder) GetHostPath(db *api.Solr) string {
	return fmt.Sprintf("%v://%s.%s.svc.cluster.local:%d", db.GetConnectionScheme(), db.ServiceName(), db.GetNamespace(), api.SolrRestPort)
}
