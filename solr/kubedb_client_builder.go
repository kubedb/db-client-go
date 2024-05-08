package solr

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-resty/resty/v2"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
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

func (o *KubeDBClientBuilder) GetSolrClient() (SLClient, error) {
	config := Config{
		host: getHostPath(o.db),
		transport: &http.Transport{
			IdleConnTimeout: time.Second * 20,
			DialContext: (&net.Dialer{
				Timeout: time.Second * 30,
			}).DialContext,
		},
		connectionScheme: o.db.GetConnectionScheme(),
		log:              o.log,
	}

	newClient := resty.New()
	newClient.SetScheme(config.connectionScheme).SetBaseURL(config.host).SetTransport(config.transport)
	newClient.SetHeader("Accept", "application/json")
	newClient.SetTimeout(time.Second * 30)
	newClient.SetDisableWarn(true)

	if !o.db.Spec.DisableSecurity {
		var authSecret core.Secret
		err := o.kc.Get(o.ctx, types.NamespacedName{
			Name:      o.db.Spec.AuthSecret.Name,
			Namespace: o.db.Namespace,
		}, &authSecret)
		if err != nil {
			config.log.Error(err, "failed to get auth secret to get solr client")
			return SLClient{}, err
		}
		newClient.SetBasicAuth(string(authSecret.Data[core.BasicAuthUsernameKey]), string(authSecret.Data[core.BasicAuthPasswordKey]))
	}

	return SLClient{
		Client: newClient,
		log:    config.log,
		Config: &config,
	}, nil

}

func getHostPath(db *api.Solr) string {
	return fmt.Sprintf("%v://%s.%s.svc.cluster.local:%d", db.GetConnectionScheme(), db.ServiceName(), db.GetNamespace(), api.SolrRestPort)
}
