package db2

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	dbapi "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	"github.com/go-resty/resty/v2"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc      client.Client
	db2     *dbapi.DB2
	url     string
	path    string
	podName string
	ctx     context.Context
}

func NewKubeDBClientBuilder(kc client.Client, db2 *dbapi.DB2) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc:  kc,
		db2: db2,
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

func (o *KubeDBClientBuilder) GetDB2Client() (*Client, error) {
	config := Config{
		host: o.url,
		api:  o.path,
		transport: &http.Transport{
			IdleConnTimeout: time.Second * 3,
			DialContext: (&net.Dialer{
				Timeout: time.Second * 30,
			}).DialContext,
		},
		connectionScheme: "http",
	}

	var username, password string
	// if security is enabled set database credentials in clientConfig
	if o.db2.Spec.AuthSecret != nil && o.db2.Spec.AuthSecret.Name != "" {
		secret := &core.Secret{}
		err := o.kc.Get(o.ctx, types.NamespacedName{
			Name:      o.db2.Spec.AuthSecret.Name,
			Namespace: o.db2.GetNamespace(),
		}, secret)
		if err != nil {
			return nil, err
		}

		if value, ok := secret.Data[core.BasicAuthUsernameKey]; ok {
			username = string(value)
		} else {
			klog.Info(fmt.Sprintf("Failed for secret: %s/%s, username is missing", secret.Namespace, secret.Name))
			return nil, errors.New("username is missing")
		}

		if value, ok := secret.Data[core.BasicAuthPasswordKey]; ok {
			password = string(value)
		} else {
			klog.Info(fmt.Sprintf("Failed for secret: %s/%s, password is missing", secret.Namespace, secret.Name))
			return nil, errors.New("password is missing")
		}

		config.username = username
		config.password = password
	}

	newClient := resty.New()
	newClient.SetTransport(config.transport).SetScheme(config.connectionScheme).SetBaseURL(config.host)
	newClient.SetHeader("Accept", "application/json")
	newClient.SetBasicAuth(config.username, config.password)
	newClient.SetTimeout(time.Second * 30)
	newClient.SetDisableWarn(true)

	return &Client{
		Client: newClient,
		Config: &config,
	}, nil
}
