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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	rmqhttp "github.com/michaelklishin/rabbit-hole/v2"
	amqp "github.com/rabbitmq/amqp091-go"
	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"kubedb.dev/apimachinery/apis/kubedb"
	dbapi "kubedb.dev/apimachinery/apis/kubedb/v1"
	olddbapi "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc                client.Client
	db                *olddbapi.RabbitMQ
	ctx               context.Context
	amqpURL           string
	httpURL           string
	podName           string
	vhost             string
	enableHTTPClient  bool
	disableAMQPClient bool
}

const (
	rabbitmqQueueTypeQuorum  = "quorum"
	rabbitmqQueueTypeClassic = "classic"
)

// NewKubeDBClientBuilder returns a client builder only for amqp client
func NewKubeDBClientBuilder(kc client.Client, db *olddbapi.RabbitMQ) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc: kc,
		db: db,
	}
}

// NewKubeDBClientBuilderForHTTP returns a KubeDB client builder only for http client
func NewKubeDBClientBuilderForHTTP(kc client.Client, db *olddbapi.RabbitMQ) *KubeDBClientBuilder {
	return NewKubeDBClientBuilder(kc, db).
		WithContext(context.TODO()).
		WithAMQPClientDisabled().
		WithHTTPClientEnabled()
}

func (o *KubeDBClientBuilder) WithPod(podName string) *KubeDBClientBuilder {
	o.podName = podName
	return o
}

func (o *KubeDBClientBuilder) WithAMQPURL(url string) *KubeDBClientBuilder {
	o.amqpURL = url
	return o
}

func (o *KubeDBClientBuilder) WithHTTPURL(url string) *KubeDBClientBuilder {
	o.httpURL = url
	return o
}

func (o *KubeDBClientBuilder) WithVHost(vhost string) *KubeDBClientBuilder {
	o.vhost = vhost
	return o
}

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}

func (o *KubeDBClientBuilder) WithHTTPClientEnabled() *KubeDBClientBuilder {
	o.enableHTTPClient = true
	return o
}

func (o *KubeDBClientBuilder) WithAMQPClientDisabled() *KubeDBClientBuilder {
	o.disableAMQPClient = true
	return o
}

func (o *KubeDBClientBuilder) GetRabbitMQClient() (*Client, error) {
	if o.ctx == nil {
		o.ctx = context.TODO()
	}
	authSecret := &core.Secret{}
	var username, password string
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
		username, password = string(authSecret.Data[core.BasicAuthUsernameKey]), string(authSecret.Data[core.BasicAuthPasswordKey])
	} else {
		username, password = "guest", "guest"
	}

	var tlsConfig *tls.Config
	if o.db.Spec.EnableSSL {
		certSecret := &core.Secret{}
		err := o.kc.Get(o.ctx, types.NamespacedName{
			Namespace: o.db.Namespace,
			Name:      o.db.GetCertSecretName(olddbapi.RabbitmqClientCert),
		}, certSecret)
		if err != nil {
			if kerr.IsNotFound(err) {
				klog.Error(err, "Client certificate secret not found")
				return nil, errors.New("client certificate secret is not found")
			}
			klog.Error(err, "Failed to get client certificate Secret")
			return nil, err
		}

		// get tls cert, clientCA and rootCA for tls config
		clientCA := x509.NewCertPool()
		rootCA := x509.NewCertPool()

		crt, err := tls.X509KeyPair(certSecret.Data[core.TLSCertKey], certSecret.Data[core.TLSPrivateKeyKey])
		if err != nil {
			klog.Error(err, "Failed to parse private key pair")
			return nil, err
		}
		clientCA.AppendCertsFromPEM(certSecret.Data[dbapi.TLSCACertFileName])
		rootCA.AppendCertsFromPEM(certSecret.Data[dbapi.TLSCACertFileName])

		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{crt},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    clientCA,
			RootCAs:      rootCA,
			MaxVersion:   tls.VersionTLS13,
		}
	}

	rmqClient := &Client{}
	defaultVhost := "/"

	if o.enableHTTPClient {
		if o.httpURL == "" {
			o.httpURL = o.GetHTTPconnURL()
		}
		httpClient, err := func(isTLSEnabled bool) (*rmqhttp.Client, error) {
			if isTLSEnabled {
				return rmqhttp.NewTLSClient(o.httpURL, username, password, &http.Transport{
					IdleConnTimeout: time.Second * 3,
					DialContext: (&net.Dialer{
						Timeout: time.Second * 30,
					}).DialContext,
					TLSClientConfig:     tlsConfig,
					TLSHandshakeTimeout: time.Second * 30,
				})
			}
			return rmqhttp.NewClient(o.httpURL, username, password)
		}(o.db.Spec.EnableSSL)
		if err != nil {
			klog.Error(err, "Failed to get http client for rabbitmq")
			return nil, err
		}

		vhosts, err := httpClient.ListVhosts()
		if err != nil {
			klog.Error(err, "Failed to list virtual hosts")
			return nil, err
		}
		for _, vhost := range vhosts {
			if vhost.Description == "Default virtual host" {
				defaultVhost = vhost.Name
				break
			}
		}
		rmqClient.HTTPClient = HTTPClient{httpClient}
	}

	if !o.disableAMQPClient {
		if o.amqpURL == "" {
			if o.vhost == "" {
				o.vhost = defaultVhost
			}
			o.amqpURL = o.GetAMQPconnURL(username, password, o.vhost)
		}

		rabbitConnection, err := amqp.DialConfig(o.amqpURL, amqp.Config{
			Vhost:  o.vhost,
			Locale: "en_US",
		})
		if err != nil {
			klog.Error(err, "Failed to connect to rabbitmq")
			return nil, err
		}
		klog.Info("Successfully created AMQP client for RabbitMQ")
		rmqClient.AMQPClient = AMQPClient{rabbitConnection}
	}

	return rmqClient, nil
}

func (o *KubeDBClientBuilder) GetAMQPconnURL(username string, password string, vhost string) string {
	return fmt.Sprintf("amqp://%s:%s@%s.%s.svc.cluster.local:%d%s", username, password, o.db.ServiceName(), o.db.Namespace, kubedb.RabbitMQAMQPPort, vhost)
}

func (o *KubeDBClientBuilder) GetHTTPconnURL() string {
	protocolScheme := o.db.GetConnectionScheme()
	connectionPort := func(scheme string) int {
		if scheme == "http" {
			return kubedb.RabbitMQManagementUIPort
		} else {
			return kubedb.RabbitMQManagementUIPortWithSSL
		}
	}(protocolScheme)
	if o.podName != "" {
		return fmt.Sprintf("%s://%s.%s.%s.svc:%d", protocolScheme, o.podName, o.db.GoverningServiceName(), o.db.Namespace, connectionPort)
	}
	return fmt.Sprintf("%s://%s.%s.svc.cluster.local:%d", protocolScheme, o.db.DashboardServiceName(), o.db.Namespace, connectionPort)
}

// RabbitMQ server have a default virtual host "/"
// for custom vhost, it must be appended at the end of the url separated by "/"
func (o *KubeDBClientBuilder) GetVirtualHostFromURL(url string) (vhost string) {
	vhost = "/"
	lastIndex := strings.LastIndex(url, vhost)
	if lastIndex != -1 && lastIndex < len(url)-1 {
		return url[lastIndex+1:]
	}
	return vhost
}
