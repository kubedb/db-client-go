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

package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"strings"

	kafkago "github.com/IBM/sarama"
	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	dbapi "kubedb.dev/apimachinery/apis/kubedb/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc      client.Client
	db      *dbapi.Kafka
	url     string
	podName string
	ctx     context.Context
}

func NewKubeDBClientBuilder(kc client.Client, db *dbapi.Kafka) *KubeDBClientBuilder {
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

func (o *KubeDBClientBuilder) GetConfig() (*kafkago.Config, error) {
	clientConfig := kafkago.NewConfig()
	sversion, err := kafkago.ParseKafkaVersion(o.db.Spec.Version)
	if err != nil {
		klog.Error(err, "Failed to parse Kafka version", "version", o.db.Spec.Version)
		return nil, errors.New("failed to parse Kafka version: " + o.db.Spec.Version)
	}
	clientConfig.Version = sversion
	if !o.db.Spec.DisableSecurity {
		if o.db.Spec.AuthSecret == nil {
			klog.Info("Auth-secret not set")
			return nil, errors.New("auth-secret is not set")
		}

		authSecret := &core.Secret{}
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

		clientConfig.Net.SASL.Enable = true
		clientConfig.Net.SASL.User = string(authSecret.Data[core.BasicAuthUsernameKey])
		clientConfig.Net.SASL.Password = string(authSecret.Data[core.BasicAuthPasswordKey])

		if o.db.Spec.EnableSSL {
			certSecret := &core.Secret{}
			err := o.kc.Get(o.ctx, types.NamespacedName{
				Namespace: o.db.Namespace,
				Name:      o.db.GetCertSecretName(dbapi.KafkaClientCert),
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
			clientConfig.Net.TLS.Enable = true
			clientConfig.Net.TLS.Config = &tls.Config{
				Certificates: []tls.Certificate{crt},
				ClientAuth:   tls.RequireAndVerifyClientCert,
				ClientCAs:    clientCA,
				RootCAs:      rootCA,
				MaxVersion:   tls.VersionTLS13,
			}
		}

	}

	clientConfig.Producer.Return.Successes = true
	clientConfig.Producer.Retry.Max = 10
	clientConfig.Producer.Timeout = 1000
	clientConfig.Consumer.Offsets.Retry.Max = 10
	clientConfig.Consumer.Return.Errors = true

	return clientConfig, nil
}

func (o *KubeDBClientBuilder) GetKafkaClient() (*Client, error) {
	clientConfig, err := o.GetConfig()
	if err != nil {
		return nil, err
	}
	kafkaClient, err := kafkago.NewClient(
		strings.Split(o.url, ","),
		clientConfig,
	)
	if err != nil {
		return nil, err
	}

	return &Client{
		Client: kafkaClient,
	}, nil
}

func (o *KubeDBClientBuilder) GetKafkaProducerClient() (*ProducerClient, error) {
	clientConfig, err := o.GetConfig()
	if err != nil {
		return nil, err
	}
	kafkaProducerClient, err := kafkago.NewSyncProducer(
		strings.Split(o.url, ","),
		clientConfig,
	)
	if err != nil {
		return nil, err
	}

	return &ProducerClient{
		kafkaProducerClient,
	}, nil
}

func (o *KubeDBClientBuilder) GetKafkaAdminClient() (*AdminClient, error) {
	clientConfig, err := o.GetConfig()
	if err != nil {
		return nil, err
	}
	kafkaAdminClient, err := kafkago.NewClusterAdmin(
		strings.Split(o.url, ","),
		clientConfig,
	)
	if err != nil {
		return nil, err
	}

	return &AdminClient{
		kafkaAdminClient,
	}, nil
}

func (o *KubeDBClientBuilder) GetKafkaConsumerClient() (*ConsumerClient, error) {
	clientConfig, err := o.GetConfig()
	if err != nil {
		return nil, err
	}
	kafkaConsumerClient, err := kafkago.NewConsumer(
		strings.Split(o.url, ","),
		clientConfig,
	)
	if err != nil {
		return nil, err
	}

	return &ConsumerClient{
		kafkaConsumerClient,
	}, nil
}
