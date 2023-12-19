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
	"fmt"
	kafkago "github.com/IBM/sarama"
	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc         client.Client
	db         *api.Kafka
	url        string
	podName    string
	postgresDB string
	ctx        context.Context
}

func NewKubeDBClientBuilder(kc client.Client, db *api.Kafka) *KubeDBClientBuilder {
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

func (o *KubeDBClientBuilder) GetKafkaClient() (*Client, error) {
	clientConfig := kafkago.NewConfig()
	if !o.db.Spec.DisableSecurity {
		if o.db.Spec.AuthSecret == nil {
			klog.Info("Authsecret not set")
			return nil, errors.New("auth-secret is not set")
		}

		authSecret := &core.Secret{}
		err := o.kc.Get(o.ctx, types.NamespacedName{
			Namespace: o.db.Namespace,
			Name:      o.db.Spec.AuthSecret.Name,
		}, authSecret)
		if err != nil {
			if kerr.IsNotFound(err) {
				klog.Error(err, "Authsecret not found")
				return nil, errors.New("auth-secret is not found")
			}
			klog.Error(err, "Failed to get authsecret")
			return nil, err
		}

		clientConfig.Net.SASL.Enable = true
		clientConfig.Net.SASL.User = string(authSecret.Data[core.BasicAuthUsernameKey])
		clientConfig.Net.SASL.Password = string(authSecret.Data[core.BasicAuthPasswordKey])

		if o.db.Spec.EnableSSL {
			certSecret := &core.Secret{}
			err := o.kc.Get(o.ctx, types.NamespacedName{
				Namespace: o.db.Namespace,
				Name:      o.db.GetCertSecretName(api.KafkaClientCert),
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
			clientCA.AppendCertsFromPEM(certSecret.Data[api.TLSCACertFileName])
			rootCA.AppendCertsFromPEM(certSecret.Data[api.TLSCACertFileName])
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

	brokers := o.getBrokerAddresses()

	clientConfig.Producer.Return.Successes = true
	clientConfig.Producer.Retry.Max = 10
	clientConfig.Producer.Timeout = 1000
	clientConfig.Consumer.Offsets.Retry.Max = 10
	clientConfig.Consumer.Return.Errors = true

	kafkaClient, err := kafkago.NewClient(
		brokers,
		clientConfig,
	)
	if err != nil {
		return nil, err
	}

	return &Client{
		Client: kafkaClient,
		config: clientConfig,
	}, nil
}

// Returns all the DNS for brokers
func (o *KubeDBClientBuilder) getBrokerAddresses() []string {
	var brokers []string
	if o.db.Spec.Topology != nil {
		if o.db.Spec.Topology.Broker != nil {
			for i := int32(0); i < *o.db.Spec.Topology.Broker.Replicas; i++ {
				brokers = append(brokers,
					fmt.Sprintf("%s-%v.%s.%s.svc.cluster.local:%v", o.db.BrokerStatefulSetName(), i, o.db.GoverningServiceName(), o.db.Namespace, api.KafkaRESTPort))
			}
		}
	} else {
		for i := int32(0); i < *o.db.Spec.Replicas; i++ {
			brokers = append(brokers,
				fmt.Sprintf("%s-%v.%s.%s.svc.cluster.local:%v", o.db.CombinedStatefulSetName(), i, o.db.GoverningServiceName(), o.db.Namespace, api.KafkaRESTPort))
		}
	}
	return brokers
}
