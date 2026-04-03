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

package milvus

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc  client.Client
	db  *api.Milvus
	ctx context.Context
}

func NewKubeDBClientBuilder(kc client.Client, db *api.Milvus) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc: kc,
		db: db,
	}
}

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}

func (o *KubeDBClientBuilder) GetMilvusClient() (*milvusclient.Client, error) {
	if o.ctx == nil {
		o.ctx = context.Background()
	}

	addr := o.db.ServiceDNS()

	config := &milvusclient.ClientConfig{
		Address: addr,
	}

	// TLS configuration
	var grpcOpts []grpc.DialOption
	if o.db.Spec.TLS != nil && o.db.Spec.TLS.External != nil && o.db.Spec.TLS.External.Mode != api.MilvusTLSModeDisabled {
		secretName := o.db.GetCertSecretName(api.MilvusCertificateTypeServer)
		var secret core.Secret
		if err := o.kc.Get(o.ctx, types.NamespacedName{
			Namespace: o.db.Namespace,
			Name:      secretName,
		}, &secret); err != nil {
			return nil, fmt.Errorf("failed to get TLS secret %q: %w", secretName, err)
		}
		caCert, ok := secret.Data["ca.crt"]
		if !ok {
			return nil, fmt.Errorf("ca.crt not found in TLS secret %q", secretName)
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to append CA cert from secret %q", secretName)
		}

		// For mTLS, also load client certificate
		var tlsConfig *tls.Config
		if o.db.Spec.TLS.External.Mode == api.MilvusTLSModeMTLS {
			clientCert, ok := secret.Data["tls.crt"]
			if !ok {
				return nil, fmt.Errorf("tls.crt not found in TLS secret %q", secretName)
			}
			clientKey, ok := secret.Data["tls.key"]
			if !ok {
				return nil, fmt.Errorf("tls.key not found in TLS secret %q", secretName)
			}
			cert, err := tls.X509KeyPair(clientCert, clientKey)
			if err != nil {
				return nil, fmt.Errorf("failed to parse client certificate: %w", err)
			}
			tlsConfig = &tls.Config{
				RootCAs:      caPool,
				Certificates: []tls.Certificate{cert},
				ServerName:   o.db.ServiceName(),
			}
		} else {
			// One-way TLS
			tlsConfig = &tls.Config{
				RootCAs:    caPool,
				ServerName: o.db.ServiceName(),
			}
		}
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
		config.EnableTLSAuth = true
	} else {
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	if !o.db.Spec.DisableSecurity {
		if o.db.Spec.AuthSecret == nil {
			return nil, fmt.Errorf("auth secret is not specified")
		}

		secretName := o.db.GetAuthSecretName()
		var secret core.Secret
		if err := o.kc.Get(o.ctx, client.ObjectKey{
			Namespace: o.db.Namespace,
			Name:      secretName,
		}, &secret); err != nil {
			return nil, fmt.Errorf("failed to get auth secret %q: %w", secretName, err)
		}

		user, ok := secret.Data[core.BasicAuthUsernameKey]
		if !ok {
			return nil, fmt.Errorf("username is missing in secret %q", secretName)
		}

		pass, ok := secret.Data[core.BasicAuthPasswordKey]
		if !ok {
			return nil, fmt.Errorf("password is missing in secret %q", secretName)
		}

		config.Username = string(user)
		config.Password = string(pass)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(config.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		klog.Warningf("gRPC dial failed: %v", err)
		return nil, err
	}
	defer conn.Close() // nolint:errcheck
	conn.Close()       // nolint:errcheck

	c, err := milvusclient.New(ctx, config)
	if err != nil {
		klog.Warningf("Failed to create Milvus client: %v", err)
		return nil, err
	}

	return c, nil
}
