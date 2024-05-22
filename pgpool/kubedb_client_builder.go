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

package pgpool

import (
	"context"

	"fmt"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	_ "github.com/lib/pq"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"kmodules.xyz/client-go/tools/certholder"
	appbinding "kmodules.xyz/custom-resources/apis/appcatalog/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"xorm.io/xorm"
)

const (
	DefaultBackendDBName = "postgres"
	DefaultPgpoolPort    = 9999
)

type KubeDBClientBuilder struct {
	kc            client.Client
	pgpool        *api.Pgpool
	url           string
	podName       string
	backendDBName string
	ctx           context.Context
}

func NewKubeDBClientBuilder(kc client.Client, pp *api.Pgpool) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc:     kc,
		pgpool: pp,
	}
}

func (o *KubeDBClientBuilder) WithURL(url string) *KubeDBClientBuilder {
	o.url = url
	return o
}

func (o *KubeDBClientBuilder) WithPod(podName string) *KubeDBClientBuilder {
	o.podName = podName
	return o
}

func (o *KubeDBClientBuilder) WithPgpoolDB(pgDB string) *KubeDBClientBuilder {
	o.backendDBName = pgDB
	return o
}

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}

func (o *KubeDBClientBuilder) GetPgpoolXormClient() (*XormClient, error) {
	if o.ctx == nil {
		o.ctx = context.Background()
	}

	connector, err := o.getConnectionString()
	if err != nil {
		return nil, err
	}

	engine, err := xorm.NewEngine("postgres", connector)
	if err != nil {
		return nil, err
	}
	_, err = engine.Query("SELECT 1")
	if err != nil {
		err = engine.Close()
		if err != nil {
			return nil, err
		}
		return nil, err
	}

	engine.SetDefaultContext(o.ctx)
	return &XormClient{
		engine,
	}, nil
}

func (o *KubeDBClientBuilder) getURL() string {
	return fmt.Sprintf("%s.%s.%s.svc", o.podName, o.pgpool.GoverningServiceName(), o.pgpool.Namespace)
}

func (o *KubeDBClientBuilder) getBackendAuth() (string, string, error) {
	pp := o.pgpool
	if pp.Spec.PostgresRef == nil {
		return "", "", fmt.Errorf("there is no postgresRef found for pgpool %s/%s", pp.Namespace, pp.Name)
	}
	apb := &appbinding.AppBinding{}
	err := o.kc.Get(o.ctx, types.NamespacedName{
		Name:      pp.Spec.PostgresRef.Name,
		Namespace: pp.Spec.PostgresRef.Namespace,
	}, apb)
	if err != nil {
		return "", "", err
	}
	if apb.Spec.Secret == nil {
		return "", "", fmt.Errorf("backend postgres auth secret unspecified for pgpool %s/%s", pp.Namespace, pp.Name)
	}

	var secret core.Secret
	err = o.kc.Get(o.ctx, client.ObjectKey{Namespace: pp.Spec.PostgresRef.Namespace, Name: apb.Spec.Secret.Name}, &secret)
	if err != nil {
		return "", "", err
	}
	user, ok := secret.Data[core.BasicAuthUsernameKey]
	if !ok {
		return "", "", fmt.Errorf("error getting backend username")
	}
	pass, ok := secret.Data[core.BasicAuthPasswordKey]
	if !ok {
		return "", "", fmt.Errorf("error getting backend password")
	}
	return string(user), string(pass), nil
}

func (o *KubeDBClientBuilder) getConnectionString() (string, error) {
	if o.podName != "" {
		o.url = o.getURL()
	}
	dnsName := o.url

	if o.backendDBName == "" {
		o.backendDBName = DefaultBackendDBName
	}

	user, pass, err := o.getBackendAuth()
	if err != nil {
		return "", fmt.Errorf("DB basic auth is not found for backend PostgreSQL %v/%v", o.pgpool.Namespace, o.pgpool.Name)
	}
	cnnstr := ""
	sslMode := o.pgpool.Spec.SSLMode

	//  sslMode == "prefer" and sslMode == "allow"  don't have support for github.com/lib/pq postgres client. as we are using
	// github.com/lib/pq postgres client utils for connecting our server we need to access with  any of require , verify-ca, verify-full or disable.
	// here we have chosen "require" sslmode to connect postgres as a client
	if sslMode == api.PgpoolSSLModePrefer || sslMode == api.PgpoolSSLModeAllow {
		sslMode = api.PgpoolSSLModeRequire
	}
	if o.pgpool.Spec.TLS != nil {
		secretName := o.pgpool.GetCertSecretName(api.PgpoolClientCert)

		var certSecret core.Secret
		err := o.kc.Get(o.ctx, client.ObjectKey{Namespace: o.pgpool.Namespace, Name: secretName}, &certSecret)
		if err != nil {
			klog.Error(err, "failed to get certificate secret.", secretName)
			return "", err
		}

		certs, _ := certholder.DefaultHolder.ForResource(api.SchemeGroupVersion.WithResource(api.ResourcePluralPgpool), o.pgpool.ObjectMeta)
		paths, err := certs.Save(&certSecret)
		if err != nil {
			klog.Error(err, "failed to save certificate")
			return "", err
		}
		if o.pgpool.Spec.ClientAuthMode == api.PgpoolClientAuthModeCert {
			cnnstr = fmt.Sprintf("user=%s password=%s host=%s port=%d connect_timeout=10 dbname=%s sslmode=%s sslrootcert=%s sslcert=%s sslkey=%s", user, pass, dnsName, DefaultPgpoolPort, o.backendDBName, sslMode, paths.CACert, paths.Cert, paths.Key)
		} else {
			cnnstr = fmt.Sprintf("user=%s password=%s host=%s port=%d connect_timeout=10 dbname=%s sslmode=%s sslrootcert=%s", user, pass, dnsName, DefaultPgpoolPort, o.backendDBName, sslMode, paths.CACert)
		}
	} else {
		cnnstr = fmt.Sprintf("user=%s password=%s host=%s port=%d connect_timeout=10 dbname=%s sslmode=%s", user, pass, dnsName, DefaultPgpoolPort, o.backendDBName, sslMode)
	}
	return cnnstr, nil
}
