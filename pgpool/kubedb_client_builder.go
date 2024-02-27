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

	_ "github.com/lib/pq"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	appbinding "kmodules.xyz/custom-resources/apis/appcatalog/v1alpha1"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"xorm.io/xorm"
)

const (
	DefaultBackendDBName = "postgres"
	DefaultPgpoolPort    = 9999
	TLSModeDisable       = "disable"
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
	user, pass, err := o.getBackendAuth()
	if err != nil {
		return "", err
	}

	if o.podName != "" {
		o.url = o.getURL()
	}

	if o.backendDBName == "" {
		o.backendDBName = DefaultBackendDBName
	}
	//TODO ssl mode is disable now need to work on this after adding tls support
	connector := fmt.Sprintf("user=%s password=%s host=%s port=%d connect_timeout=10 dbname=%s sslmode=%s", user, pass, o.url, DefaultPgpoolPort, o.backendDBName, TLSModeDisable)
	return connector, nil
}
