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

package pgbouncer

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	appbinding "kmodules.xyz/custom-resources/apis/appcatalog/v1alpha1"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	_ "github.com/lib/pq"
	core "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"xorm.io/xorm"
)

const (
	DefaultBackendDBName = "postgres"
	DefaultPgBouncerPort = api.PgBouncerDatabasePort
	TLSModeDisable       = "disable"
)

type KubeDBClientBuilder struct {
	kc            client.Client
	pgbouncer     *api.PgBouncer
	url           string
	podName       string
	backendDBName string
	ctx           context.Context
	databaseRef   *api.Databases
}

func NewKubeDBClientBuilder(kc client.Client, pb *api.PgBouncer) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc:        kc,
		pgbouncer: pb,
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

func (o *KubeDBClientBuilder) WithDatabaseRef(db *api.Databases) *KubeDBClientBuilder {
	o.databaseRef = db
	return o
}

func (o *KubeDBClientBuilder) WithPgBouncerDB(pgDB string) *KubeDBClientBuilder {
	o.backendDBName = pgDB
	return o
}

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}

func (o *KubeDBClientBuilder) GetPgBouncerXormClient() (*XormClient, error) {
	if o.ctx == nil {
		o.ctx = context.Background()
	}

	connector, err := o.getConnectionString()
	if err != nil {
		return nil, err
	}

	engine, err := xorm.NewEngine(DefaultBackendDBName, connector)
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
	return fmt.Sprintf("%s.%s.%s.svc", o.podName, o.pgbouncer.GoverningServiceName(), o.pgbouncer.Namespace)
}

func (o *KubeDBClientBuilder) getBackendAuth() (string, string, error) {

	db := o.databaseRef

	if db == nil || &db.DatabaseRef == nil {
		return "", "", fmt.Errorf("there is no Database found for pgBouncer %s/%s", o.pgbouncer.Namespace, o.pgbouncer.Name)
	}
	appBinding := &appbinding.AppBinding{}
	err := o.kc.Get(o.ctx, types.NamespacedName{
		Name:      db.DatabaseRef.Name,
		Namespace: db.DatabaseRef.Namespace,
	}, appBinding)
	if err != nil {
		return "", "", err
	}
	if appBinding.Spec.Secret == nil {
		return "", "", fmt.Errorf("backend postgres auth secret unspecified for pgBouncer %s/%s", o.pgbouncer.Namespace, o.pgbouncer.Name)
	}

	var secret core.Secret
	err = o.kc.Get(o.ctx, client.ObjectKey{Namespace: appBinding.Namespace, Name: appBinding.Spec.Secret.Name}, &secret)
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
	var listeningPort int = DefaultPgBouncerPort
	if o.pgbouncer.Spec.ConnectionPool.Port != nil {
		listeningPort = int(*o.pgbouncer.Spec.ConnectionPool.Port)
	}
	//TODO ssl mode is disable now need to work on this after adding tls support
	connector := fmt.Sprintf("user=%s password=%s host=%s port=%d connect_timeout=10 dbname=%s sslmode=%s", user, pass, o.url, listeningPort, o.backendDBName, TLSModeDisable)
	return connector, nil
}

func GetXormClientList(kc client.Client, pb *api.PgBouncer, ctx context.Context) (*XormClientList, error) {
	Clientlist := &XormClientList{
		list: []XormClient{},
	}

	podList := &corev1.PodList{}
	err := kc.List(context.Background(), podList, client.MatchingLabels(pb.PodLabels()))

	for _, postgresRef := range pb.Spec.Databases {
		for _, pods := range podList.Items {

		}

	}
	return Clientlist, nil
}
func (l *XormClientList) getXormClient(kc client.Client, pb *api.PgBouncer, ctx context.Context, podName string, postgresRef *api.Databases) {
	NewKubeDBClientBuilder(kc, pb).WithContext(ctx).WithDatabaseRef(postgresRef).WithPod(podName).GetPgBouncerXormClient()
	if err != nil {
		klog.V(5).ErrorS(err, fmt.Sprintf("failed to create xorm client for pgbouncer %v to make pool with postgres pod %s/%s", key, postgresRef.DatabaseRef.Namespace, postgresRef.DatabaseRef.Name))
		if hcs.HasFailed(health.HealthCheckClientFailure, err) {
			// Since the client was unable to connect the database,
			// update "AcceptingConnection" to "false".
			// update "Ready" to "false"
			if err := c.updateConditionsForUnhealthy(ctx, db, err); err != nil {
				return
			}
		}
		klog.Error(err)
		return
	}
	dbXormClient = append(dbXormClient, *XormClient)
}
