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
		err2 := engine.Close()
		if err2 != nil {
			return nil, err2
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
	clientlist := &XormClientList{
		list: []*XormClient{},
	}

	podList := &corev1.PodList{}
	err := kc.List(context.Background(), podList, client.MatchingLabels(pb.PodLabels()))

	if err != nil {
		return nil, fmt.Errorf("failed get pod list for XormClientList")
	}
	ch := make(chan string)
	for _, postgresRef := range pb.Spec.Databases {
		for _, pod := range podList.Items {
			go clientlist.addXormClient(kc, pb, ctx, pod.Name, &postgresRef, ch, len(podList.Items))
		}

	}
	message := <-ch
	if message == "" {
		return clientlist, nil
	}
	return nil, fmt.Errorf(message)
}
func (l *XormClientList) addXormClient(kc client.Client, pb *api.PgBouncer, ctx context.Context, podName string, postgresRef *api.Databases, c chan string, pgReplica int) {
	xormClient, err := NewKubeDBClientBuilder(kc, pb).WithContext(ctx).WithDatabaseRef(postgresRef).WithPod(podName).GetPgBouncerXormClient()
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if err != nil {
		klog.V(5).ErrorS(err, fmt.Sprintf("failed to create xorm client for pgbouncer %s/%s to make pool with postgres pod %s/%s", pb.Namespace, pb.Name, postgresRef.DatabaseRef.Namespace, postgresRef.DatabaseRef.Name))
		l.list = append(l.list, nil)
		if l.message == "" {
			l.message = fmt.Sprintf("failed to create xorm client for: pgbouncer %s/%s make pool with postgres pod %s/%s;", pb.Namespace, pb.Name, postgresRef.DatabaseRef.Namespace, postgresRef.DatabaseRef.Name)
		} else {
			l.message = fmt.Sprintf("%s pgbouncer %s/%s make pool with postgres pod %s/%s;", l.message, pb.Namespace, pb.Name, postgresRef.DatabaseRef.Namespace, postgresRef.DatabaseRef.Name)
		}
	} else {
		l.list = append(l.list, xormClient)
	}
	if (pgReplica * len(pb.Spec.Databases)) <= len(l.list) {
		c <- l.message
	}
}
