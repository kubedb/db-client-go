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

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	_ "github.com/lib/pq"
	core "k8s.io/api/core/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	appbinding "kmodules.xyz/custom-resources/apis/appcatalog/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"xorm.io/xorm"
)

const (
	DefaultBackendDBType = "postgres"
	DefaultPgBouncerPort = api.PgBouncerDatabasePort
	TLSModeDisable       = "disable"
)

type Auth struct {
	userName string
	password string
}

type KubeDBClientBuilder struct {
	kc            client.Client
	pgbouncer     *api.PgBouncer
	url           string
	podName       string
	backendDBType string
	backendDBName string
	ctx           context.Context
	databaseRef   *api.Database
	auth          *Auth
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

func (o *KubeDBClientBuilder) WithAuth(auth *Auth) *KubeDBClientBuilder {
	if auth != nil && auth.userName != "" && auth.password != "" {
		o.auth = auth
	}
	return o
}

func GenerateAuth(userName, password string) *Auth {
	return &Auth{userName: userName, password: password}
}

func (o *KubeDBClientBuilder) WithPod(podName string) *KubeDBClientBuilder {
	o.podName = podName
	return o
}

func (o *KubeDBClientBuilder) WithDatabaseRef(db *api.Database) *KubeDBClientBuilder {
	o.databaseRef = db
	return o
}

func (o *KubeDBClientBuilder) WithDBName(dbName string) *KubeDBClientBuilder {
	if dbName == "" {
		o.backendDBName = o.databaseRef.DatabaseName
	} else {
		o.backendDBType = dbName
	}
	return o
}

func (o *KubeDBClientBuilder) WithPgBouncerDB(pgDB string) *KubeDBClientBuilder {
	if pgDB == "" {
		o.backendDBType = DefaultBackendDBType
	} else {
		o.backendDBType = pgDB
	}
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

	engine, err := xorm.NewEngine(o.backendDBType, connector)
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
	if o.auth != nil {
		return o.auth.userName, o.auth.password, nil
	}

	db := o.databaseRef

	if db == nil || &db.DatabaseRef == nil {
		return "", "", fmt.Errorf("there is no DatabaseReference found for pgBouncer %s/%s", o.pgbouncer.Namespace, o.pgbouncer.Name)
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

	user, present := secret.Data[core.BasicAuthUsernameKey]
	if !present {
		return "", "", fmt.Errorf("error getting backend username")
	}

	pass, present := secret.Data[core.BasicAuthPasswordKey]
	if !present {
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

	if o.backendDBType == "" {
		o.backendDBType = DefaultBackendDBType
	}
	var listeningPort int = DefaultPgBouncerPort
	if o.pgbouncer.Spec.ConnectionPool.Port != nil {
		listeningPort = int(*o.pgbouncer.Spec.ConnectionPool.Port)
	}
	// TODO ssl mode is disable now need to work on this after adding tls support
	connector := fmt.Sprintf("user=%s password=%s host=%s port=%d connect_timeout=10 dbname=%s sslmode=%s", user, pass, o.url, listeningPort, o.backendDBName, TLSModeDisable)
	return connector, nil
}

func GetXormClientList(kc client.Client, pb *api.PgBouncer, ctx context.Context, auth *Auth, dbType string, dbName string) (*XormClientList, error) {
	clientlist := &XormClientList{
		List: []*XormClient{},
	}

	podList := &corev1.PodList{}
	err := kc.List(context.Background(), podList, client.MatchingLabels(pb.PodLabels()))
	if err != nil {
		return nil, fmt.Errorf("failed get pod list for XormClientList")
	}
	ch := make(chan string)
	if &pb.Spec.Database != nil {
		postgresRef := pb.Spec.Database
		for _, pod := range podList.Items {
			go clientlist.addXormClient(kc, pb, ctx, pod.Name, &postgresRef, ch, len(podList.Items), auth, dbType, dbName)
		}
	}
	message := <-ch
	if message == "" {
		return clientlist, nil
	}
	return nil, fmt.Errorf(message)
}

func (l *XormClientList) addXormClient(kc client.Client, pb *api.PgBouncer, ctx context.Context, podName string, postgresRef *api.Database, c chan string, pgReplica int, auth *Auth, dbType string, dbName string) {
	xormClient, err := NewKubeDBClientBuilder(kc, pb).WithContext(ctx).WithDatabaseRef(postgresRef).WithPod(podName).WithAuth(auth).WithPgBouncerDB(dbType).WithDBName(dbName).GetPgBouncerXormClient()
	l.Mutex.Lock()
	defer l.Mutex.Unlock()
	if err != nil {
		klog.V(5).ErrorS(err, fmt.Sprintf("failed to create xorm client for pgbouncer %s/%s to make pool with database %s", pb.Namespace, pb.Name, postgresRef.DatabaseName))
		l.List = append(l.List, nil)
		if l.message == "" {
			l.message = fmt.Sprintf("failed to create xorm client for: pgbouncer %s/%s to make pool with database %s", pb.Namespace, pb.Name, postgresRef.DatabaseName)
		} else {
			l.message = fmt.Sprintf("%s pgbouncer %s/%s to make pool with database %s", l.message, pb.Namespace, pb.Name, postgresRef.DatabaseName)
		}
	} else {
		l.List = append(l.List, xormClient)
	}
	if (pgReplica) <= len(l.List) {
		c <- l.message
	}
}
