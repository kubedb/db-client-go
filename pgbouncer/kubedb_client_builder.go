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

	"kubedb.dev/apimachinery/apis/kubedb"
	dbapi "kubedb.dev/apimachinery/apis/kubedb/v1"

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
	DefaultBackendDBType = "postgres"
)

type Auth struct {
	UserName string
	Password string
}

type KubeDBClientBuilder struct {
	kc              client.Client
	pgbouncer       *dbapi.PgBouncer
	url             string
	podName         string
	pbContainerPort *int32
	backendDBName   string
	ctx             context.Context
	databaseRef     *dbapi.Database
	auth            *Auth
}

func NewKubeDBClientBuilder(kc client.Client, pb *dbapi.PgBouncer) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc:        kc,
		pgbouncer: pb,
	}
}

func (o *KubeDBClientBuilder) WithURL(url string) *KubeDBClientBuilder {
	o.url = url
	return o
}

func (o *KubeDBClientBuilder) WithPbPort(listenPort int32) *KubeDBClientBuilder {
	o.pbContainerPort = &listenPort
	return o
}

func (o *KubeDBClientBuilder) WithAuth(auth *Auth) *KubeDBClientBuilder {
	if auth != nil && auth.UserName != "" && auth.Password != "" {
		o.auth = auth
	}
	return o
}

func (o *KubeDBClientBuilder) WithPod(podName string) *KubeDBClientBuilder {
	o.podName = podName
	return o
}

func (o *KubeDBClientBuilder) WithDatabaseRef(db *dbapi.Database) *KubeDBClientBuilder {
	o.databaseRef = db
	return o
}

func (o *KubeDBClientBuilder) WithDatabaseName(dbName string) *KubeDBClientBuilder {
	if dbName == "" {
		o.backendDBName = o.databaseRef.DatabaseName
	} else {
		o.backendDBName = dbName
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

	engine, err := xorm.NewEngine(DefaultBackendDBType, connector)
	if err != nil {
		return nil, err
	}
	if engine == nil {
		return nil, fmt.Errorf("Xorm Engine can't be build for pgbouncer")
	}

	engine.SetDefaultContext(o.ctx)
	return &XormClient{
		engine,
	}, nil
}

func (o *KubeDBClientBuilder) getURL() string {
	return fmt.Sprintf("%s.%s.%s.svc", o.podName, o.pgbouncer.GoverningServiceName(), o.pgbouncer.Namespace)
}

func (o *KubeDBClientBuilder) GetBackendAuth() (string, string, error) {
	if o.auth != nil {
		return o.auth.UserName, o.auth.Password, nil
	}

	db := o.databaseRef

	if db == nil {
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

func (o *KubeDBClientBuilder) getTLSConfig(ctx context.Context) (*certholder.Paths, error) {
	secretName := o.pgbouncer.GetCertSecretName(dbapi.PgBouncerClientCert)

	var certSecret core.Secret
	err := o.kc.Get(ctx, client.ObjectKey{Namespace: o.pgbouncer.Namespace, Name: secretName}, &certSecret)
	if err != nil {
		klog.Error(err, "failed to get certificate secret.", secretName)
		return nil, err
	}

	certs, _ := certholder.DefaultHolder.ForResource(dbapi.SchemeGroupVersion.WithResource(dbapi.ResourcePluralPgBouncer), o.pgbouncer.ObjectMeta)
	paths, err := certs.Save(&certSecret)
	if err != nil {
		klog.Error(err, "failed to save certificate")
		return nil, err
	}
	return paths, nil
}

func (o *KubeDBClientBuilder) getConnectionString() (string, error) {
	user, pass, err := o.GetBackendAuth()
	if err != nil {
		return "", err
	}

	if o.podName != "" {
		o.url = o.getURL()
	}

	var listeningPort int = kubedb.PgBouncerDatabasePort
	if o.pbContainerPort != nil {
		listeningPort = int(*o.pbContainerPort)
	}

	sslMode := o.pgbouncer.Spec.SSLMode
	if sslMode == "" {
		sslMode = dbapi.PgBouncerSSLModeDisable
	}
	connector := ""
	if o.pgbouncer.Spec.TLS != nil {
		paths, err := o.getTLSConfig(o.ctx)
		if err != nil {
			return "", err
		}
		if o.pgbouncer.Spec.SSLMode == dbapi.PgBouncerSSLModeVerifyCA || o.pgbouncer.Spec.SSLMode == dbapi.PgBouncerSSLModeVerifyFull {
			connector = fmt.Sprintf("user=%s password=%s host=%s port=%d connect_timeout=10 dbname=%s sslmode=%s sslrootcert=%s sslcert=%s sslkey=%s", user, pass, o.url, listeningPort, o.backendDBName, sslMode, paths.CACert, paths.Cert, paths.Key)
		} else {
			connector = fmt.Sprintf("user=%s password=%s host=%s port=%d connect_timeout=10 dbname=%s sslmode=%s sslrootcert=%s", user, pass, o.url, listeningPort, o.backendDBName, sslMode, paths.CACert)
		}
	} else {
		connector = fmt.Sprintf("user=%s password=%s host=%s port=%d connect_timeout=10 dbname=%s sslmode=%s", user, pass, o.url, listeningPort, o.backendDBName, sslMode)
	}
	return connector, nil
}

func GetXormClientList(kc client.Client, pb *dbapi.PgBouncer, ctx context.Context, auth *Auth, dbName string, listenPort rune) (*XormClientList, error) {
	clientlist := &XormClientList{
		List: []*XormClient{},
	}
	clientlist.context = ctx
	clientlist.pb = pb
	clientlist.auth = auth
	clientlist.dbName = dbName
	clientlist.pbContainerPort = listenPort

	for i := 0; int32(i) < *pb.Spec.Replicas; i++ {
		podName := fmt.Sprintf("%s-%d", pb.OffshootName(), i)
		pod := core.Pod{}
		err := kc.Get(ctx, types.NamespacedName{Name: podName, Namespace: pb.Namespace}, &pod)
		if err != nil {
			return clientlist, err
		}
		clientlist.Mutex.Lock()
		clientlist.WG.Add(1)
		clientlist.Mutex.Unlock()
		go clientlist.addXormClient(kc, podName)
	}

	clientlist.WG.Wait()

	if len(clientlist.List) != int(*pb.Spec.Replicas) {
		return clientlist, fmt.Errorf("Failed to generate Xorm Client List")
	}

	return clientlist, nil
}

func (l *XormClientList) addXormClient(kc client.Client, podName string) {
	xormClient, err := NewKubeDBClientBuilder(kc, l.pb).WithContext(l.context).WithDatabaseRef(&l.pb.Spec.Database).
		WithPod(podName).WithAuth(l.auth).WithDatabaseName(l.dbName).WithPbPort(l.pbContainerPort).GetPgBouncerXormClient()

	l.Mutex.Lock()
	defer l.Mutex.Unlock()
	if err != nil {
		klog.V(5).ErrorS(err, fmt.Sprintf("failed to create xorm client for pgbouncer %s/%s ", l.pb.Namespace, l.pb.Name))
	} else {
		l.List = append(l.List, xormClient)
	}
	l.WG.Done()
}
