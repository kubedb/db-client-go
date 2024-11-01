package cassandra

import (
	"context"
	"errors"
	"fmt"

	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"kubedb.dev/apimachinery/apis/kubedb"

	"github.com/gocql/gocql"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc   client.Client
	db   *api.Cassandra
	url  string
	port *int
	ctx  context.Context
}

func NewKubeDBClientBuilder(kc client.Client, db *api.Cassandra) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc: kc,
		db: db,
	}
}

func (o *KubeDBClientBuilder) WithURL(url string) *KubeDBClientBuilder {
	o.url = url
	return o
}

func (o *KubeDBClientBuilder) WithPort(port *int) *KubeDBClientBuilder {
	o.port = port
	return o
}

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}
func (o *KubeDBClientBuilder) GetCassandraClient() (*Client, error) {
	host := o.url
	cluster := gocql.NewCluster(host)
	cluster.Port = kubedb.CassandraNativeTcpPort
	cluster.Keyspace = "system"
	if o.db.Spec.Topology == nil {
		cluster.Consistency = gocql.One
	} else {
		cluster.Consistency = gocql.Quorum
	}
	if !o.db.Spec.DisableSecurity {

		authSecret := &core.Secret{}
		err := o.kc.Get(o.ctx, types.NamespacedName{
			Namespace: o.db.Namespace,
			Name:      o.db.GetAuthSecretName(),
		}, authSecret)
		if err != nil {
			if kerr.IsNotFound(err) {
				klog.Error(err, "AuthSecret not found")
				return nil, errors.New("auth-secret not found")
			}
			return nil, err
		}
		userName := string(authSecret.Data[core.BasicAuthUsernameKey])
		password := string(authSecret.Data[core.BasicAuthPasswordKey])
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: userName,
			Password: password,
		}
	}
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("unable to connect to Cassandra cluster: %v", err)
	}

	return &Client{session}, nil
}
