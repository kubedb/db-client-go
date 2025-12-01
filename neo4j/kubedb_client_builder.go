package neo4j

import (
	"context"
	"errors"
	"fmt"
	"time"

	"strings"

	"github.com/go-logr/logr"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"kubedb.dev/apimachinery/apis/kubedb"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	apiutils "kubedb.dev/apimachinery/pkg/utils"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc      client.Client
	db      *api.Neo4j
	log     logr.Logger
	url     string
	podName string
	ctx     context.Context
}

func NewKubeDBClientBuilder(kc client.Client, db *api.Neo4j) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc: kc,
		db: db,
	}
}

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}

func (o *KubeDBClientBuilder) WithLog(log logr.Logger) *KubeDBClientBuilder {
	o.log = log
	return o
}

func (o *KubeDBClientBuilder) GetNeo4jClient() (*Client, error) {
	// Guard against nil receiver or missing required fields to avoid nil deref
	if o == nil {
		return nil, fmt.Errorf("KubeDBClientBuilder is nil")
	}
	if o.db == nil {
		return nil, fmt.Errorf("neo4j object is nil")
	}
	if o.kc == nil {
		return nil, fmt.Errorf("kubernetes client is nil")
	}
	// Ensure context is non-nil
	if o.ctx == nil {
		o.ctx = context.TODO()
	}
	// Get domain and fallback to cluster.local if not found
	domain := apiutils.FindDomain()
	if domain == "" {
		domain = "cluster.local"
	}

	// Construct URL - use default service if podName not provided
	if o.podName != "" {
		o.url = fmt.Sprintf("neo4j://%s.%s.%s.svc.%s:%d", o.podName, o.db.GoverningServiceName(), o.db.Namespace, domain, kubedb.Neo4jBoltPort)
	} else {
		o.url = fmt.Sprintf("neo4j://%s.%s.svc.%s:%d", o.db.ServiceName(), o.db.Namespace, domain, kubedb.Neo4jBoltPort)
	}

	klog.V(3).Infof("Attempting to connect to Neo4j at: %s", o.url)

	var dbUser, dbPassword string
	authSecret := &core.Secret{}

	if !o.db.Spec.DisableSecurity {
		err := o.kc.Get(o.ctx, types.NamespacedName{
			Namespace: o.db.Namespace,
			Name:      o.db.GetAuthSecretName(),
		}, authSecret)
		if err != nil {
			if kerr.IsNotFound(err) {
				klog.Error(err, "Auth-secret not found")
				return nil, errors.New("auth-secret is not found")
			}
			klog.Error(err, "Failed to get auth-secret")
			return nil, err
		}

		// Extract NEO4J_AUTH from Secret.Data
		auth, ok := authSecret.Data["NEO4J_AUTH"]
		if !ok {
			return nil, fmt.Errorf("secret %s/%s does not contain key NEO4J_AUTH", o.db.Namespace, o.db.GetAuthSecretName())
		}
		authStr := strings.TrimSpace(string(auth))

		// Expect "username/password", split safely
		parts := strings.SplitN(authStr, "/", 2)
		if len(parts) != 2 || parts[0] == "" {
			return nil, fmt.Errorf("invalid NEO4J_AUTH format in secret %s/%s. Expected \"username/password\"", o.db.Namespace, o.db.GetAuthSecretName())
		}
		dbUser = parts[0]
		dbPassword = parts[1]
	} else {
		klog.Info("Security is disabled for Neo4j, no credentials will be used.")
	}

	// Create driver and check for errors immediately
	driver, err := neo4j.NewDriverWithContext(o.url, neo4j.BasicAuth(dbUser, dbPassword, ""), func(c *neo4j.Config) {
		c.SocketConnectTimeout = 60 * time.Second
		c.ConnectionAcquisitionTimeout = 60 * time.Second
		c.MaxTransactionRetryTime = 60 * time.Second
		c.MaxConnectionLifetime = 30 * time.Minute
		c.MaxConnectionPoolSize = 100
	})
	if o.db.Spec.DisableSecurity {
		driver, err = neo4j.NewDriverWithContext(o.url, neo4j.NoAuth())
	}
	if err != nil {
		klog.Error(err, "Failed to create Neo4j driver")
		return nil, err
	}

	// Now verify connectivity on the successfully created driver
	if err = driver.VerifyConnectivity(o.ctx); err != nil {
		klog.Error(err, "Failed to connect to Neo4j")
		// Close driver on verification failure
		_ = driver.Close(o.ctx)
		return nil, err
	}

	fmt.Println("Connection established.")

	return &Client{
		DriverWithContext: driver,
	}, nil
}

func (c *Client) ExecuteQuery(ctx context.Context, query string, params map[string]any, dbName string) (*neo4j.EagerResult, error) {
	return neo4j.ExecuteQuery(ctx, c, query, params, neo4j.EagerResultTransformer,
		neo4j.ExecuteQueryWithDatabase(dbName))
}
