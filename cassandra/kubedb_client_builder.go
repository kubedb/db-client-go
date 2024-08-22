package cassandra

import (
	"context"
	"fmt"

	"strconv"

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
func (o *KubeDBClientBuilder) GetCassandraClient(dns string) (*Client, error) {
	host := dns
	port := "9042"
	cluster := gocql.NewCluster(host)
	p, err := strconv.Atoi(port)
	if err != nil {
		return nil, fmt.Errorf("invalid port : %v", err)
	}
	cluster.Port = p
	cluster.Keyspace = "system"
	//cluster.Consistency = gocql.Any  //ANY ConsistencyLevel is only supported for writes
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("unable to connect to Cassandra cluster: %v", err)
	}

	return &Client{session}, nil
}

// CreateKeyspace creates a keyspace
func (c *Client) CreateKeyspace() error {
	return c.Query(`CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'}`).Exec()
}

// CreateTable creates a table
func (c *Client) CreateTable() error {
	return c.Query(`CREATE TABLE IF NOT EXISTS mykeyspace.users (
        id UUID PRIMARY KEY,
        name TEXT,
        age INT,
        email TEXT
    )`).Exec()
}

// InsertUser inserts a user into the table
func (c *Client) InsertUser(id gocql.UUID, name string, age int, email string) error {
	return c.Query(`INSERT INTO mykeyspace.users (id, name, age, email) VALUES (?, ?, ?, ?)`,
		id, name, age, email).Exec()
}
func (c *Client) DeleteUser(id gocql.UUID) error {
	return c.Query(`DELETE FROM mykeyspace.users WHERE id = ?`, id).Exec()
}

// QueryUser queries a user by ID
func (c *Client) QueryUser(id gocql.UUID) (string, int, string, error) {
	var name string
	var age int
	var email string

	iter := c.Query(`SELECT name, age, email FROM mykeyspace.users WHERE id = ?`, id).Iter()
	if iter.Scan(&name, &age, &email) {
		if err := iter.Close(); err != nil {
			return "", 0, "", fmt.Errorf("unable to query data: %v", err)
		}
		return name, age, email, nil
	}
	return "", 0, "", fmt.Errorf("no data found")
}
