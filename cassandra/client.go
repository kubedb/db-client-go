package cassandra

import (
	"fmt"
	"log"

	"k8s.io/klog/v2"
	health "kmodules.xyz/client-go/tools/healthchecker"

	"github.com/gocql/gocql"
)

type Client struct {
	*gocql.Session
}

// CreateKeyspace creates a keyspace
func (c *Client) CreateKeyspace() error {
	return c.Query(`CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'}`).Exec()
}

// CreateTable creates a table
func (c *Client) CreateTable() error {
	return c.Query(`CREATE TABLE IF NOT EXISTS test_keyspace.test_table (
        name TEXT PRIMARY KEY,
        product TEXT
    )`).Exec()
}

// InsertUser inserts a user into the table
func (c *Client) InsertUser(name string, product string) error {
	return c.Query(`INSERT INTO test_keyspace.test_table ( name, product) VALUES (?, ?)`,
		name, product).Exec()
}

func (c *Client) DeleteUser(name string) error {
	return c.Query(`DELETE FROM test_keyspace.test_table WHERE name = ?`, name).Exec()
}

// queries a user by ID
func (c *Client) QueryUser(name string) error {
	var product string

	iter := c.Query(`SELECT product FROM test_keyspace.test_table WHERE name = ?`, name).Iter()
	if iter.Scan(&product) {
		if err := iter.Close(); err != nil {
			return fmt.Errorf("unable to query data: %v", err)
		}
		return nil
	}
	return fmt.Errorf("no data found")
}

func (c *Client) CheckDbReadWrite() error {
	if err := c.CreateKeyspace(); err != nil {
		log.Fatal("Unable to create keyspace:", err)
	}
	if err := c.CreateTable(); err != nil {
		log.Fatal("Unable to create table:", err)
	}
	if err := c.InsertUser("Appscode", "KubeDB"); err != nil {
		log.Fatal("Unable to insert data:", err)
	}

	err := c.QueryUser("Appscode")
	if err != nil {
		return err
	}
	klog.Infoln("DB Read Write Successful")
	err = c.DeleteUser("Appscode")
	return err
}

func (c *Client) PingCassandra() error {
	query := c.Query("SELECT now() FROM system.local")
	if err := query.Exec(); err != nil {
		return err
	}
	return nil
}

func (c *Client) CloseCassandraClient(hcf *health.HealthCard) {
	if c != nil {
		c.Close()
	}
	hcf.ClientClosed()
}
