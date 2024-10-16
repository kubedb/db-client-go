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

// queries a user by ID
func (c *Client) QueryUser(id gocql.UUID) error {
	var name string
	var age int
	var email string

	iter := c.Query(`SELECT name, age, email FROM mykeyspace.users WHERE id = ?`, id).Iter()
	if iter.Scan(&name, &age, &email) {
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
	id := gocql.TimeUUID()
	if err := c.InsertUser(id, "John Doe", 30, "john.doe@example.com"); err != nil {
		log.Fatal("Unable to insert data:", err)
	}

	err := c.QueryUser(id)
	if err != nil {
		return err
	}
	klog.Infoln("DB Read Write Successful")
	err = c.DeleteUser(id)
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
