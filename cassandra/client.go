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

package cassandra

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"k8s.io/klog/v2"
)

type Client struct {
	*gocql.Session
}

// CreateKeyspace creates a keyspace
func (c *Client) CreateKeyspace() error {
	return c.Query(`CREATE KEYSPACE IF NOT EXISTS kubedb_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'}`).Exec()
}

// CreateTable creates a table
func (c *Client) CreateTable() error {
	return c.Query(`CREATE TABLE IF NOT EXISTS kubedb_keyspace.healthcheck_table (
        name TEXT PRIMARY KEY,
        product TEXT
    )`).Exec()
}

// UpdateData updates a record in the table
func (c *Client) UpdateData(name string, product string) error {
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	updatedProduct := fmt.Sprintf("%s - %s", product, currentTime)

	return c.Query(`UPDATE kubedb_keyspace.healthcheck_table SET product = ? where name = ? `,
		updatedProduct, name).Exec()
}

// queries a Data by ID
func (c *Client) QueryData(name string) error {
	var product string

	iter := c.Query(`SELECT product FROM kubedb_keyspace.healthcheck_table WHERE name = ?`, name).Iter()
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
		klog.Error("Unable to create keyspace:", err)
		return err
	}
	if err := c.CreateTable(); err != nil {
		klog.Error("Unable to create table:", err)
		return err
	}
	if err := c.UpdateData("Appscode", "KubeDB"); err != nil {
		klog.Error("Unable to update data:", err)
		return err
	}

	err := c.QueryData("Appscode")
	if err != nil {
		klog.Error("Unable to query data:", err)
		return err
	}
	return nil
}

func (c *Client) PingCassandra() error {
	query := c.Query("SELECT now() FROM system.local")
	if err := query.Exec(); err != nil {
		klog.Error("Unable to ping cassandra:", err)
		return err
	}
	return nil
}

func (c *Client) CloseCassandraClient() {
	if c != nil {
		c.Close()
	}
}
