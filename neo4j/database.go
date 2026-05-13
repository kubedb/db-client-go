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

package neo4j

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"k8s.io/klog/v2"
)

func (c *Client) DatabasesHealthy(ctx context.Context) (bool, error) {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	query := "SHOW DATABASES YIELD name, currentStatus"
	result, err := session.Run(ctx, query, nil)
	if err != nil {
		return false, fmt.Errorf("failed to execute database health check query: %w", err)
	}

	records, err := result.Collect(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to collect database health check results: %w", err)
	}

	for _, record := range records {
		name, _ := record.Get("name")
		status, _ := record.Get("currentStatus")

		if status.(string) != "online" {
			klog.Infof("Database %s is not healthy. Current status: %s", name, status)
			return false, nil
		}
	}

	return true, nil
}

func (c *Client) IsDryRunEmpty(ctx context.Context) (bool, error) {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	query := "DRYRUN REALLOCATE DATABASES"

	result, err := session.Run(ctx, query, nil)
	if err != nil {
		return false, fmt.Errorf("DRYRUN failed: %w", err)
	}

	records, err := result.Collect(ctx)
	if err != nil {
		return false, fmt.Errorf("collecting DRYRUN results: %w", err)
	}

	if len(records) > 0 {
		klog.Infof("DRYRUN: %d reallocation(s) pending", len(records))
	}

	return len(records) == 0, nil
}

func (c *Client) IsDryRunEmptyForDeallocation(ctx context.Context, serverName string) (bool, error) {
	state, err := c.GetServerState(ctx, serverName)
	if err != nil {
		return false, fmt.Errorf("failed to check if server %s is deallocated: %w", serverName, err)
	}
	if isDeallocationTerminalState(state) {
		return true, nil
	}

	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	query := fmt.Sprintf("DRYRUN DEALLOCATE DATABASES FROM SERVER '%s'", serverName)

	result, err := session.Run(ctx, query, nil)
	if err != nil {
		return false, fmt.Errorf("DRYRUN failed: %w", err)
	}

	records, err := result.Collect(ctx)
	if err != nil {
		return false, fmt.Errorf("collecting DRYRUN results: %w", err)
	}

	if len(records) > 0 {
		klog.Infof("DRYRUN: %d deallocation(s) pending", len(records))
	}

	return len(records) == 0, nil
}

func (c *Client) ReallocateDatabasesBatch(ctx context.Context, batchSize int32) error {
	// wait for cluster to stabilise after batch
	healthy, err := c.DatabasesHealthy(ctx)
	if err != nil {
		return err
	}
	if !healthy {
		return fmt.Errorf("waiting for databases to be healthy before triggering reallocation")
	}

	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	query := "CALL dbms.cluster.reallocateNumberOfDatabases($n)"

	_, err = session.Run(ctx, query, map[string]any{"n": batchSize})
	if err != nil {
		return fmt.Errorf("reallocateNumberOfDatabases failed: %w", err)
	}

	klog.Infof("Batch of %d triggered successfully", batchSize)

	return nil
}

func (c *Client) DeallocateDatabasesBatch(ctx context.Context, batchSize int32, serverName string) error {
	// wait for cluster to stabilise before triggering next batch
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	for {
		healthy, err := c.DatabasesHealthy(waitCtx)
		if err != nil {
			klog.Errorf("failed to check database health: %v", err)
		} else if healthy {
			break // healthy, proceed with next batch
		} else {
			klog.Info("waiting for databases to be healthy before triggering next batch...")
		}

		select {
		case <-waitCtx.Done():
			return fmt.Errorf("timeout waiting for databases to be healthy before next batch")
		case <-time.After(30 * time.Second):
		}
	}

	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	query := "CALL dbms.cluster.deallocateNumberOfDatabases($name,$n)"
	result, err := session.Run(ctx, query, map[string]any{"name": serverName, "n": batchSize})
	if err != nil {
		return fmt.Errorf("deallocateNumberOfDatabases failed: %w", err)
	}
	if _, err := result.Consume(ctx); err != nil {
		return fmt.Errorf("consuming result failed: %w", err)
	}

	klog.Infof("Batch of %d triggered successfully", batchSize)
	return nil
}

func (c *Client) ReallocateDatabases(ctx context.Context) error {
	isEmpty, err := c.IsDryRunEmpty(ctx)
	if err != nil {
		return fmt.Errorf("DRYRUN check failed: %w", err)
	}
	if isEmpty {
		return nil
	}

	// wait for cluster to stabilise after batch
	healthy, err := c.DatabasesHealthy(ctx)
	if err != nil {
		return err
	}
	if !healthy {
		return fmt.Errorf("waiting for databases to be healthy before triggering reallocation")
	}

	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	query := "REALLOCATE DATABASES"

	_, err = session.Run(timeoutCtx, query, nil)
	if err != nil {
		return fmt.Errorf("full reallocation failed: %w", err)
	}

	klog.Info("Full reallocation triggered successfully")
	return nil
}

func (c *Client) DeallocateDatabases(ctx context.Context, serverName string) error {
	state, err := c.GetServerState(ctx, serverName)
	if err != nil {
		return fmt.Errorf("failed to check if server %s is deallocated: %w", serverName, err)
	}
	if isDeallocationTerminalState(state) {
		return nil
	}

	// wait for cluster to stabilise after batch
	healthy, err := c.DatabasesHealthy(ctx)
	if err != nil {
		return err
	}
	if !healthy {
		return fmt.Errorf("waiting for databases to be healthy before triggering reallocation")
	}

	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	query := fmt.Sprintf("DEALLOCATE DATABASES FROM SERVER '%s'", serverName)
	_, err = session.Run(timeoutCtx, query, nil)
	if err != nil {
		return fmt.Errorf("full deallocation failed for server %s: %w", serverName, err)
	}

	klog.Infof("Full deallocation triggered successfully for server %s: ", serverName)

	return nil
}

func (c *Client) CheckDatabaseExists(ctx context.Context, dbName string) (bool, error) {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	result, err := session.Run(ctx, "SHOW DATABASES YIELD name WHERE name = $db RETURN name LIMIT 1", map[string]any{"db": dbName})
	if err != nil {
		return false, err
	}

	records, err := result.Collect(ctx)
	if err != nil {
		return false, err
	}

	return len(records) > 0, nil
}

func (c *Client) DropDatabase(ctx context.Context, dbName string) error {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	query := fmt.Sprintf("DROP DATABASE `%s` IF EXISTS", dbName)

	result, err := session.Run(ctx, query, nil)
	if err != nil {
		return err
	}
	_, err = result.Consume(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) StopDatabase(ctx context.Context, dbName string) error {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	query := fmt.Sprintf("STOP DATABASE `%s`", dbName)
	_, err := session.Run(ctx, query, nil)
	if err != nil {
		return err
	}
	return nil
}

func (c Client) GetdatabaseState(ctx context.Context, dbName string) (string, error) {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	query := fmt.Sprintf("SHOW DATABASE `%s`", dbName)
	result, err := session.Run(ctx, query, nil)
	if err != nil {
		return "", err
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return "", err
	}

	if len(records) == 0 {
		return "", fmt.Errorf("no database found with name %s", dbName)
	}

	stateValue, ok := records[0].Get("currentStatus")
	if !ok || stateValue == nil {
		return "", fmt.Errorf("database %s does not have currentStatus in SHOW DATABASE result", dbName)
	}
	state, ok := stateValue.(string)
	if !ok {
		return "", fmt.Errorf("database %s currentStatus has unexpected type %T", dbName, stateValue)
	}
	return state, nil
}

func (c *Client) WaitForDatabaseState(ctx context.Context, dbName string, state string) error {
	for {
		currentState, err := c.GetdatabaseState(ctx, dbName)
		if err != nil {
			return err
		}

		if currentState == state {
			return nil
		}
		klog.Infof("Waiting for database %s to reach state %s. Current state: %s", dbName, state, currentState)

		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for database %s to reach state %s", dbName, state)
		case <-time.After(5 * time.Second):
		}

	}
}

func (c *Client) CreateDatabase(ctx context.Context, dbName string, primary, secondary int64, options map[string]string, ifNotExists bool) error {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	query := fmt.Sprintf("CREATE DATABASE `%s`", dbName)
	if ifNotExists {
		query = fmt.Sprintf("CREATE DATABASE `%s` IF NOT EXISTS", dbName)
	}

	if primary > 0 || secondary > 0 {
		ql := fmt.Sprintf("TOPOLOGY %d PRIMARY %d SECONDARY", primary, secondary)
		query += " " + ql
	}
	if len(options) > 0 {
		var optionParts []string
		for key, value := range options {
			optionParts = append(optionParts, fmt.Sprintf("%s: '%s'", key, value))
		}
		query += " OPTIONS {" + strings.Join(optionParts, ", ") + "}"
	}

	_, err := session.Run(ctx, query, nil)
	if err != nil {
		return err
	}
	return nil
}
