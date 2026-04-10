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
