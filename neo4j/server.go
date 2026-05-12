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

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"k8s.io/klog/v2"
)

type ServerInfo struct {
	Name    string
	Address string
	Health  string
	State   string
	ID      string
}

func (c *Client) RenameServer(ctx context.Context) error {
	servers, err := c.GetServersInfo(ctx)
	if err != nil {
		return fmt.Errorf("failed to get server info: %w", err)
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

	for _, server := range servers {
		podName := strings.Split(server.Address, ".")[0]
		if podName != server.Name && server.Address != "" {
			_, err := session.Run(ctx, "RENAME SERVER $current TO $desired", map[string]any{"current": server.Name, "desired": podName})
			if err != nil {
				return fmt.Errorf("failed to rename server from %s to %s: %w", server.Name, podName, err)
			}
		}
	}
	return nil
}

func (c *Client) GetServersInfo(ctx context.Context) ([]ServerInfo, error) {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeRead,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	query := "SHOW SERVERS YIELD name, address, health, state, serverId RETURN name, address, health, state, serverId"

	result, err := session.Run(ctx, query, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query to get server info: %w", err)
	}

	records, err := result.Collect(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to collect query results to get server info: %w", err)
	}

	var servers []ServerInfo
	for _, record := range records {
		name, _ := record.Get("name")
		addressV, _ := record.Get("address")
		health, _ := record.Get("health")
		state, _ := record.Get("state")
		id, _ := record.Get("serverId")

		var address string
		if addressV != nil {
			if s, ok := addressV.(string); ok {
				address = s
			}
		}

		servers = append(servers, ServerInfo{
			Name:    name.(string),
			Address: address,
			Health:  health.(string),
			State:   state.(string),
			ID:      id.(string),
		})
	}

	return servers, nil
}

func (c *Client) GetServerState(ctx context.Context, serverName string) (string, error) {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	query := fmt.Sprintf("SHOW SERVERS YIELD name, state WHERE name = '%s' RETURN state", serverName)
	result, err := session.Run(ctx, query, nil)
	if err != nil {
		return "", fmt.Errorf("failed to execute query to get server state for server %s: %w", serverName, err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to collect query results to get server state for server %s: %w", serverName, err)
	}
	if len(records) == 0 {
		return "", fmt.Errorf("no server found with name %s", serverName)
	}
	state, _ := records[0].Get("state")
	return state.(string), nil
}

func (c *Client) IsServerDeallocated(ctx context.Context, serverName string) (bool, error) {
	state, err := c.GetServerState(ctx, serverName)
	if err != nil {
		return false, fmt.Errorf("failed to get server state: %w", err)
	}
	return strings.EqualFold(state, "Deallocated"), nil
}

func (c *Client) DropServer(ctx context.Context, serverName string) error {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()
	query := fmt.Sprintf("DROP SERVER '%s'", serverName)
	_, err := session.Run(ctx, query, nil)
	if err != nil {
		return fmt.Errorf("failed to drop server %s: %w", serverName, err)
	}

	klog.Infof("dropped server %s", serverName)

	return nil
}

func (c *Client) NumberOfHealthyServers(ctx context.Context) (int32, error) {
	servers, err := c.GetServersInfo(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get server info: %w", err)
	}
	var count int32 = 0
	for _, server := range servers {
		if server.Health == "Available" {
			count++
		}
	}
	return count, nil
}
