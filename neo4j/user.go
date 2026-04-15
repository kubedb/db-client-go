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

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"k8s.io/klog/v2"
)

func (c *Client) CreateUser(ctx context.Context, username, password string) error {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	query := fmt.Sprintf("CREATE USER `%s` IF NOT EXISTS SET PASSWORD $password CHANGE NOT REQUIRED", username)

	params := map[string]any{
		"password": password,
	}

	_, err := session.Run(ctx, query, params)
	if err != nil {
		return fmt.Errorf("failed to create user %s: %w", username, err)
	}

	return nil
}

func (c *Client) DropUser(ctx context.Context, username string) error {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	query := fmt.Sprintf("DROP USER `%s` IF EXISTS", username)

	_, err := session.Run(ctx, query, nil)
	if err != nil {
		return fmt.Errorf("failed to drop user %s: %w", username, err)
	}

	return nil
}

func (c *Client) GrantRoleToUser(ctx context.Context, username, role string) error {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	query := fmt.Sprintf("GRANT ROLE `%s` TO `%s`", role, username)

	_, err := session.Run(ctx, query, nil)
	if err != nil {
		return fmt.Errorf("failed to grant role %s to user %s: %w", role, username, err)
	}
	return nil
}

func (c *Client) UpdateUserPassword(ctx context.Context, username, newPassword string) error {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	query := fmt.Sprintf("ALTER USER `%s` SET PASSWORD $password", username)

	params := map[string]any{
		"password": newPassword,
	}

	_, err := session.Run(ctx, query, params)
	if err != nil {
		return fmt.Errorf("failed to update password for user %s, pass %s: %w", username, newPassword, err)
	}
	return nil
}
