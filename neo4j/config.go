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

func (c *Client) ReloadTLS(ctx context.Context) error {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	_, err := session.Run(timeoutCtx, `
	CALL dbms.security.reloadTLS()
	`, nil)
	if err != nil {
		return fmt.Errorf("failed to execute TLS reload procedure: %w", err)
	}

	return nil
}

func (c *Client) SetConfigKeyValue(ctx context.Context, key, value string) error {
	session := c.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			klog.Error(err, "failed to close neo4j session")
		}
	}()

	query := fmt.Sprintf("CALL dbms.setConfigValue('%s', '%s')", key, value)

	_, err := session.Run(ctx, query, nil)
	if err != nil {
		return fmt.Errorf("failed to set config value for key %s to %s: %w", key, value, err)
	}
	return nil
}
