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

package hanadb

import (
	"context"
	"fmt"
	"time"
)

// TenantDatabaseExists checks if a tenant database exists in SYSTEMDB
func (sc *SqlClient) TenantDatabaseExists(ctx context.Context, dbName string) (bool, error) {
	query := `SELECT COUNT(*) FROM M_DATABASES WHERE DATABASE_NAME = ?`

	var count int
	err := sc.QueryRowContext(ctx, query, dbName).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check if database %s exists: %v", dbName, err)
	}

	return count > 0, nil
}

// CreateTenantDatabase creates a new tenant database with the specified name and password
func (sc *SqlClient) CreateTenantDatabase(ctx context.Context, dbName, password string) error {
	// Create database with timeout (can take ~1 minute)
	query := fmt.Sprintf(`CREATE DATABASE %s SYSTEM USER PASSWORD "%s"`, dbName, password)

	// Use a longer timeout for database creation (120 seconds)
	createCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	_, err := sc.ExecContext(createCtx, query)
	if err != nil {
		return fmt.Errorf("failed to create database %s: %v", dbName, err)
	}

	return nil
}

// EnsureTenantDatabase checks if the tenant database exists, creates it if not
func (sc *SqlClient) EnsureTenantDatabase(ctx context.Context, dbName, password string) error {
	exists, err := sc.TenantDatabaseExists(ctx, dbName)
	if err != nil {
		return err
	}

	if !exists {
		fmt.Printf("Tenant database %s does not exist, creating it...\n", dbName)
		if err := sc.CreateTenantDatabase(ctx, dbName, password); err != nil {
			return err
		}
		fmt.Printf("Successfully created tenant database %s\n", dbName)

		// Wait a bit for the database to be fully ready
		time.Sleep(5 * time.Second)
	}

	return nil
}

// DropTenantDatabase drops a tenant database
func (sc *SqlClient) DropTenantDatabase(ctx context.Context, dbName string) error {
	query := fmt.Sprintf(`DROP DATABASE %s CASCADE`, dbName)

	dropCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	_, err := sc.ExecContext(dropCtx, query)
	if err != nil {
		return fmt.Errorf("failed to drop database %s: %v", dbName, err)
	}

	return nil
}

// ListTenantDatabases lists all tenant databases (excluding SYSTEMDB)
func (sc *SqlClient) ListTenantDatabases(ctx context.Context) ([]string, error) {
	query := `SELECT DATABASE_NAME FROM M_DATABASES WHERE DATABASE_NAME != 'SYSTEMDB' ORDER BY DATABASE_NAME`

	rows, err := sc.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list tenant databases: %v", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, fmt.Errorf("failed to scan database name: %v", err)
		}
		databases = append(databases, dbName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tenant databases: %v", err)
	}

	return databases, nil
}

// GetTenantDatabaseStatus gets the operational status of a tenant database
func (sc *SqlClient) GetTenantDatabaseStatus(ctx context.Context, dbName string) (string, error) {
	query := `SELECT ACTIVE_STATUS FROM M_DATABASES WHERE DATABASE_NAME = ?`

	var status string
	err := sc.QueryRowContext(ctx, query, dbName).Scan(&status)
	if err != nil {
		return "", fmt.Errorf("failed to get status for database %s: %v", dbName, err)
	}

	return status, nil
}

// StopTenantDatabase stops a tenant database
func (sc *SqlClient) StopTenantDatabase(ctx context.Context, dbName string) error {
	query := fmt.Sprintf(`ALTER SYSTEM STOP DATABASE %s`, dbName)

	stopCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	_, err := sc.ExecContext(stopCtx, query)
	if err != nil {
		return fmt.Errorf("failed to stop database %s: %v", dbName, err)
	}

	return nil
}

// StartTenantDatabase starts a tenant database
func (sc *SqlClient) StartTenantDatabase(ctx context.Context, dbName string) error {
	query := fmt.Sprintf(`ALTER SYSTEM START DATABASE %s`, dbName)

	startCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	_, err := sc.ExecContext(startCtx, query)
	if err != nil {
		return fmt.Errorf("failed to start database %s: %v", dbName, err)
	}

	return nil
}

// CreateTenantUser creates a user in a tenant database
func (sc *SqlClient) CreateTenantUser(ctx context.Context, username, password string) error {
	query := fmt.Sprintf(`CREATE USER %s PASSWORD "%s"`, username, password)

	_, err := sc.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create user %s: %v", username, err)
	}

	return nil
}

// GrantTenantUserPermissions grants permissions to a user in a tenant database
func (sc *SqlClient) GrantTenantUserPermissions(ctx context.Context, username string, permissions []string) error {
	for _, perm := range permissions {
		query := fmt.Sprintf(`GRANT %s TO %s`, perm, username)
		_, err := sc.ExecContext(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to grant %s to %s: %v", perm, username, err)
		}
	}

	return nil
}

// TenantUserExists checks if a user exists in the tenant database
func (sc *SqlClient) TenantUserExists(ctx context.Context, username string) (bool, error) {
	query := `SELECT COUNT(*) FROM SYS.USERS WHERE USER_NAME = ?`

	var count int
	err := sc.QueryRowContext(ctx, query, username).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check if user %s exists: %v", username, err)
	}

	return count > 0, nil
}
