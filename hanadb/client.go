package hanadb

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type SqlClient struct {
	*sql.DB
}

func (sc *SqlClient) Close() error {
	if sc.DB != nil {
		return sc.DB.Close()
	}
	return nil
}

// TenantDatabaseExists checks if a tenant database exists in SYSTEMDB.
func (sc *SqlClient) TenantDatabaseExists(ctx context.Context, dbName string) (bool, error) {
	query := `SELECT COUNT(*) FROM M_DATABASES WHERE DATABASE_NAME = ?`

	var count int
	err := sc.QueryRowContext(ctx, query, dbName).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check if database %s exists: %v", dbName, err)
	}

	return count > 0, nil
}

// CreateTenantDatabase creates a new tenant database with the specified name and password.
func (sc *SqlClient) CreateTenantDatabase(ctx context.Context, dbName, password string) error {
	query := fmt.Sprintf(`CREATE DATABASE %s SYSTEM USER PASSWORD "%s"`, dbName, password)

	createCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	if _, err := sc.ExecContext(createCtx, query); err != nil {
		return fmt.Errorf("failed to create database %s: %v", dbName, err)
	}

	return nil
}

// EnsureTenantDatabase checks if the tenant database exists, and creates it if not.
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

		time.Sleep(5 * time.Second) // wait briefly for database readiness
	}

	return nil
}
