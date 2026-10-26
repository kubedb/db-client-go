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

package yugabytedb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// YSQLClient is a connection pool to the YSQL (PostgreSQL compatible) API, backed by pgx.
type YSQLClient struct {
	*sql.DB
}

// YCQLClient is a session to the YCQL (Cassandra compatible) API.
type YCQLClient struct {
	*gocql.Session
}

// Ping runs SELECT 1.
func (c *YSQLClient) Ping(ctx context.Context) error {
	var one int
	if err := c.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		return err
	}
	if one != 1 {
		return fmt.Errorf("SELECT 1 returned %d", one)
	}
	return nil
}

// AlterRolePassword runs ALTER ROLE <role> PASSWORD '<password>'. A utility statement takes no bind
// parameters, so the password is a quoted literal, sent with the simple protocol so that it is never
// kept in a prepared statement.
func (c *YSQLClient) AlterRolePassword(ctx context.Context, role, password string) error {
	conn, err := c.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close() // nolint:errcheck

	stmt := AlterRolePasswordSQL(role, password)
	return conn.Raw(func(driverConn any) error {
		_, err := driverConn.(*stdlib.Conn).Conn().Exec(ctx, stmt, pgx.QueryExecModeSimpleProtocol)
		return err
	})
}

// GUCContexts returns pg_settings.context for each of the names that exists.
func (c *YSQLClient) GUCContexts(ctx context.Context, names []string) (map[string]string, error) {
	rows, err := c.QueryContext(ctx, "SELECT name, context FROM pg_settings WHERE name = ANY($1)", names)
	if err != nil {
		return nil, err
	}
	defer rows.Close() // nolint:errcheck

	out := map[string]string{}
	for rows.Next() {
		var n, v string
		if err := rows.Scan(&n, &v); err != nil {
			return nil, err
		}
		out[n] = v
	}
	return out, rows.Err()
}

// CheckDbReadWrite writes a row into kubedb_write_check and reads it back.
func (c *YSQLClient) CheckDbReadWrite(ctx context.Context) error {
	if _, err := c.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS kubedb_write_check (id int PRIMARY KEY, written_at timestamptz)`); err != nil {
		return fmt.Errorf("failed to create the table kubedb_write_check: %w", err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := c.ExecContext(ctx, `INSERT INTO kubedb_write_check (id, written_at) VALUES (1, $1) ON CONFLICT (id) DO UPDATE SET written_at = EXCLUDED.written_at`, now); err != nil {
		return fmt.Errorf("failed to write into kubedb_write_check: %w", err)
	}
	var got time.Time
	if err := c.QueryRowContext(ctx, `SELECT written_at FROM kubedb_write_check WHERE id = 1`).Scan(&got); err != nil {
		return fmt.Errorf("failed to read from kubedb_write_check: %w", err)
	}
	if !got.Equal(now) {
		return fmt.Errorf("read %v from kubedb_write_check, wrote %v", got, now)
	}
	return nil
}

// Ping runs SELECT now() FROM system.local.
func (c *YCQLClient) Ping(ctx context.Context) error {
	var now time.Time
	return c.Query("SELECT now() FROM system.local").WithContext(ctx).Scan(&now)
}

// AlterRolePassword runs ALTER ROLE "<role>" WITH PASSWORD = '<password>'.
func (c *YCQLClient) AlterRolePassword(ctx context.Context, role, password string) error {
	return c.Query(AlterRolePasswordCQL(role, password)).WithContext(ctx).Exec()
}

// AlterRolePasswordSQL is the YSQL statement that sets the password of a role.
func AlterRolePasswordSQL(role, password string) string {
	return fmt.Sprintf("ALTER ROLE %s PASSWORD %s", pgx.Identifier{role}.Sanitize(), QuoteLiteral(password))
}

// AlterRolePasswordCQL is the YCQL statement that sets the password of a role.
func AlterRolePasswordCQL(role, password string) string {
	return fmt.Sprintf("ALTER ROLE %q WITH PASSWORD = %s", role, QuoteLiteral(password))
}

// QuoteLiteral quotes s as a string literal. Both YSQL (standard_conforming_strings is on) and YCQL
// escape a quote by doubling it.
func QuoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
