package clickhouse_db_client

import (
	"database/sql"
)

type Client struct {
	*sql.DB
}
