package clickhouse

import (
	"database/sql"
)

type Client struct {
	*sql.DB
}
