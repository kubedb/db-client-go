package db2

import (
	"database/sql"
)

type Client struct {
	*sql.DB
}
