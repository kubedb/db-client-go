package hanadb

import (
	"database/sql"
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
