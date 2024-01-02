package singlestore

import (
	"database/sql"

	"xorm.io/xorm"
)

type Client struct {
	*sql.DB
}

type XormClient struct {
	*xorm.Engine
}
