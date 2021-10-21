package postgres

import (
	"database/sql"
	"github.com/go-xorm/xorm"
)

type Client interface{}

type sqlClient struct {
	*sql.DB
	Client
}

type xormClient struct {
	*xorm.Engine
	Client
}
