package mariadb

import (
	"database/sql"
	"github.com/go-xorm/xorm"
)

type Client struct {
	*sql.DB
}

type XormClient struct {
	*xorm.Engine
}
