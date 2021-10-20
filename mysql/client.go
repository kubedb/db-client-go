package mysql

import (
	"github.com/go-xorm/xorm"
)

type Client struct {
	*xorm.Engine
}
