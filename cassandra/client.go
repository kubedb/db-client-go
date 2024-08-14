package cassandra

import (
	"github.com/gocql/gocql"
)

type Client struct {
	*gocql.Session
}
