package redis

import (
	rd "github.com/go-redis/redis/v8"
)

type Client struct {
	*rd.Client
}
