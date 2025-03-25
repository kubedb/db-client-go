package memcached

import (
	"github.com/kubedb/gomemcache/memcache"
)

type Client struct {
	*memcache.Client
}
