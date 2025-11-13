package weaviate

import (
	weaviate "github.com/weaviate/weaviate-go-client/v5/weaviate"
)

type Client struct {
	*weaviate.Client
}
