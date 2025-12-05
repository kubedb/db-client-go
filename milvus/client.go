package milvus

import (
	"context"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

type MilvusClient struct {
	*milvusclient.Client
}

func (mc *MilvusClient) Close(ctx context.Context) error {
	if mc.Client != nil {
		return mc.Client.Close(ctx)
	}
	return nil
}
