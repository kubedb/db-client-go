package qdrant

import "github.com/qdrant/go-client/qdrant"

type QdrantClient struct {
	*qdrant.Client
}

func (qc *QdrantClient) Close() error {
	if qc.Client != nil {
		return qc.Client.Close()
	}
	return nil
}
