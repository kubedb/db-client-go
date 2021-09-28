package mongodb

import (
	"go.mongodb.org/mongo-driver/mongo"
)

type Client struct {
	*mongo.Client
}
