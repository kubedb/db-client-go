package neo4j

import (
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type Client struct {
	neo4jdriver.DriverWithContext
}
