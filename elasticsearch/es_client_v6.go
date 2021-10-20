package elasticsearch

import (
	"encoding/json"

	esv6 "github.com/elastic/go-elasticsearch/v6"
	"github.com/pkg/errors"
)

type ESClientV6 struct {
	client *esv6.Client
}

func (es *ESClientV6) ClusterHealthInfo() (map[string]interface{}, error) {
	res, err := es.client.Cluster.Health(
		es.client.Cluster.Health.WithPretty(),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	response := make(map[string]interface{})
	if err2 := json.NewDecoder(res.Body).Decode(&response); err2 != nil {
		return nil, errors.Wrap(err2, "failed to parse the response body")
	}
	return response, nil
}

func (es *ESClientV6) NodesStats() (map[string]interface{}, error) {
	// todo: need to implement for version 6
	return nil, nil
}
