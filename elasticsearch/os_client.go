package elasticsearch

import (
	"context"
	"encoding/json"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	"github.com/opensearch-project/opensearch-go"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
)

var _ ESClient = &OSClient{}

type OSClient struct {
	client *opensearch.Client
}

func (es *OSClient) ClusterHealthInfo() (map[string]interface{}, error) {
	return nil, nil
}

func (es *OSClient) NodesStats() (map[string]interface{}, error) {
	return nil, nil
}

// GetIndicesInfo will return the indices' info of an Elasticsearch database
func (es *OSClient) GetIndicesInfo() ([]interface{}, error) {
	return nil, nil
}

func (es *OSClient) ClusterStatus() (string, error) {
	res, err := es.client.Cluster.Health(
		es.client.Cluster.Health.WithPretty(),
	)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	response := make(map[string]interface{})
	if err2 := json.NewDecoder(res.Body).Decode(&response); err2 != nil {
		return "", errors.Wrap(err2, "failed to parse the response body")
	}
	if value, ok := response["status"]; ok {
		if strValue, ok := value.(string); ok {
			return strValue, nil
		}
		return "", errors.New("failed to convert response to string")
	}
	return "", errors.New("status is missing")
}

func (es *OSClient) SyncCredentialFromSecret(secret *core.Secret) error {
	return nil
}

func (es *OSClient) GetClusterWriteStatus(ctx context.Context, db *api.Elasticsearch) error {
	return nil
}

func (es *OSClient) GetClusterReadStatus(ctx context.Context, db *api.Elasticsearch) error {
	return nil
}
