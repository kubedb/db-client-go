package elasticsearch

import (
	"encoding/json"
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

//func (es *ESClientV8) GetClusterWriteStatus(ctx context.Context) error {
//
//	// Build the request body.
//	reqBody := map[string]string{
//		"managed_by": "kubedb",
//	}
//	body, err2 := json.Marshal(reqBody)
//	if err2 != nil {
//		return err2
//	}
//
//	res, err := esapi.BulkRequest{
//		Index:  "kubedb_system",
//		Body:   strings.NewReader(string(body)),
//		Pretty: true,
//	}.Do(ctx, es.client.Transport)
//
//	defer func(Body io.ReadCloser) {
//		err = Body.Close()
//		if err != nil {
//			klog.Errorf("failed to close auth response body", err)
//		}
//	}(res.Body)
//
//	return errors.New("CredSyncFailed")
//}
