package elasticsearch

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	kutil "kmodules.xyz/client-go"
)

var _ ESClient = &OSClientV1{}

type OSClientV1 struct {
	client *opensearch.Client
}

func (os *OSClientV1) ClusterHealthInfo() (map[string]interface{}, error) {
	return nil, nil
}

func (os *OSClientV1) NodesStats() (map[string]interface{}, error) {
	return nil, nil
}

// GetIndicesInfo will return the indices' info of an Elasticsearch database
func (os *OSClientV1) GetIndicesInfo() ([]interface{}, error) {
	return nil, nil
}

func (os *OSClientV1) ClusterStatus() (string, error) {
	res, err := os.client.Cluster.Health(
		os.client.Cluster.Health.WithPretty(),
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

func (os *OSClientV1) SyncCredentialFromSecret(secret *core.Secret) error {
	return nil
}

func (os *OSClientV1) GetClusterWriteStatus(ctx context.Context, db *api.Elasticsearch) error {
	// Build the request body.
	indexReq := map[string]map[string]string{
		"index": {
			"_id":   "info",
			"_type": "_doc",
		},
	}
	reqBody := map[string]interface{}{
		"Labels": db.OffshootLabels(),
		"Metadata": map[string]interface{}{
			"name":              db.GetName(),
			"Namespace":         db.GetNamespace(),
			"Generation":        strconv.FormatInt(db.GetGeneration(), 10),
			"uid":               string(db.GetUID()),
			"ResourceVersion":   db.GetResourceVersion(),
			"creationTimestamp": db.GetCreationTimestamp().String(),
			"annotations":       db.GetAnnotations(),
		},
	}
	index, err1 := json.Marshal(indexReq)
	if err1 != nil {
		return err1
	}
	body, err2 := json.Marshal(reqBody)
	if err2 != nil {
		return err2
	}

	res, err3 := opensearchapi.BulkRequest{
		Index:  "kubedb-system",
		Body:   strings.NewReader(strings.Join([]string{string(index), string(body)}, "\n") + "\n"),
		Pretty: true,
	}.Do(ctx, os.client.Transport)

	defer func(res *opensearchapi.Response) {
		if res != nil {
			err3 = res.Body.Close()
			if err3 != nil {
				klog.Errorf("failed to close write request response body", err3)
			}
		}
	}(res)

	if err3 != nil {
		klog.Infoln("Failed to check", db.Name, "write Access", err3)
		return err3
	}

	responseBody := make(map[string]interface{})
	if err4 := json.NewDecoder(res.Body).Decode(&responseBody); err4 != nil {
		return errors.Wrap(err4, "failed to parse the response body")
	}
	if value, ok := responseBody["errors"]; ok {
		if strValue, ok := value.(bool); ok {
			if !strValue {
				return nil
			}
			return errors.New("DBWriteCheckFailed")
		}
		return errors.New("failed to convert response to string")
	}

	if !res.IsError() {
		return nil
	}

	klog.Infoln("DB Write Request Failed with status code ", res.StatusCode)
	return errors.New("DBWriteCheckFailed")
}

func (os *OSClientV1) GetClusterReadStatus(ctx context.Context, db *api.Elasticsearch) error {
	res, err := opensearchapi.GetRequest{
		Index:      "kubedb-system",
		DocumentID: "info",
	}.Do(ctx, os.client.Transport)

	defer func(res *opensearchapi.Response) {
		if res != nil {
			err = res.Body.Close()
			if err != nil {
				klog.Errorf("failed to close read request response body", err)
			}
		}
	}(res)

	if err != nil {
		klog.Infoln("Failed to check", db.Name, "read Access", err)
		return err
	}

	if !res.IsError() {
		return nil
	}

	if res.StatusCode == http.StatusNotFound {
		return kutil.ErrNotFound
	}

	klog.Infoln("DB Read request failed with status code ", res.StatusCode)
	return errors.New("DBReadCheckFailed")
}
