/*
Copyright AppsCode Inc. and Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"k8s.io/klog/v2"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"strconv"
	"strings"

	esv6 "github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esapi"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
)

var _ ESClient = &ESClientV6{}

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

// GetIndicesInfo will return the indices info of an Elasticsearch database
func (es *ESClientV6) GetIndicesInfo() ([]interface{}, error) {
	req := esapi.CatIndicesRequest{
		Bytes:  "b", // will return resource size field into byte unit
		Format: "json",
		Pretty: true,
		Human:  true,
	}

	resp, err := req.Do(context.Background(), es.client)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	indicesInfo := make([]interface{}, 0)
	if err := json.NewDecoder(resp.Body).Decode(&indicesInfo); err != nil {
		return nil, fmt.Errorf("failed to deserialize the response: %v", err)
	}

	return indicesInfo, nil
}

func (es *ESClientV6) ClusterStatus() (string, error) {
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

// kibana_system, logstash_system etc. internal users
// are not supported for versions 6.x.x and,
// kibana, logstash can be accessed using elastic superuser
// so, sysncing is not required for other builtin users
func (es *ESClientV6) SyncCredentialFromSecret(secret *core.Secret) error {
	return nil
}

func (es *ESClientV6) GetClusterWriteStatus(ctx context.Context, db *api.Elasticsearch) error {

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

	res, err3 := esapi.BulkRequest{
		Index:  "kubedb-system",
		Body:   strings.NewReader(strings.Join([]string{string(index), string(body)}, "\n") + "\n"),
		Pretty: true,
	}.Do(ctx, es.client.Transport)

	defer func(Body io.ReadCloser) {
		err3 = Body.Close()
		if err3 != nil {
			klog.Errorf("failed to close write request response body", err3)
		}
	}(res.Body)

	if !res.IsError() {
		return nil
	}

	klog.Infoln(err3, "Failed to check", db.Name, "write Access")
	return errors.New("DBWriteCheckFailed")
}

func (es *ESClientV6) GetClusterReadStatus(ctx context.Context, db *api.Elasticsearch) error {
	res, err1 := esapi.GetRequest{
		Index:      "kubedb-system",
		DocumentID: "info",
	}.Do(ctx, es.client.Transport)

	defer func(Body io.ReadCloser) {
		err1 = Body.Close()
		if err1 != nil {
			klog.Errorf("failed to close read request response body", err1)
		}
	}(res.Body)

	if !res.IsError() {
		return nil
	}

	klog.Infoln("Failed to check", db.Name, "read Access")
	return errors.New("DBReadCheckFailed")
}
