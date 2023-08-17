/*
Copyright AppsCode Inc. and Contributors

Licensed under the AppsCode Community License 1.0.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://github.com/appscode/licenses/raw/1.0.0/AppsCode-Community-1.0.0.md

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
	"strings"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	esv5 "github.com/elastic/go-elasticsearch/v5"
	"github.com/elastic/go-elasticsearch/v5/esapi"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
)

var _ ESClient = &ESClientV5{}

type ESClientV5 struct {
	client *esv5.Client
}

func (es *ESClientV5) ClusterHealthInfo() (map[string]interface{}, error) {
	return nil, nil
}

func (es *ESClientV5) NodesStats() (map[string]interface{}, error) {
	// Todo: need to implement for version 5
	return nil, nil
}

// GetIndicesInfo will return the indices info of an Elasticsearch database
func (es *ESClientV5) GetIndicesInfo() ([]interface{}, error) {
	return nil, nil
}

func (es *ESClientV5) ClusterStatus() (string, error) {
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
func (es *ESClientV5) SyncCredentialFromSecret(secret *core.Secret) error {
	return nil
}

func (es *ESClientV5) GetClusterWriteStatus(ctx context.Context, db *api.Elasticsearch) error {
	return nil
}

func (es *ESClientV5) GetClusterReadStatus(ctx context.Context, db *api.Elasticsearch) error {
	return nil
}

func (es *ESClientV5) GetTotalDiskUsage(ctx context.Context) (string, error) {
	return "", nil
}

func (es *ESClientV5) GetDBUserRole(ctx context.Context) (error, bool) {
	return errors.New("not supported in es version 5"), false
}

func (es *ESClientV5) CreateDBUserRole(ctx context.Context) error {
	return errors.New("not supported in es version 5")
}

func (es *ESClientV5) CreateIndex(_index string) error {
	reqCreateIndex := esapi.IndicesCreateRequest{
		Index:  _index,
		Pretty: true,
		Human:  true,
	}

	res, err := reqCreateIndex.Do(context.Background(), es.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return decodeError(res.Body, res.StatusCode)
	}

	return nil
}

func (es *ESClientV5) CountData(_index string) (int, error) {
	req := esapi.CountRequest{
		Index: []string{_index},
	}

	res, err := req.Do(context.Background(), es.client)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return 0, decodeError(res.Body, res.StatusCode)
	}

	var response map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return 0, err
	}

	count := int(response["count"].(int))
	//fmt.Printf("Number of documents in index %s: %d\n", _index, count)
	return count, nil
}

func (es *ESClientV5) DeleteIndex(_index string) error {
	req := esapi.IndicesDeleteRequest{
		Index: []string{_index},
	}

	res, err := req.Do(context.Background(), es.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return decodeError(res.Body, res.StatusCode)
	}

	return nil
}

func (es *ESClientV5) PutData(_index, _id string, data map[string]interface{}) error {
	var b strings.Builder
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return errors.Wrap(err, "failed to Marshal data")
	}
	b.Write(dataBytes)

	req := esapi.CreateRequest{
		Index:      _index,
		DocumentID: _id,
		Body:       strings.NewReader(b.String()),
		Pretty:     true,
		Human:      true,
	}

	res, err := req.Do(context.Background(), es.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return decodeError(res.Body, res.StatusCode)
	}
	return nil
}
