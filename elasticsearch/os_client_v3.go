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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"text/template"

	dbapi "kubedb.dev/apimachinery/apis/kubedb/v1"

	osv3api "github.com/opensearch-project/opensearch-go/v3/opensearchapi"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	kutil "kmodules.xyz/client-go"
)

var _ ESClient = &OSClientV3{}

type OSClientV3 struct {
	client *osv3api.Client
}

func (os *OSClientV3) ClusterHealthInfo() (map[string]interface{}, error) {
	res, err := os.client.Cluster.Health(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	body := res.Inspect().Response.Body
	defer body.Close()

	response := make(map[string]interface{})
	if err2 := json.NewDecoder(body).Decode(&response); err2 != nil {
		return nil, errors.Wrap(err2, "failed to parse the response body")
	}
	return response, nil
}

func (os *OSClientV3) NodesStats() (map[string]interface{}, error) {
	res, err := os.client.Nodes.Stats(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	body := res.Inspect().Response.Body
	defer body.Close()

	nodesStats := make(map[string]interface{})
	if err := json.NewDecoder(body).Decode(&nodesStats); err != nil {
		return nil, fmt.Errorf("failed to deserialize the response: %v", err)
	}

	return nodesStats, nil
}

func (os *OSClientV3) DisableShardAllocation() error {
	bodyReader := strings.NewReader(DisableShardAllocation)

	res, err := os.client.Cluster.PutSettings(context.Background(), osv3api.ClusterPutSettingsReq{
		Body: bodyReader,
	})
	if err != nil {
		return err
	}

	respBody := res.Inspect().Response.Body
	defer respBody.Close()

	if res.Inspect().Response.StatusCode != http.StatusOK {
		return fmt.Errorf("received status code: %d", res.Inspect().Response.StatusCode)
	}

	return nil
}

func (os *OSClientV3) ShardStats() ([]ShardInfo, error) {
	res, err := os.client.Cat.Shards(context.Background(), &osv3api.CatShardsReq{
		Params: osv3api.CatShardsParams{
			Bytes: "b",
			H:     []string{"index", "shard", "prirep", "state", "unassigned.reason"},
		},
	})
	if err != nil {
		return nil, err
	}

	respBody := res.Inspect().Response.Body
	defer respBody.Close()

	body, err := io.ReadAll(respBody)
	if err != nil {
		fmt.Println("Error reading body:", err)
		return nil, err
	}

	var shardStats []ShardInfo
	err = json.Unmarshal(body, &shardStats)
	if err != nil {
		return nil, err
	}
	return shardStats, nil
}

func (os *OSClientV3) GetIndicesInfo() ([]interface{}, error) {
	res, err := os.client.Cat.Indices(context.Background(), &osv3api.CatIndicesReq{
		Params: osv3api.CatIndicesParams{
			Bytes: "b",
		},
	})
	if err != nil {
		return nil, err
	}

	body := res.Inspect().Response.Body
	defer body.Close()

	indicesInfo := make([]interface{}, 0)
	if err := json.NewDecoder(body).Decode(&indicesInfo); err != nil {
		return nil, fmt.Errorf("failed to deserialize the response: %v", err)
	}

	return indicesInfo, nil
}

func (os *OSClientV3) ClusterStatus() (string, error) {
	res, err := os.client.Cluster.Health(context.Background(), nil)
	if err != nil {
		return "", err
	}

	body := res.Inspect().Response.Body
	defer body.Close()

	response := make(map[string]interface{})
	if err2 := json.NewDecoder(body).Decode(&response); err2 != nil {
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

func (os *OSClientV3) GetClusterWriteStatus(ctx context.Context, db *dbapi.Elasticsearch) error {
	indexBody := WriteRequestIndexBody{
		ID: writeRequestID,
	}

	indexReq := WriteRequestIndex{indexBody}
	ReqBody := db.Spec

	index, err1 := json.Marshal(indexReq)
	if err1 != nil {
		return errors.Wrap(err1, "Failed to encode index for performing write request")
	}
	bodyData, err2 := json.Marshal(ReqBody)
	if err2 != nil {
		return errors.Wrap(err2, "Failed to encode request body for performing write request")
	}

	bodyReader := strings.NewReader(strings.Join([]string{string(index), string(bodyData)}, "\n") + "\n")

	res, err3 := os.client.Bulk(ctx, osv3api.BulkReq{
		Body: bodyReader,
		Params: osv3api.BulkParams{
			Pretty: true,
		},
		Index: writeRequestIndex,
	})
	if err3 != nil {
		return errors.Wrap(err3, "Failed to perform write request")
	}

	respBody := res.Inspect().Response.Body
	defer func() {
		if err := respBody.Close(); err != nil {
			klog.Errorf("Failed to close write request response body, reason: %s", err)
		}
	}()

	if res.Inspect().Response.StatusCode >= 400 {
		return fmt.Errorf("failed to get response from write request with error statuscode %d", res.Inspect().Response.StatusCode)
	}

	responseBody := make(map[string]interface{})
	if err4 := json.NewDecoder(respBody).Decode(&responseBody); err4 != nil {
		return errors.Wrap(err4, "Failed to decode response from write request")
	}

	if value, ok := responseBody["errors"]; ok {
		if strValue, ok := value.(bool); ok {
			if !strValue {
				return nil
			}
			return errors.Errorf("Write request responded with error, %v", responseBody)
		}
		return errors.New("Failed to parse value for `errors` in response from write request")
	}
	return errors.New("Failed to parse key `errors` in response from write request")
}

func (os *OSClientV3) GetClusterReadStatus(ctx context.Context, db *dbapi.Elasticsearch) error {
	res, err := os.client.Document.Get(ctx, osv3api.DocumentGetReq{
		Index:      writeRequestIndex,
		DocumentID: writeRequestID,
	})
	if err != nil {
		return errors.Wrap(err, "Failed to perform read request")
	}

	body := res.Inspect().Response.Body
	defer func() {
		if err := body.Close(); err != nil {
			klog.Errorf("failed to close read request response body, reason: %s", err)
		}
	}()

	statusCode := res.Inspect().Response.StatusCode
	if statusCode == http.StatusNotFound {
		return kutil.ErrNotFound
	}
	if statusCode >= 400 {
		return fmt.Errorf("failed to get response from read request with error statuscode %d", statusCode)
	}

	return nil
}

func (os *OSClientV3) GetTotalDiskUsage(ctx context.Context) (string, error) {
	// Manual POST request to /{index}/_disk_usage?run_expensive_tasks=true&expand_wildcards=all
	// Note: diskUsageRequestIndex is likely "*" or specific indices
	path := "/" + diskUsageRequestIndex + "/_disk_usage"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, path, nil)
	if err != nil {
		return "", errors.Wrap(err, "Failed to create Disk Usage Request")
	}

	// Set query parameters
	q := req.URL.Query()
	q.Add("run_expensive_tasks", "true")
	q.Add("expand_wildcards", diskUsageRequestWildcards) // e.g., "all"
	// Optional: q.Add("pretty", "true")
	// q.Add("human", "true")
	req.URL.RawQuery = q.Encode()

	// Perform using low-level transport
	res, err := os.client.Client.Transport.Perform(req)
	if err != nil {
		return "", errors.Wrap(err, "Failed to perform Disk Usage Request")
	}
	defer func() {
		if closeErr := res.Body.Close(); closeErr != nil {
			klog.Errorf("failed to close response body from Disk Usage Request, reason: %s", closeErr)
		}
	}()

	if res.StatusCode != http.StatusOK {
		// Read error body for debugging if needed
		return "", fmt.Errorf("Disk Usage Request failed with status code: %d", res.StatusCode)
	}

	// Parse the json response to get total storage used for all index
	totalDiskUsage, err := calculateDatabaseSize(res.Body)
	if err != nil {
		return "", errors.Wrap(err, "Failed to parse json response to get disk usage")
	}

	return totalDiskUsage, nil
}

func (os *OSClientV3) SyncCredentialFromSecret(secret *core.Secret) error {
	return nil
}

func (os *OSClientV3) GetDBUserRole(ctx context.Context) (error, bool) {
	return errors.New("not supported in os version 3"), false
}

func (os *OSClientV3) CreateDBUserRole(ctx context.Context) error {
	return errors.New("not supported in os version 3")
}

func (os *OSClientV3) IndexExistsOrNot(index string) error {
	// 1. Remove the '&' (Pass by value, not pointer)
	// 2. Use 'Indices' as the field name
	// 3. Ensure it is a slice: []string{index}
	res, err := os.client.Indices.Exists(context.Background(), osv3api.IndicesExistsReq{
		Indices: []string{index},
	})
	if err != nil {
		klog.Errorf("failed to get response while checking index existence: %v", err)
		return err
	}

	// Always close the body to prevent memory leaks
	if res.Body != nil {
		defer res.Body.Close()
	}

	// In OpenSearch 'Exists' APIs:
	// 200 OK = Index exists
	// 404 Not Found = Index does not exist
	if res.StatusCode == 404 {
		return fmt.Errorf("index %s does not exist", index)
	}

	if res.IsError() {
		return fmt.Errorf("error checking index existence, status: %s", res.Status())
	}

	return nil
}

func (os *OSClientV3) CreateIndex(index string) error {
	res, err := os.client.Indices.Create(context.Background(), osv3api.IndicesCreateReq{
		Index: index,
	})
	if err != nil {
		klog.Errorf("failed to apply create index request, reason: %s", err)
		return err
	}

	body := res.Inspect().Response.Body
	defer func() {
		if err := body.Close(); err != nil {
			klog.Errorf("failed to close response body for creating index, reason: %s", err)
		}
	}()

	if res.Inspect().Response.StatusCode >= 400 {
		klog.Errorf("creating index failed with statuscode %d", res.Inspect().Response.StatusCode)
		return errors.New("failed to create index")
	}

	return nil
}

func (os *OSClientV3) DeleteIndex(index string) error {
	res, err := os.client.Indices.Delete(context.Background(), osv3api.IndicesDeleteReq{
		Indices: []string{index},
	})
	if err != nil {
		return err
	}

	// In V3 Typed API, access the underlying HTTP response via Inspect()
	resp := res.Inspect().Response
	if resp.Body != nil {
		defer resp.Body.Close()
	}

	if resp.StatusCode >= 400 {
		klog.Errorf("failed to delete index %s, status code: %d", index, resp.StatusCode)
		return fmt.Errorf("failed to delete index, status: %d", resp.StatusCode)
	}

	return nil
}

func (os *OSClientV3) PutData(index, id string, data map[string]interface{}) error {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return errors.Wrap(err, "failed to Marshal data")
	}

	res, err := os.client.Document.Create(context.Background(), osv3api.DocumentCreateReq{
		Index:      index,
		DocumentID: id,
		Body:       bytes.NewReader(dataBytes),
	})
	if err != nil {
		klog.Errorf("failed to put data in the index, reason: %s", err)
		return err
	}

	body := res.Inspect().Response.Body
	defer func() {
		if err := body.Close(); err != nil {
			klog.Errorf("failed to close response body for putting data in the index, reason: %s", err)
		}
	}()

	if res.Inspect().Response.StatusCode >= 400 {
		klog.Errorf("failed to put data in an index with statuscode %d", res.Inspect().Response.StatusCode)
		return errors.New("failed to put data in an index")
	}
	return nil
}

func (os *OSClientV3) ReEnableShardAllocation() error {
	bodyReader := strings.NewReader(ReEnableShardAllocation)

	res, err := os.client.Cluster.PutSettings(context.Background(), osv3api.ClusterPutSettingsReq{
		Body: bodyReader,
	})
	if err != nil {
		return err
	}

	respBody := res.Inspect().Response.Body
	defer respBody.Close()

	if res.Inspect().Response.StatusCode != http.StatusOK {
		return fmt.Errorf("received status code: %d", res.Inspect().Response.StatusCode)
	}

	return nil
}

func (os *OSClientV3) CheckVersion() (string, error) {
	res, err := os.client.Info(context.Background(), nil)
	if err != nil {
		return "", err
	}

	body := res.Inspect().Response.Body
	defer body.Close()

	nodeInfo := new(Info)
	if err := json.NewDecoder(body).Decode(&nodeInfo); err != nil {
		return "", errors.Wrap(err, "failed to deserialize the response")
	}

	if nodeInfo.Version.Number == "" {
		return "", errors.New("elasticsearch version is empty")
	}

	return nodeInfo.Version.Number, nil
}

func (os *OSClientV3) GetClusterStatus() (string, error) {
	res, err := os.client.Cluster.Health(context.Background(), nil)
	if err != nil {
		return "", err
	}

	body := res.Inspect().Response.Body
	defer body.Close()

	response := make(map[string]interface{})
	if err2 := json.NewDecoder(body).Decode(&response); err2 != nil {
		return "", errors.Wrap(err2, "failed to parse the response body")
	}
	if value, ok := response["status"]; ok {
		return value.(string), nil
	}
	return "", errors.New("status is missing")
}

func (os *OSClientV3) CountIndex() (int, error) {
	// 1. Pass by VALUE (remove &)
	// 2. Use 'Indices' (plural) as the field name
	res, err := os.client.Indices.Get(context.Background(), osv3api.IndicesGetReq{
		Indices: []string{"_all"},
	})
	if err != nil {
		return 0, err
	}

	// 3. Access metadata via Inspect()
	resp := res.Inspect().Response
	if resp.Body != nil {
		defer resp.Body.Close()
	}

	if resp.StatusCode >= 400 {
		return 0, fmt.Errorf("failed to get indices, status code: %d", resp.StatusCode)
	}

	// 4. Decode the result map
	// OpenSearch returns a JSON object where each key is an index name
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to parse indices response: %w", err)
	}

	// The count of top-level keys is the number of indices
	return len(result), nil
}

func (os *OSClientV3) GetData(_index, _type, _id string) (map[string]interface{}, error) {
	res, err := os.client.Document.Get(context.Background(), osv3api.DocumentGetReq{
		Index:      _index,
		DocumentID: _id,
	})
	if err != nil {
		return nil, err
	}

	body := res.Inspect().Response.Body
	defer body.Close()

	if res.Inspect().Response.StatusCode >= 400 {
		return nil, fmt.Errorf("received status code: %d", res.Inspect().Response.StatusCode)
	}

	response := make(map[string]interface{})
	if err2 := json.NewDecoder(body).Decode(&response); err2 != nil {
		return nil, errors.Wrap(err2, "failed to parse the response body")
	}

	return response, nil
}

func (os *OSClientV3) CountNodes() (int64, error) {
	res, err := os.client.Nodes.Info(context.Background(), nil)
	if err != nil {
		return -1, err
	}

	body := res.Inspect().Response.Body
	defer body.Close()

	nodeInfo := new(NodeInfo)
	if err := json.NewDecoder(body).Decode(&nodeInfo); err != nil {
		return -1, errors.Wrap(err, "failed to deserialize the response")
	}

	if nodeInfo.Nodes.Total == "" {
		return -1, errors.New("Node count is empty")
	}

	return nodeInfo.Nodes.Total.Int64()
}

func (os *OSClientV3) CountData(index string) (int, error) {
	// 1. Use the Indices namespace
	// 2. Use '&' as this specific API requires a pointer
	// 3. Use 'Indices' (plural) slice
	res, err := os.client.Indices.Count(context.Background(), &osv3api.IndicesCountReq{
		Indices: []string{index},
	})
	if err != nil {
		klog.Errorf("failed to perform count request: %v", err)
		return 0, err
	}

	// 4. Access the HTTP response metadata via Inspect()
	resp := res.Inspect().Response
	if resp.Body != nil {
		defer resp.Body.Close()
	}

	// 5. Check for HTTP errors
	if resp.StatusCode >= 400 {
		klog.Errorf("count API returned error status: %d", resp.StatusCode)
		return 0, fmt.Errorf("failed to count data in index %s, status: %d", index, resp.StatusCode)
	}

	// 6. Manually decode the "count" field from the JSON body.
	// This is the safest way if the SDK's built-in field name is unclear.
	var result struct {
		Count int `json:"count"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		klog.Errorf("failed to decode count JSON: %v", err)
		return 0, fmt.Errorf("failed to parse count response: %w", err)
	}

	return result.Count, nil
}

func (os *OSClientV3) AddVotingConfigExclusions(nodes []string) error {
	nodeNames := strings.Join(nodes, ",")

	// 1. Remove '&' (Pass by value)
	res, err := os.client.Cluster.PostVotingConfigExclusions(context.Background(), osv3api.ClusterPostVotingConfigExclusionsReq{
		Params: osv3api.ClusterPostVotingConfigExclusionsParams{
			NodeNames: nodeNames,
		},
	})
	if err != nil {
		return err
	}

	// 2. Access .Body and .StatusCode directly (No .Inspect())
	if res.Body != nil {
		defer res.Body.Close()
	}

	if res.StatusCode >= 400 {
		return fmt.Errorf("failed with response.StatusCode: %d", res.StatusCode)
	}

	return nil
}

func (os *OSClientV3) DeleteVotingConfigExclusions() error {
	// Remove '&' if the compiler throws a similar error here
	res, err := os.client.Cluster.DeleteVotingConfigExclusions(
		context.Background(),
		osv3api.ClusterDeleteVotingConfigExclusionsReq{},
	)
	if err != nil {
		return err
	}

	// Access directly
	if res.Body != nil {
		defer res.Body.Close()
	}

	if res.StatusCode > 299 {
		return fmt.Errorf("failed with response.StatusCode: %d", res.StatusCode)
	}

	return nil
}

func (os *OSClientV3) ExcludeNodeAllocation(nodes []string) error {
	list := strings.Join(nodes, ",")
	var bodyBuf bytes.Buffer
	t, err := template.New("").Parse(ExcludeNodeAllocation)
	if err != nil {
		return errors.Wrap(err, "failed to parse the template")
	}

	if err := t.Execute(&bodyBuf, list); err != nil {
		return err
	}

	res, err := os.client.Cluster.PutSettings(context.Background(), osv3api.ClusterPutSettingsReq{
		Body: bytes.NewReader(bodyBuf.Bytes()),
	})
	if err != nil {
		return err
	}

	body := res.Inspect().Response.Body
	defer body.Close()

	if res.Inspect().Response.StatusCode >= 400 {
		return fmt.Errorf("received status code: %d", res.Inspect().Response.StatusCode)
	}

	return nil
}

func (os *OSClientV3) DeleteNodeAllocationExclusion() error {
	bodyReader := strings.NewReader(DeleteNodeAllocationExclusion)

	res, err := os.client.Cluster.PutSettings(context.Background(), osv3api.ClusterPutSettingsReq{
		Body: bodyReader,
	})
	if err != nil {
		return err
	}

	body := res.Inspect().Response.Body
	defer body.Close()

	if res.Inspect().Response.StatusCode >= 400 {
		return fmt.Errorf("received status code: %d", res.Inspect().Response.StatusCode)
	}

	return nil
}

func (os *OSClientV3) GetUsedDataNodes() ([]string, error) {
	res, err := os.client.Cat.Shards(context.Background(), &osv3api.CatShardsReq{
		Params: osv3api.CatShardsParams{
			H: []string{"index,node"},
		},
	})
	if err != nil {
		return nil, err
	}

	body := res.Inspect().Response.Body
	defer body.Close()

	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}

	var list []IndexDistribution
	err = json.Unmarshal(data, &list)
	if err != nil {
		return nil, err
	}

	var nodes []string
	for _, value := range list {
		if !IsIgnorableIndex(value.Index) {
			nodes = append(nodes, value.Node)
		}
	}
	return nodes, nil
}

func (os *OSClientV3) AssignedShardsSize(node string) (int64, error) {
	res, err := os.client.Nodes.Stats(context.Background(), &osv3api.NodesStatsReq{
		NodeID: []string{node},
	})
	if err != nil {
		return 0, err
	}

	body := res.Inspect().Response.Body
	defer body.Close()

	response := new(NodesStats)
	if err := json.NewDecoder(body).Decode(&response); err != nil {
		return 0, err
	}

	for _, value := range response.Nodes {
		return value.Indices.Store.SizeInBytes, nil
	}
	return 0, errors.New("empty response body")
}

func (os *OSClientV3) EnableUpgradeModeML() error {
	return nil
}

func (os *OSClientV3) DisableUpgradeModeML() error {
	return nil
}
