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
	"os"
	"strings"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	esv8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	kutil "kmodules.xyz/client-go"
)

var _ ESClient = &ESClientV8{}

type ESClientV8 struct {
	client *esv8.Client
}

func (es *ESClientV8) ClusterHealthInfo() (map[string]interface{}, error) {
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

func (es *ESClientV8) NodesStats() (map[string]interface{}, error) {
	req := esapi.NodesStatsRequest{
		Pretty: true,
		Human:  true,
	}

	resp, err := req.Do(context.Background(), es.client)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	nodesStats := make(map[string]interface{})
	if err := json.NewDecoder(resp.Body).Decode(&nodesStats); err != nil {
		return nil, fmt.Errorf("failed to deserialize the response: %v", err)
	}

	return nodesStats, nil
}

// GetIndicesInfo will return the indices' info of an Elasticsearch database
func (es *ESClientV8) GetIndicesInfo() ([]interface{}, error) {
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

func (es *ESClientV8) ClusterStatus() (string, error) {
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

func (es *ESClientV8) SyncCredentialFromSecret(secret *core.Secret) error {
	// get auth creds from secret
	var username, password string
	if value, ok := secret.Data[core.BasicAuthUsernameKey]; ok {
		username = string(value)
	} else {
		return errors.New("username is missing")
	}
	if value, ok := secret.Data[core.BasicAuthPasswordKey]; ok {
		password = string(value)
	} else {
		return errors.New("password is missing")
	}

	// Build the request body.
	reqBody := map[string]string{
		"password": password,
	}
	body, err2 := json.Marshal(reqBody)
	if err2 != nil {
		return err2
	}

	// send change password request via _security/user/username/_password api
	// use admin client to make request
	req := esapi.SecurityChangePasswordRequest{
		Body:     strings.NewReader(string(body)),
		Username: username,
		Pretty:   true,
	}

	res, err := req.Do(context.Background(), es.client.Transport)
	if err != nil {
		klog.Errorf("failed to send change password request for", username)
		return err
	}

	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			klog.Errorf("failed to close auth response body", err)
		}
	}(res.Body)

	if !res.IsError() {
		klog.V(5).Infoln(username, "user credentials successfully synced")
		return nil
	}

	klog.V(5).Infoln("Failed to sync", username, "credentials")
	return errors.New("CredSyncFailed")
}

func (es *ESClientV8) GetClusterWriteStatus(ctx context.Context, db *api.Elasticsearch) error {
	// Build the request index & request body
	// send the db specs as body
	indexBody := WriteRequestIndexBody{
		ID: writeRequestID,
	}

	indexReq := WriteRequestIndex{indexBody}
	ReqBody := db.Spec

	// encode the request index & request body
	index, err1 := json.Marshal(indexReq)
	if err1 != nil {
		return errors.Wrap(err1, "Failed to encode index for performing write request")
	}
	body, err2 := json.Marshal(ReqBody)
	if err2 != nil {
		return errors.Wrap(err2, "Failed to encode request body for performing write request")
	}

	// make write request & fetch response
	// check for write request failure & error from response body
	// Bulk API Performs multiple indexing or delete operations in a single API call
	// This reduces overhead and can greatly increase indexing speed it Indexes the specified document
	// If the document exists, replaces the document and increments the version
	res, err3 := esapi.BulkRequest{
		Index:  writeRequestIndex,
		Body:   strings.NewReader(strings.Join([]string{string(index), string(body)}, "\n") + "\n"),
		Pretty: true,
	}.Do(ctx, es.client.Transport)
	if err3 != nil {
		return errors.Wrap(err3, "Failed to perform write request")
	}
	if res.IsError() {
		return errors.New(fmt.Sprintf("Failed to get response from write request with error statuscode %d", res.StatusCode))
	}

	defer func(res *esapi.Response) {
		if res != nil {
			err3 = res.Body.Close()
			if err3 != nil {
				klog.Errorf("Failed to close write request response body", err3)
			}
		}
	}(res)

	responseBody := make(map[string]interface{})
	if err4 := json.NewDecoder(res.Body).Decode(&responseBody); err4 != nil {
		return errors.Wrap(err4, "Failed to decode response from write request")
	}

	// Parse the responseBody to check if write operation failed after request being successful
	// `errors` field(boolean) in the json response becomes true if there's and error caused, otherwise it stays nil
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

func (es *ESClientV8) GetClusterReadStatus(ctx context.Context, db *api.Elasticsearch) error {
	// Perform a read request in writeRequestIndex/writeRequestID (kubedb-system/info) API
	// Handle error specifically if index has not been created yet
	res, err := esapi.GetRequest{
		Index:      writeRequestIndex,
		DocumentID: writeRequestID,
	}.Do(ctx, es.client.Transport)
	if err != nil {
		return errors.Wrap(err, "Failed to perform read request")
	}

	defer func(res *esapi.Response) {
		if res != nil {
			err = res.Body.Close()
			if err != nil {
				klog.Errorf("failed to close read request response body", err)
			}
		}
	}(res)

	if res.StatusCode == http.StatusNotFound {
		return kutil.ErrNotFound
	}
	if res.IsError() {
		return errors.New(fmt.Sprintf("Failed to get response from write request with error statuscode %d", res.StatusCode))
	}

	return nil
}

func (es *ESClientV8) GetTotalDiskUsage(ctx context.Context) (string, error) {
	// Perform a DiskUsageRequest to database to calculate store size of all the elasticsearch indices
	// primary purpose of this function is to provide operator calculated storage of interimVolumeTemplate while taking backup
	// Analyzing field disk usage is resource-intensive. To use the API, RunExpensiveTasks must be set to true. Defaults to false.
	// Get disk usage for all indices using "*" wildcard.
	flag := true
	res, err := esapi.IndicesDiskUsageRequest{
		Index:             diskUsageRequestIndex,
		Pretty:            true,
		Human:             true,
		RunExpensiveTasks: &flag,
		ExpandWildcards:   diskUsageRequestWildcards,
	}.Do(ctx, es.client.Transport)
	if err != nil {
		return "", errors.Wrap(err, "Failed to perform Disk Usage Request")
	}
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			klog.Errorf("failed to close response body from Disk Usage Request", err)
		}
	}(res.Body)

	// Parse the json response to get total storage used for all index
	totalDiskUsage, err := calculateDatabaseSize(res.Body)
	if err != nil {
		return "", errors.Wrap(err, "Failed to parse json response to get disk usage")
	}

	return totalDiskUsage, nil
}

func (es *ESClientV8) GetDBUserRole(ctx context.Context) (error, bool) {
	req := esapi.SecurityGetRoleRequest{
		Name: []string{"my_admin"},
	}
	res, err := req.Do(ctx, es.client.Transport)
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			klog.Errorf("failed to close response body from GetDBUser", err)
		}
	}(res.Body)
	fmt.Println("retrieved role------------------>", res.Body)
	if err != nil {
		fmt.Println("faced error while making request in getdbuserrole function")
		return err, false
	}
	if res.IsError() {
		return err, false
	}
	return nil, true
	//resBody, err := io.ReadAll(res.Body)
	//if err != nil {
	//	fmt.Println("faced error while converting io.readcloser to bytes")
	//	return false
	//}
	//fmt.Println("---------role we got ", string(resBody))
	//if string(resBody) == "{}" {
	//	return false
	//}
	//fmt.Println("---------------------existing role", string(resBody))
	//return true
}

func (es *ESClientV8) EnsureDBUserRole(ctx context.Context) error {
	err, flg := es.GetDBUserRole(ctx)
	if err != nil {
		return err
	}
	if !flg {
		map1 := map[string]interface{}{
			"cluster": []string{"all"},
			"indices": []map[string]interface{}{{
				"names":                    []string{"*"},
				"privileges":               []string{"read", "write"},
				"allow_restricted_indices": false,
			}},
			"applications": []map[string]interface{}{
				{
					"application": "kibana-.kibana",
					"privileges":  []string{"admin", "read"},
					"resources":   []string{"*"},
				},
			},
			"run_as": []string{},
			"transient_metadata": map[string]interface{}{
				"enabled": true,
			},
		}
		fmt.Println(map1)
		jsonStr, err := json.Marshal(map1)
		if err != nil {
			fmt.Printf("Error: %s", err.Error())
		} else {
			fmt.Println(string(jsonStr))
		}
		body := bytes.NewReader(jsonStr)
		req := esapi.SecurityPutRoleRequest{
			Name: "my_admin",
			Body: body,
		}

		res, err := req.Do(ctx, es.client.Transport)
		defer res.Body.Close()
		if err != nil {
			fmt.Println("faced error while making request in ensuredbuserrole function")
			os.Exit(1)
		}
		resBody, err := io.ReadAll(res.Body)
		fmt.Println("---------------------------newly added role ", string(resBody))
		if err != nil {
			fmt.Println("faced error while converting io.readcloser to bytes")
			os.Exit(1)
		}
	}
	return nil
}
