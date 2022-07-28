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
	"net/http"
	"strconv"
	"strings"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	esv7 "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	kutil "kmodules.xyz/client-go"
)

var _ ESClient = &ESClientV7{}

type ESClientV7 struct {
	client *esv7.Client
}

func (es *ESClientV7) ClusterHealthInfo() (map[string]interface{}, error) {
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

func (es *ESClientV7) NodesStats() (map[string]interface{}, error) {
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

// GetIndicesInfo will return the indices info of an Elasticsearch database
func (es *ESClientV7) GetIndicesInfo() ([]interface{}, error) {
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

func (es *ESClientV7) ClusterStatus() (string, error) {
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

func (es *ESClientV7) SyncCredentialFromSecret(secret *core.Secret) error {
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

func (es *ESClientV7) GetClusterWriteStatus(ctx context.Context, db *api.Elasticsearch) error {
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

	defer func(res *esapi.Response) {
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
			if strValue == false {
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

func (es *ESClientV7) GetClusterReadStatus(ctx context.Context, db *api.Elasticsearch) error {
	res, err := esapi.GetRequest{
		Index:      "kubedb-system",
		DocumentID: "info",
	}.Do(ctx, es.client.Transport)

	defer func(res *esapi.Response) {
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
