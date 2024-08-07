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

package druid

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"time"

	druidgo "github.com/grafadruid/go-druid"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/pkg/errors"
	"k8s.io/klog/v2"
	health "kmodules.xyz/client-go/tools/healthchecker"
)

type Client struct {
	*druidgo.Client
}

type DruidTaskType int32

const (
	DruidIngestionTask DruidTaskType = 0
	DruidKillTask      DruidTaskType = 1
)

const (
	DruidHealthDataZero        = "0"
	DruidHealthDataOne         = "1"
	DruidHealthCheckDataSource = "kubedb-datasource"
)

func (c *Client) CloseDruidClient(hcs *health.HealthCard) {
	err := c.Close()
	if err != nil {
		klog.Error(err, "Failed to close druid middleManagers client")
		return
	}
	hcs.ClientClosed()
}

func IsDBConnected(druidClients []*Client) (bool, error) {
	// First, check the health of the nodes
	for _, druidClient := range druidClients {
		healthStatus, err := druidClient.CheckNodeHealth()
		if err != nil {
			klog.Error(err, "Failed to check node health")
			return false, err
		}
		// If the health of any node is false, no point of checking health of the remaining
		if !healthStatus {
			return false, nil
		}
	}

	// Check self-discovery status, i.e. indicating whether the node has received
	// a confirmation from the central node discovery mechanism (currently ZooKeeper) of the Druid cluster
	for _, druidClient := range druidClients {
		discoveryStatus, err := druidClient.CheckNodeDiscoveryStatus()
		if err != nil {
			klog.Error(err, "Failed to check node discovery status")
			return false, err
		}
		// If the health of any node is false, no point of checking health of the remaining
		if !discoveryStatus {
			return false, nil
		}
	}
	return true, nil
}

func (c *Client) CheckNodeHealth() (bool, error) {
	healthStatus, _, err := c.Common().Health()
	if err != nil {
		klog.Error(err, "Failed to check node health")
		return false, err
	}
	return bool(*healthStatus), err
}

func (c *Client) CheckNodeDiscoveryStatus() (bool, error) {
	discoveryStatus, _, err := c.Common().SelfDiscovered()
	if err != nil {
		klog.Error(err, "Failed to check node discovery status")
		return false, err
	}
	return discoveryStatus.SelfDiscovered, err
}

func (c *Client) CheckDataSourceExistence() (bool, error) {
	method := "POST"
	path := "druid/v2/sql"

	data := map[string]interface{}{
		"query": "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'kubedb-datasource'",
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return false, errors.Wrap(err, "failed to marshal json response")
	}
	rawMessage := json.RawMessage(jsonData)
	response, err := c.SubmitRequest(method, path, rawMessage)
	if err != nil {
		return false, err
	}

	exists, err := parseDatasourceExistenceQueryResponse(response)
	if err != nil {
		return false, errors.Wrap(err, "Failed to parse response of datasource existence request")
	}

	if err := closeResponse(response); err != nil {
		return exists, err
	}
	return exists, nil
}

func (c *Client) SubmitRequest(method, path string, opts interface{}) (*druidgo.Response, error) {
	res, err := c.NewRequest(method, path, opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to submit API request")
	}
	http := retryablehttp.NewClient()

	var b []byte
	buf := bytes.NewBuffer(b)
	http.Logger = log.New(buf, "", 0)

	resp, err := http.Do(res)
	if err != nil {
		return nil, err
	}
	response := &druidgo.Response{Response: resp}
	return response, nil
}

func parseDatasourceExistenceQueryResponse(res *druidgo.Response) (bool, error) {
	var responseBody []map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&responseBody); err != nil {
		return false, errors.Wrap(err, "failed to deserialize the response")
	}
	return len(responseBody) != 0, nil
}

func closeResponse(response *druidgo.Response) error {
	err := response.Body.Close()
	if err != nil {
		return errors.Wrap(err, "Failed to close the response body")
	}
	return nil
}

// CheckDBReadWriteAccess checks read and write access in the DB
// if there is an error
// flag == false, corresponds to write check error
// flag == true, corresponds to read check error
func CheckDBReadWriteAccess(druidCoordinatorsClient *Client, druidBrokersClient *Client, druidOverlordsClient *Client) (error, bool) {
	exist, err := druidBrokersClient.CheckDataSourceExistence()
	if err != nil {
		klog.Error(err, "Failed to check the existence of kubedb-datasource")
		return err, false
	}

	var oldData, newData string
	if exist {
		oldData, err = druidBrokersClient.GetData()
		if err != nil {
			klog.Error(err, "Failed to read datasource")
			return err, false
		}
		if oldData == DruidHealthDataZero {
			newData = DruidHealthDataOne
		} else {
			newData = DruidHealthDataZero
		}
	} else {
		// In the first iteration of health Check update coordinators config
		// to delete unused segments after 5 seconds of being leader
		err := druidCoordinatorsClient.updateCoordinatorsWaitBeforeDeletingConfig(500)
		if err != nil {
			return err, false
		}
		klog.V(5).Info("Successfully updated coordinators config to stop waiting before deleting segment")
		oldData = DruidHealthDataZero
		newData = DruidHealthDataOne
	}

	// Submit Ingestion Task and check status
	if err := druidOverlordsClient.SubmitTaskRecurrently(DruidIngestionTask, DruidHealthCheckDataSource, newData); err != nil {
		klog.Error(err, "Ingestion task failed")
		return err, true
	}

	if !exist {
		time.Sleep(5 * time.Second)
	}

	// Check if new data can be read
	if err := druidBrokersClient.checkDBReadAccess(oldData); err != nil {
		return err, true
	}

	// Drop the unused segments of previous health checks
	if err := druidOverlordsClient.SubmitTaskRecurrently(DruidKillTask, DruidHealthCheckDataSource, ""); err != nil {
		klog.Error(err, "Kill task for dropping unused segment failed")
		return err, true
	}
	return nil, false
}

func (c *Client) GetData() (string, error) {
	id, err := c.runSelectQuery()
	if err != nil {
		klog.Error(err, "Failed to query the datasource")
		return "", err
	}
	return id, nil
}

func (c *Client) runSelectQuery() (string, error) {
	method := "POST"
	path := "druid/v2/sql"

	data := map[string]interface{}{
		"query": "SELECT * FROM \"kubedb-datasource\"",
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal query json data")
	}
	rawMessage := json.RawMessage(jsonData)
	response, err := c.SubmitRequest(method, path, rawMessage)
	if err != nil {
		return "", err
	}
	if response == nil {
		return "", errors.New("response body is empty")
	}

	id, err := parseSelectQueryResponse(response, "id")
	if err != nil {
		return "", errors.Wrap(err, "failed to parse the response body")
	}

	if err := closeResponse(response); err != nil {
		return "", err
	}
	return id.(string), nil
}

func parseSelectQueryResponse(res *druidgo.Response, key string) (interface{}, error) {
	var responseBody []map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&responseBody); err != nil {
		return "", errors.Wrap(err, "failed to deserialize the response")
	}
	value := responseBody[0][key]
	return value, nil
}

func (c *Client) updateCoordinatorsWaitBeforeDeletingConfig(value int32) error {
	data := map[string]interface{}{
		"millisToWaitBeforeDeleting": value,
	}
	if err := c.updateCoordinatorDynamicConfig(data); err != nil {
		klog.Error(err, "Failed to update coordinator dynamic config")
		return err
	}
	return nil
}

func (c *Client) updateCoordinatorDynamicConfig(data map[string]interface{}) error {
	method := "POST"
	path := "druid/coordinator/v1/config"

	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	rawMessage := json.RawMessage(jsonData)

	response, err := c.SubmitRequest(method, path, rawMessage)
	if err != nil {
		return err
	}
	if err := closeResponse(response); err != nil {
		return err
	}
	return nil
}

func (c *Client) SubmitTaskRecurrently(taskType DruidTaskType, dataSource string, data string) error {
	taskID, err := c.submitTask(taskType, dataSource, data)
	if err != nil {
		klog.Error(err, "Failed to submit task")
		return err
	}

	var taskStatus bool
	for i := 0; i < 10; i++ {
		taskStatus, err = c.CheckTaskStatus(taskID)
		if err != nil {
			klog.Error(err, "Failed to check task status")
			return err
		}
		if taskStatus {
			klog.V(5).Info("Task successful")
			return nil
		}
		time.Sleep(6 * time.Second)
	}
	return errors.New("task status is failed")
}

func (c *Client) submitTask(taskType DruidTaskType, dataSource string, data string) (string, error) {
	var task string
	if taskType == DruidIngestionTask {
		task = GetIngestionTaskDefinition(dataSource, data)
	} else {
		task = GetKillTaskDefinition()
	}

	rawMessage := json.RawMessage(task)
	method := "POST"
	path := "druid/indexer/v1/task"

	response, err := c.SubmitRequest(method, path, rawMessage)
	if err != nil {
		return "", err
	}

	taskID, err := GetValueFromClusterResponse(response, "task")
	if err != nil {
		return "", errors.Wrap(err, "failed to parse response of task api request")
	}
	if err = closeResponse(response); err != nil {
		return "", err
	}
	return fmt.Sprintf("%v", taskID), nil
}

func GetValueFromClusterResponse(res *druidgo.Response, key string) (interface{}, error) {
	responseBody := make(map[string]interface{})
	if err := json.NewDecoder(res.Body).Decode(&responseBody); err != nil {
		return "", errors.Wrap(err, "failed to deserialize the response")
	}
	value := responseBody[key]
	return value, nil
}

func GetIngestionTaskDefinition(dataSource string, data string) string {
	task := `{
		"type": "index_parallel",
	  "spec": {
	    "ioConfig": {
	      "type": "index_parallel",
	      "inputSource": {
	        "type": "inline",
	        "data": "{\"id\": \"%s\", \"name\": \"%s\", \"time\": \"2015-09-12T00:46:58.771Z\"}"
	      },
	      "inputFormat": {
	        "type": "json"
	      }
	    },
	    "tuningConfig": {
	      "type": "index_parallel",
	      "partitionsSpec": {
	        "type": "dynamic"
	      }
	    },
	    "dataSchema": {
	      "dataSource": "%s",
	      "timestampSpec": {
	        "column": "time",
	        "format": "iso"
	      },
	      "dimensionsSpec": {
	        "dimensions": ["id", "name", "time"]
	      },
	      "granularitySpec": {
	        "queryGranularity": "none",
	        "rollup": false,
	        "segmentGranularity": "day",
			"intervals": ["2015-09-12/2015-09-13"]
	      }
	    }
	  }
	}`
	task = fmt.Sprintf(task, data, dataSource, dataSource)
	return task
}

func GetKillTaskDefinition() string {
	task := `{
		"type": "kill",
 		"dataSource": "kubedb-datasource",
 		"interval": "2015-09-12/2015-09-13"
	}`
	return task
}

func (c *Client) CheckTaskStatus(taskID string) (bool, error) {
	method := "GET"
	path := fmt.Sprintf("druid/indexer/v1/task/%s/status", taskID)
	response, err := c.SubmitRequest(method, path, nil)
	if err != nil {
		return false, errors.Wrap(err, "failed to check task status")
	}

	statusRes, err := GetValueFromClusterResponse(response, "status")
	if err != nil {
		return false, errors.Wrap(err, "failed to parse respons of task ingestion request")
	}
	statusMap := statusRes.(map[string]interface{})
	status := statusMap["status"].(string)

	if err = closeResponse(response); err != nil {
		return false, err
	}
	return status == "SUCCESS", nil
}

func (c *Client) checkDBReadAccess(oldData string) error {
	for i := 0; i < 5; i++ {
		klog.V(5).Info("waiting for the segments to be available for query...")
		time.Sleep(6 * time.Second)

		data, err := c.GetData()
		if err != nil {
			klog.Error(err, "failed to read ingested data")
			return err
		}
		if data != oldData {
			klog.V(5).Info("successfully read ingested data")
			return nil
		}
	}
	return errors.New("failed to read ingested data")
}
