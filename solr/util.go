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

package solr

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"
	dbapi "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	"github.com/pkg/errors"
)

func (sc *Client) DecodeResponse(response *Response) (map[string]interface{}, error) {
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			err1 := errors.Wrap(err, "failed to parse response body")
			if err1 != nil {
				return
			}
			return
		}
	}(response.body)

	responseBody := make(map[string]interface{})
	if err := json.NewDecoder(response.body).Decode(&responseBody); err != nil {
		return nil, fmt.Errorf("failed to deserialize the response: %v", err)
	}

	return responseBody, nil
}

func (sc *Client) GetResponseStatus(responseBody map[string]interface{}) (int, error) {
	err, ok := responseBody["error"].(map[string]interface{})
	if ok {
		msg, ok := err["msg"].(string)
		if !ok {
			return -1, errors.New("no msg found in error message while getting response status")

		}
		code, ok := err["code"].(float64)
		if !ok {
			return -1, errors.New("error occurred but didn't found error code while getting response status")

		}
		return -1, errors.New(fmt.Sprintf("Error: %v with code %d", msg, int(code)))
	}
	responseHeader, ok := responseBody["responseHeader"].(map[string]interface{})
	if !ok {
		return -1, errors.New("didn't find responseHeader")
	}

	status, ok := responseHeader["status"].(float64)
	if !ok {
		return -1, errors.New("didn't find status")
	}

	if int(status) != 0 {
		msg, ok := responseBody["message"].(string)
		if !ok {
			return -1, errors.New("no msg found in error message")

		}
		return -1, errors.New(fmt.Sprintf("Error: %v with code %d", msg, int(status)))
	}
	return int(status), nil
}

func (sc *Client) GetVal(ival interface{}, ss string) float64 {
	val := reflect.ValueOf(ival)
	fmt.Println(ss, ival, reflect.TypeOf(ival), val, val.Type(), val.Kind())

	realVal := val.Interface().(float64)

	return realVal
}

func (sc *Client) RetrieveMetrics(responseBody map[string]interface{}) (*Metrics, error) {
	metrics := &Metrics{}

	responseMetrics, ok := responseBody["metrics"].(map[string]interface{})
	if !ok {
		return nil, errors.New("didn't find metrics")
	}

	for metricsKey, metricsVal := range responseMetrics {
		if metricsKey == "solr.jvm" {
			metrics.JVM = JVM{}
			jvmMetrics := metricsVal.(map[string]interface{})
			for jvmKey, jvmVal := range jvmMetrics {
				val := jvmVal
				switch jvmKey {
				case "buffers.direct.Count":
					metrics.JVM.BuffersDirectCount = sc.GetVal(val, jvmKey)
				case "buffers.direct.MemoryUsed":
					metrics.JVM.BuffersDirectMemoryUsed = sc.GetVal(val, jvmKey)
				case "buffers.direct.TotalCapacity":
					metrics.JVM.BuffersDirectTotalCapacity = sc.GetVal(val, jvmKey)
				case "buffers.mapped.Count":
					metrics.JVM.BuffersMappedCount = sc.GetVal(val, jvmKey)
				case "buffers.mapped.MemoryUsed":
					metrics.JVM.BuffersMappedMemoryUsed = sc.GetVal(val, jvmKey)
				case "buffers.mapped.TotalCapacity":
					metrics.JVM.BuffersMappedTotalCapacity = sc.GetVal(val, jvmKey)
				case "memory.heap.max":
					metrics.JVM.MemoryHeapMax = sc.GetVal(val, jvmKey)
				case "memory.heap.used":
					metrics.JVM.MemoryHeapUsed = sc.GetVal(val, jvmKey)
				case "memory.heap.usage":
					metrics.JVM.MemoryHeapUsage = sc.GetVal(val, jvmKey)
				case "threads.count":
					metrics.JVM.ThreadsCount = sc.GetVal(val, jvmKey)
				case "threads.peak.count":
					metrics.JVM.ThreadsPeakCount = sc.GetVal(val, jvmKey)
				case "threads.runnable.count":
					metrics.JVM.ThreadsRunnableCount = sc.GetVal(val, jvmKey)
				default:
					klog.Info(fmt.Sprintf("Unsupported metrics key: %s", jvmKey))
				}
			}
		} else if metricsKey == "solr.jetty" {
			metrics.Jetty = Jetty{}
			jettyMetrics := metricsVal.(map[string]interface{})
			for jettyKey, jettyVal := range jettyMetrics {
				pos := 0
				pos = strings.LastIndex(jettyKey, ".")
				key := jettyKey[pos+1:]
				val := reflect.ValueOf(jettyVal)
				switch key {
				case "jobs":
					metrics.Jetty.Jobs = val
				case "size":
					metrics.Jetty.Size = val
				case "utilization":
					metrics.Jetty.Utilization = val
				default:
					klog.Info(fmt.Sprintf("Unsupported metrics key: %s", jettyKey))
				}
			}
		}
	}

	return metrics, nil
}

func (sc *Client) GetAsyncStatus(responseBody map[string]interface{}) (string, error) {
	status, ok := responseBody["status"].(map[string]interface{})
	if !ok {
		return "unknown", errors.New("didn't find status")
	}

	state, ok := status["state"].(string)
	if !ok {
		return "unknown", errors.New("didn't find state")
	}

	return state, nil
}

func (sc *Client) DecodeCollectionHealth(responseBody map[string]interface{}) error {
	clusterInfo, ok := responseBody["cluster"].(map[string]interface{})
	if !ok {
		return errors.New(fmt.Sprintf("did not find cluster %v\n", responseBody))
	}
	collections, ok := clusterInfo["collections"].(map[string]interface{})
	if !ok {
		return errors.New("didn't find collections")
	}
	for name, info := range collections {
		collectionInfo := info.(map[string]interface{})
		health, ok := collectionInfo["health"].(string)
		if !ok {
			return errors.New("didn't find health")
		}
		if health != "GREEN" {
			if name == writeCollectionName {
				response, err := sc.DeleteCollection(name)
				if err != nil {
					return err
				}
				responseBody, err := sc.DecodeResponse(response)
				if err != nil {
					klog.Error(err)
					return err
				}

				_, err = sc.GetResponseStatus(responseBody)
				if err != nil {
					klog.Error(err)
					return err
				}
			}
			config := sc.GetConfig()
			config.log.Error(errors.New(""), fmt.Sprintf("Health of collection %s IS NOT GREEN", name))
			return errors.New(fmt.Sprintf("health for collection %s is not green", name))
		}
	}
	return nil
}

func (sc *Client) GetCollectionList(responseBody map[string]interface{}) ([]string, error) {
	collectionList, ok := responseBody["collections"].([]interface{})
	if !ok {
		return []string{}, errors.New("didn't find collection list")
	}

	collections := make([]string, 0)

	for idx := range collectionList {
		collections = append(collections, collectionList[idx].(string))
	}
	return collections, nil
}

func (sc *Client) SearchCollection(collections []string) bool {
	for _, collection := range collections {
		if collection == "kubedb-system" {
			return true
		}
	}
	return false
}

func (sc *Client) CheckupStatus(async string) error {
	var wg sync.WaitGroup
	wg.Add(1)
	var errr error
	go func() {
		defer wg.Done()
		asyncId := async
		for {
			resp, err := sc.RequestStatus(asyncId)
			if err != nil {
				klog.Error(fmt.Sprintf("Failed to get response for asyncId %s. Error: %v", asyncId, err))
				errr = err
				return
			}

			responseBody, err := sc.DecodeResponse(resp)
			if err != nil {
				klog.Error(fmt.Sprintf("Failed to decode response for asyncId %s. Error: %v", asyncId, err))
				errr = err
				return
			}

			_, err = sc.GetResponseStatus(responseBody)
			if err != nil {
				klog.Error(fmt.Sprintf("status is non zero while checking status for asyncId %s. Error %v", asyncId, err))
				errr = err
				return
			}

			state, err := sc.GetAsyncStatus(responseBody)
			if err != nil {
				klog.Error(fmt.Sprintf("status is non zero while checking state of async for asyncId %s. Error %v", asyncId, err))
				errr = err
				return
			}
			klog.Info(fmt.Sprintf("State for asyncid %v is %v\n", asyncId, state))
			if state == "completed" {
				klog.Info("Status is completed for ", asyncId)
				err = sc.FlushAsyncStatus(asyncId)
				if err != nil {
					errr = err
					return
				}
				return
			} else if state == "failed" {
				klog.Info(fmt.Sprintf("API call for asyncId %s failed", asyncId))
				err = sc.FlushAsyncStatus(asyncId)
				if err != nil {
					errr = err
					return
				}
				errr = fmt.Errorf("response for asyncid %v. failed with response %v", asyncId, responseBody)
				break
			} else if state == "notfound" {
				klog.Info(fmt.Sprintf("API call for asyncid %s not found", asyncId))
				break
			}
			time.Sleep(10 * time.Second)
		}
	}()
	wg.Wait()
	return errr
}

func (sc *Client) FlushAsyncStatus(asyncId string) error {
	resp, err := sc.FlushStatus(asyncId)
	if err != nil {
		return err
	}

	responseBody, err := sc.DecodeResponse(resp)
	if err != nil {
		return err
	}

	_, err = sc.GetResponseStatus(responseBody)
	if err != nil {
		klog.Error("status is non zero while flushing status", err)
		return err
	}

	return nil
}

func (sc *Client) CleanupAsync(async string) error {
	var wg sync.WaitGroup
	wg.Add(1)
	var errr error
	go func() {
		defer wg.Done()
		asyncId := async
		for {
			resp, err := sc.RequestStatus(asyncId)
			if err != nil {
				klog.Error(fmt.Sprintf("Failed to get response for asyncId %s. Error: %v", asyncId, err))
				errr = err
				break
			}

			responseBody, err := sc.DecodeResponse(resp)
			if err != nil {
				klog.Error(fmt.Sprintf("Failed to decode response for asyncId %s. Error: %v", asyncId, err))
				errr = err
				break
			}

			_, err = sc.GetResponseStatus(responseBody)
			if err != nil {
				klog.Error(fmt.Sprintf("status is non zero while checking status for asyncId %s. Error %v", asyncId, err))
				errr = err
				break
			}

			state, err := sc.GetAsyncStatus(responseBody)
			if err != nil {
				klog.Error(fmt.Sprintf("status is non zero while checking state of async for asyncId %s. Error %v", asyncId, err))
				errr = err
				break
			}
			klog.Info(fmt.Sprintf("State for asyncid %v is %v\n", asyncId, state))
			if state == "completed" || state == "notfound" || state == "failed" {
				err := sc.FlushAsyncStatus(asyncId)
				if err != nil {
					klog.Error(fmt.Sprintf("Failed to flush api call for asyncId %s. Error %v", asyncId, err))
					time.Sleep(20 * time.Second)
					continue
				}
				break
			}
			time.Sleep(10 * time.Second)
		}
	}()
	wg.Wait()
	return errr
}

func (sc *Client) Balance() error {
	var errr error
	async := "balance-replica"
	err := sc.CleanupAsync(async)
	if err != nil {
		errr = err
	} else {
		klog.Info(fmt.Sprintf("Cleanup async successful for %v", async))
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		resp, err := sc.BalanceReplica(async)
		if err != nil {
			klog.Error(fmt.Errorf("failed to do balance request. err %v", err))
			errr = err
		}
		responseBody, err := sc.DecodeResponse(resp)
		if err != nil {
			klog.Error(fmt.Errorf("failed to decode response for async %s, err %v", async, err))
			errr = err
		}
		_, err = sc.GetResponseStatus(responseBody)
		if err != nil {
			klog.Error(fmt.Errorf("failed to decode response for async %s, err %v", async, err))
			errr = err
		}

		err = sc.CheckupStatus(async)
		if err != nil {
			errr = err
		}
	}()
	wg.Wait()
	return errr
}

func (sc *Client) Run(lst []UpdateList) error {
	var errr error
	var wg sync.WaitGroup
	for _, x := range lst {
		target := x.target
		replica := x.replica
		collection := x.collection
		async := fmt.Sprintf("%s-%s-%s", replica, collection, target)
		err := sc.CleanupAsync(async)
		if err != nil {
			errr = err
		}
		klog.Info(fmt.Sprintf("Cleanup async successful for %v", async))

		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := sc.MoveReplica(target, replica, collection, async)
			if err != nil {
				klog.Error(fmt.Errorf("failed to do request for target %s, replica %s, collection %s, err %v", target, replica, collection, err))
				errr = err
			}
			responseBody, err := sc.DecodeResponse(resp)
			if err != nil {
				klog.Error(fmt.Errorf("failed to decode response for target %s, replica %s, collection %s, err %v", target, replica, collection, err))
				errr = err
			}
			_, err = sc.GetResponseStatus(responseBody)
			if err != nil {
				klog.Error(fmt.Errorf("failed to decode response for target %s, replica %s, collection %s, err %v, responsebody %v", target, replica, collection, err, responseBody))
				errr = err
			}

			err = sc.CheckupStatus(async)
			if err != nil {
				errr = err
			}
		}()
	}
	wg.Wait()
	return errr
}

func (sc *Client) Down(nodeList []string, x int, mp map[string][]CoreList) error {
	n := len(nodeList)
	ls2 := nodeList[n-x:]
	ls1 := nodeList[:n-x]
	fmt.Println("ls1 ", ls1)
	fmt.Println("ls2 ", ls2)
	ar := make([]UpdateList, 0)
	for _, node := range ls2 {
		for _, core := range mp[node] {
			id := -1
			mx := 1000000000
			for j, l1 := range ls1 {
				if len(mp[l1]) < mx {
					mx = len(mp[l1])
					id = j
				}
			}
			ar = append(ar, UpdateList{
				target:     ls1[id],
				replica:    core.coreName,
				collection: core.collection,
			})
			mp[ls1[id]] = append(mp[ls1[id]], core)
			fmt.Println(core.coreName, core.collection, ls1[id])
		}
	}
	err := sc.Run(ar)
	return err
}
func (sc *Client) Up(nodeList []string, mp map[string][]CoreList) error {
	for _, x := range nodeList {
		if _, ok := mp[x]; !ok {
			mp[x] = make([]CoreList, 0)
		}
	}
	ar := make([]UpdateList, 0)
	for {
		mn := 10000000000
		minNode := ""
		mx := -1
		maxNode := ""
		for x, y := range mp {
			n := len(y)
			if mx < n {
				mx = n
				maxNode = x
			}

			if mn > n {
				mn = n
				minNode = x
			}
		}
		if maxNode == minNode || mx-mn <= 1 {
			break
		}
		target := minNode
		core := mp[maxNode][0].coreName
		collection := mp[maxNode][0].collection
		mp[minNode] = append(mp[minNode], mp[maxNode][0])
		mp[maxNode] = mp[maxNode][1:]
		ar = append(ar, UpdateList{
			target:     target,
			replica:    core,
			collection: collection,
		})
		fmt.Println(target, core, collection)
	}
	err := sc.Run(ar)
	return err
}

func (sc *Client) UpReplicaManual(db *dbapi.Solr) error {

	response, err := sc.GetClusterStatus()
	if err != nil {
		klog.Error(err)
		return err
	}

	responseBody, err := sc.DecodeResponse(response)
	if err != nil {
		klog.Error(err)
		return err
	}

	_, err = sc.GetResponseStatus(responseBody)
	if err != nil {
		klog.Error(err)
		return err
	}

	clusterInfo, ok := responseBody["cluster"].(map[string]interface{})
	if !ok {
		klog.Error(fmt.Errorf("did not find cluster %v\n", responseBody))
	}
	collections, ok := clusterInfo["collections"].(map[string]interface{})
	if !ok {
		klog.Error("didn't find collections")
	}
	mp := make(map[string][]CoreList)
	for collection, info := range collections {
		collectionInfo := info.(map[string]interface{})
		shardInfo := collectionInfo["shards"].(map[string]interface{})
		for _, info := range shardInfo {
			shardInfo := info.(map[string]interface{})
			replicaInfo := shardInfo["replicas"].(map[string]interface{})
			for core, info := range replicaInfo {
				coreInfo := info.(map[string]interface{})
				nodeName := coreInfo["node_name"].(string)
				if _, ok := mp[nodeName]; !ok {
					mp[nodeName] = make([]CoreList, 0)
				}
				mp[nodeName] = append(mp[nodeName], CoreList{
					coreName:   core,
					collection: collection,
				})
			}
		}
	}

	nodeList := make([]string, 0)

	liveNodes, ok := clusterInfo["live_nodes"]
	if !ok {
		return errors.New("Failed to get livenodes")
	}
	xx := liveNodes.([]interface{})
	for _, node := range xx {
		x := node.(string)
		nodeList = append(nodeList, x)
	}
	sort.Strings(nodeList)
	fmt.Println(nodeList)

	if db.Spec.Topology != nil {
		str := strings.Join([]string{db.Name, "data"}, "-")
		mn := 10000000000
		mx := -1
		for i, node := range nodeList {
			if strings.Contains(node, str) {
				mn = min(mn, i)
				mx = max(mx, i)
			}
		}
		nodeList = nodeList[mn : mx+1]
		fmt.Println("nodes ", mx-mn+1, nodeList)
	}

	err = sc.Up(nodeList, mp)
	return err
}

func (sc *Client) BalanceReplicaManual(db *dbapi.Solr, desired int) error {

	response, err := sc.GetClusterStatus()
	if err != nil {
		klog.Error(err)
		return err
	}

	responseBody, err := sc.DecodeResponse(response)
	if err != nil {
		klog.Error(err)
		return err
	}

	_, err = sc.GetResponseStatus(responseBody)
	if err != nil {
		klog.Error(err)
		return err
	}

	clusterInfo, ok := responseBody["cluster"].(map[string]interface{})
	if !ok {
		klog.Error(fmt.Errorf("did not find cluster %v\n", responseBody))
	}
	collections, ok := clusterInfo["collections"].(map[string]interface{})
	if !ok {
		klog.Error("didn't find collections")
	}
	mp := make(map[string][]CoreList)
	for collection, info := range collections {
		collectionInfo := info.(map[string]interface{})
		shardInfo := collectionInfo["shards"].(map[string]interface{})
		for _, info := range shardInfo {
			shardInfo := info.(map[string]interface{})
			replicaInfo := shardInfo["replicas"].(map[string]interface{})
			for core, info := range replicaInfo {
				coreInfo := info.(map[string]interface{})
				nodeName := coreInfo["node_name"].(string)
				if _, ok := mp[nodeName]; !ok {
					mp[nodeName] = make([]CoreList, 0)
				}
				mp[nodeName] = append(mp[nodeName], CoreList{
					coreName:   core,
					collection: collection,
				})
			}
		}
	}

	nodeList := make([]string, 0)

	liveNodes, ok := clusterInfo["live_nodes"]
	if !ok {
		return errors.New("Failed to get livenodes")
	}
	xx := liveNodes.([]interface{})
	for _, node := range xx {
		x := node.(string)
		nodeList = append(nodeList, x)
	}
	sort.Strings(nodeList)
	fmt.Println(nodeList)

	if db.Spec.Topology != nil {
		str := strings.Join([]string{db.Name, "data"}, "-")
		mn := 10000000000
		mx := -1
		for i, node := range nodeList {
			if strings.Contains(node, str) {
				mn = min(mn, i)
				mx = max(mx, i)
			}
		}
		nodeList = nodeList[mn : mx+1]
		fmt.Println("nodes ", mx-mn+1, nodeList)
	}

	err = sc.Down(nodeList, desired, mp)
	return err
}
