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

package rabbitmq

import (
	"fmt"
	"k8s.io/klog/v2"
)

func (c *HTTPClient) IsAllNodesRunningInCluster(replicas int) (bool, error) {
	nodes, err := c.ListNodes()
	if err != nil {
		klog.Error(err, "Failed to get node lists")
		return true, err
	}

	if len(nodes) < replicas {
		klog.Info(fmt.Sprintf("Cluster requires %v nodes but only %v nodes joined", replicas, len(nodes)))
		return false, nil
	}
	for _, node := range nodes {
		if !node.IsRunning {
			klog.Error(err, fmt.Sprintf("Node: %s is not running", node.Name))
			return false, nil
		}
	}

	klog.Info("All required nodes running in cluster")
	return true, nil
}
