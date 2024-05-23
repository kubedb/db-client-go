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
	rabbithole "github.com/michaelklishin/rabbit-hole/v2"
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

func (c *HTTPClient) GetQueues() ([]rabbithole.QueueInfo, error) {
	queues, err := c.Client.ListQueues()
	if err != nil {
		klog.Error(err, "Failed to get queue lists")
		return nil, err
	}
	return queues, nil
}

func (c *HTTPClient) GetClassicQueues() ([]rabbithole.QueueInfo, error) {
	queues, err := c.GetQueues()
	if err != nil {
		klog.Error(err, "Failed to get queue lists")
		return nil, err
	}
	classicQueues := []rabbithole.QueueInfo{}

	for _, q := range queues {
		if q.Type == rabbitmqQueueTypeClassic {
			classicQueues = append(classicQueues, q)
		}
	}

	return classicQueues, nil
}

func (c *HTTPClient) HasNodeAnyClassicQueue(queues []rabbithole.QueueInfo, node string) bool {
	for _, q := range queues {
		if q.Type == rabbitmqQueueTypeClassic && q.Node == node {
			return true
		}
	}
	return false
}

func (c *HTTPClient) GetQuorumQueues() ([]rabbithole.QueueInfo, error) {
	queues, err := c.GetQueues()
	if err != nil {
		klog.Error(err, "Failed to get queue lists")
		return nil, err
	}
	quorumQueues := []rabbithole.QueueInfo{}

	for _, q := range queues {
		if q.Type == rabbitmqQueueTypeQuorum {
			quorumQueues = append(quorumQueues, q)
		}
	}

	return quorumQueues, nil
}

func (c *HTTPClient) IsNodePrimaryReplica(queues []rabbithole.QueueInfo, node string) bool {
	for _, q := range queues {
		if q.Type == rabbitmqQueueTypeQuorum && q.Leader == node {
			return true
		}
	}
	return false
}

func (c *HTTPClient) GetNodeNameFromPodURL(url string) string {
	podClient, err := rabbithole.NewClient(url, c.Username, c.Password)
	if err != nil {
		return ""
	}
	overview, err := podClient.Overview()
	if err != nil {
		return ""
	}
	return overview.Node
}
