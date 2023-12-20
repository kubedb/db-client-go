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

package kafka

import (
	"fmt"

	kafkago "github.com/IBM/sarama"
	"k8s.io/klog/v2"
)

type Client struct {
	kafkago.Client
}

type ProducerClient struct {
	kafkago.SyncProducer
}

type AdminClient struct {
	kafkago.ClusterAdmin
}

type MessageMetadata struct {
	Key       string
	Value     string
	Partition int32
	Offset    int64
}

func (c *Client) IsDBConnected() (bool, error) {
	// TODO: try using refreshcontroller
	controller, err := c.RefreshController()
	if err != nil || controller == nil {
		klog.Error(err, "Failed to Get kafka controller")
		return false, err
	}

	connected, err := controller.Connected()
	if err != nil {
		klog.Error(err, fmt.Sprintf("Failed to connect broker: %s", controller.Addr()))
		return false, err
	}
	if connected {
		klog.Info(fmt.Sprintf("Successfully connected broker: %s", controller.Addr()))
	} else {
		klog.Info(fmt.Sprintf("Failed to connect broker: %s", controller.Addr()))
	}

	return connected, nil
}

func (c *Client) GetPartitionLeaderAddress(partition int32, topic string) (string, error) {
	err := c.RefreshMetadata(topic)
	if err != nil {
		klog.Error(err, fmt.Sprintf("Failed to refresh metadata for %s topic", topic))
		return "", err
	}
	_, err = c.RefreshController()
	if err != nil {
		klog.Error(err, fmt.Sprintf("Failed to refresh controller for %s topic", topic))
		return "", err
	}
	leader, err := c.Leader(topic, partition)
	if err != nil {
		klog.Error(err, "Failed to get leader", "partition", partition)
		return "", err
	}
	err = c.RefreshBrokers([]string{leader.Addr()})
	if err != nil {
		klog.Error(err, fmt.Sprintf("Failed to refresh broker for %s topic", topic))
		return "", err
	}
	return leader.Addr(), nil
}

func (a *AdminClient) EnsureKafkaTopic(topic string, topicConfig map[string]*string, partitions int32, replicationFactor int16) error {
	topics, err := a.ListTopics()
	if err != nil {
		klog.Error(err, "Failed to list kafka topics")
		return err
	}

	if _, topicExists := topics[topic]; !topicExists {
		err = a.CreateTopic(topic, &kafkago.TopicDetail{
			NumPartitions:     partitions,
			ReplicationFactor: replicationFactor,
			ConfigEntries:     topicConfig,
		}, true)
		if err != nil {
			klog.Error(err, fmt.Sprintf("Failed to create topic - %s", topic))
			return err
		}
		klog.Info(fmt.Sprintf("Created topic - %s", topic))
	}
	return nil
}

func (p *ProducerClient) SendMessageWithProducer(partition int32, topic, key, message string) (*MessageMetadata, error) {
	producerMsg := &kafkago.ProducerMessage{
		Topic:     topic,
		Key:       kafkago.StringEncoder(key),
		Value:     kafkago.StringEncoder(message),
		Partition: partition,
	}
	msgMetadata := MessageMetadata{
		Key:   key,
		Value: message,
	}
	var err error
	msgMetadata.Partition, msgMetadata.Offset, err = p.SendMessage(producerMsg)
	if err != nil {
		klog.Error(err, "Failed to send message", "topic", topic, "partition", partition)
		return nil, err
	}

	return &msgMetadata, nil
}
