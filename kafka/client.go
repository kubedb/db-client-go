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
	"time"

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

type ConsumerClient struct {
	kafkago.Consumer
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

func (c *Client) RefreshTopicMetadata(topics ...string) error {
	err := c.RefreshMetadata(topics...)
	if err != nil {
		klog.Error(err, "Failed to refresh metadata", "Topics", topics)
		return err
	}
	_, err = c.RefreshController()
	if err != nil {
		klog.Error(err, "Failed to refresh controller")
		return err
	}
	return nil
}

func (c *Client) GetPartitionLeaderAddress(partition int32, topic string) (string, error) {
	if err := c.RefreshTopicMetadata(topic); err != nil {
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
func (c *Client) DeleteTopic(topics ...string) {
	broker, err := c.RefreshController()
	if err != nil {
		klog.Error(err, "Failed to refresh controller for kafka-health topic")
		return
	}
	_, err = broker.DeleteTopics(&kafkago.DeleteTopicsRequest{
		Topics:  topics,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		klog.Error(err, "Failed to delete kafka health topic")
		return
	}

	err = c.RefreshTopicMetadata(topics...)
	if err != nil {
		klog.Error("Failed to refresh topic metadata")
		return
	}
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

func (c ConsumerClient) ConsumeMessages(partition int32, topic string, offset int64, signal *chan bool, message *chan MessageMetadata) error {
	var err error
	var partitionConsumer kafkago.PartitionConsumer
	partitionConsumer, err = c.ConsumePartition(topic, partition, offset)
	if err != nil {
		klog.Error(err, "Failed to create partition consumer")
		return err
	}
	defer partitionConsumer.AsyncClose()

	for {
		select {
		case <-*signal:
			return nil
		case err := <-partitionConsumer.Errors():
			klog.Info(fmt.Sprintf("could not process message, err: %s", err.Error()))
			return err
		case msg := <-partitionConsumer.Messages():
			msgMetadata := MessageMetadata{
				Key:       string(msg.Key),
				Value:     string(msg.Value),
				Partition: msg.Partition,
				Offset:    msg.Offset,
			}
			*message <- msgMetadata
		}
	}
}
