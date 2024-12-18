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
	"context"
	"fmt"
	"github.com/go-logr/logr"
	"time"

	kafkago "github.com/IBM/sarama"
	clog "sigs.k8s.io/controller-runtime/pkg/log"
)

var log logr.Logger

func init() {
	log = clog.FromContext(context.Background()).WithName("db-client-go").WithName("kafka")
}

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
	broker, err := c.Controller()
	if err != nil || broker == nil {
		log.Error(err, "Failed to Get kafka broker")
		return false, err
	}

	connected, err := broker.Connected()
	if err != nil {
		log.Error(err, fmt.Sprintf("Failed to connect broker: %s", broker.Addr()))
		return false, err
	}
	if connected {
		log.V(1).Info(fmt.Sprintf("Successfully connected broker: %s", broker.Addr()))
	} else {
		log.Error(fmt.Errorf("failed to connect broker: %s", broker.Addr()), "failed to connect broker")
	}

	return connected, nil
}

func (c *Client) CheckKafkaBrokers(numOfBrokers int32) error {
	for i := int32(0); i < numOfBrokers; i++ {
		broker, err := c.Broker(i)
		if err != nil {
			log.Error(err, "Failed to get broker with id", "id", i)
			return err
		}
		connected, err := broker.Connected()
		if err != nil {
			log.Error(err, "Failed to connect broker", "id", i)
			return err
		}
		if !connected {
			log.Error(err, "Broker is not connected", "id", i)
			return fmt.Errorf("broker %d, is not connected", i)
		}
	}
	log.V(1).Info("all brokers are available to accept request")
	return nil
}

func (c *Client) RefreshTopicMetadata(topics ...string) error {
	err := c.RefreshMetadata(topics...)
	if err != nil {
		log.Error(err, "Failed to refresh metadata", "Topics", topics)
		return err
	}
	_, err = c.RefreshController()
	if err != nil {
		log.Error(err, "Failed to refresh controller")
		return err
	}
	return nil
}

func (c *Client) GetPartitionLeaderAddress(partition int32, topic string) (string, error) {
	leader, err := c.Leader(topic, partition)
	if err != nil {
		log.Error(err, "Failed to get leader", "partition", partition)
		return "", err
	}
	return leader.Addr(), nil
}

func (a *AdminClient) IsTopicExists(topic string) (bool, error) {
	topics, err := a.ListTopics()
	if err != nil {
		log.Error(err, "Failed to list kafka topics")
		return false, err
	}
	_, topicExists := topics[topic]
	return topicExists, nil
}

func (a *AdminClient) CreateKafkaTopic(topic string, topicConfig map[string]*string, partitions int32, replicationFactor int16) error {
	err := a.CreateTopic(topic, &kafkago.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
		ConfigEntries:     topicConfig,
	}, true)
	if err != nil {
		log.Error(err, fmt.Sprintf("Failed to create topic - %s", topic))
		return err
	}
	log.Info(fmt.Sprintf("Created topic - %s", topic))
	return nil
}

func (c *Client) DeleteKafkaTopics(topics ...string) {
	broker, err := c.Controller()
	if err != nil {
		log.Error(err, "Failed to get controller broker")
		return
	}
	_, err = broker.DeleteTopics(&kafkago.DeleteTopicsRequest{
		Topics:  topics,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		log.Error(err, "Failed to delete kafka health topic")
		return
	}
}

func (p *ProducerClient) PublishMessages(partition int32, topic, key, message string) (*MessageMetadata, error) {
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
		log.Error(err, "Failed to send message", "topic", topic, "partition", partition)
		return nil, err
	}

	return &msgMetadata, nil
}

func (c *ConsumerClient) ConsumeMessages(partition int32, topic string, offset int64, signal *chan bool, message *chan MessageMetadata) error {
	var err error
	var partitionConsumer kafkago.PartitionConsumer
	partitionConsumer, err = c.ConsumePartition(topic, partition, offset)
	if err != nil {
		log.Error(err, "Failed to create partition consumer")
		return err
	}
	defer partitionConsumer.AsyncClose()

	for {
		select {
		case <-*signal:
			return nil
		case err := <-partitionConsumer.Errors():
			log.Error(err, "could not process message")
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
