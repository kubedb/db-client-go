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
	"context"
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
	"k8s.io/klog/v2"
)

func (c *AMQPClient) GetMessagingChannel() *Channel {
	// Create a channel for sending and receiving messages
	messagingCh, err := c.Channel()
	if err != nil {
		klog.Error(err, "Failed to Open a Messaging Channel")
		return &Channel{nil}
	}
	return &Channel{messagingCh}
}

func (ch *Channel) GetNewQueue(name string, isDurable bool, isTypeQuoeum bool) (*amqp.Queue, error) {
	// Declare a non-persistent queue, where messages will be sent
	q, err := ch.QueueDeclare(
		name,      // name
		isDurable, // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		klog.Error(err, "Failed to create a queue for publishing message")
		return nil, err
	}
	return &q, nil
}

func (ch *Channel) PublishMessageOnce(ctx context.Context, queueName string, message string) error {
	// Declare a non-persistent queue, where messages will be sent
	err := ch.PublishWithContext(
		ctx,
		"",        // exchange
		queueName, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	if err != nil {
		klog.Error(err, "Failed to publish message")
		return err
	}
	return nil
}

func (ch *Channel) ConsumeMessageOnce(ctx context.Context, cancelFunc context.CancelFunc, queueName string) error {
	// start delivering messages through the channel,
	// a  goroutine will be collecting messages with the context timeout
	deliveryCh, err := ch.ConsumeWithContext(
		ctx,
		queueName, // queue
		"",        // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		klog.Error(err, "Failed to consume message")
		return err
	}
	received := false
	go func() {
		for d := range deliveryCh {
			if d.Body != nil {
				received = true
				cancelFunc()
				return
			}
		}
	}()

	<-ctx.Done()
	if !received {
		return errors.New("failed to consume message due to timeout")
	}
	return nil
}
