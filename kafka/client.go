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
	config *kafkago.Config
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
