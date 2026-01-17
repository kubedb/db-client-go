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

package db2

import (
	"net/http"

	"github.com/go-resty/resty/v2"
	"k8s.io/klog/v2"
)

type Client struct {
	*resty.Client
	Config *Config
}

type Config struct {
	host             string
	api              string
	username         string
	password         string
	connectionScheme string
	transport        *http.Transport
}

func (cc *Client) PingDB2() (bool, error) {
	req := cc.Client.R().SetDoNotParseResponse(true)
	res, err := req.Get("/ready")
	if err != nil {
		klog.Error(err, "Failed to send http request")
		return false, err
	}

	return res.StatusCode() == http.StatusOK, nil
}

func (cc *Client) ReadWriteCheck() (bool, error) {
	req := cc.Client.R().SetDoNotParseResponse(true)
	res, err := req.Get("/test")
	if err != nil {
		klog.Error(err, "Failed to send http request")
		return false, err
	}

	return res.StatusCode() == http.StatusOK, nil
}
