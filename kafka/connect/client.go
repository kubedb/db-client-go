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

package connect

import (
	"encoding/json"
	"github.com/go-logr/logr"
	"github.com/go-resty/resty/v2"
	"github.com/pkg/errors"
	"io"
	"k8s.io/klog/v2"
	"net/http"
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
	log              logr.Logger
}

type Response struct {
	Code   int
	header http.Header
	body   io.ReadCloser
}

type ResponseBody struct {
	Version        string `json:"version"`
	Commit         string `json:"commit"`
	KafkaClusterId string `json:"kafka_cluster_id"`
}

func (cc *Client) GetKafkaConnectClusterStatus() (*Response, error) {
	req := cc.Client.R().SetDoNotParseResponse(true)
	res, err := req.Get(cc.Config.api)
	if err != nil {
		klog.Error(err, "Failed to send http request")
		return nil, err
	}
	response := &Response{
		Code:   res.StatusCode(),
		header: res.Header(),
		body:   res.RawBody(),
	}

	return response, nil
}

// IsConnectClusterActive parse health response in json from server and
// return overall status of the server
func (cc *Client) IsConnectClusterActive(response *Response) (bool, error) {
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

	var responseBody ResponseBody
	body, _ := io.ReadAll(response.body)
	err := json.Unmarshal(body, &responseBody)
	if err != nil {
		return false, errors.Wrap(err, "Failed to parse response body")
	}

	return true, nil
}
