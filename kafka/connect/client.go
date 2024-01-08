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
	"io"
	"net/http"

	"github.com/go-resty/resty/v2"
	"github.com/pkg/errors"
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

type Response struct {
	Code   int
	Header http.Header
	Body   io.ReadCloser
}

type ResponseBody struct {
	Version        string `json:"version"`
	Commit         string `json:"commit"`
	KafkaClusterId string `json:"kafka_cluster_id"`
}

func (cc *Client) GetConnectClusterStatus() (*Response, error) {
	req := cc.Client.R().SetDoNotParseResponse(true)
	res, err := req.Get(cc.Config.api)
	if err != nil {
		klog.Error(err, "Failed to send http request")
		return nil, err
	}
	response := &Response{
		Code:   res.StatusCode(),
		Header: res.Header(),
		Body:   res.RawBody(),
	}

	return response, nil
}

func (cc *Client) GetConnector() (*Response, error) {
	return cc.GetConnectClusterStatus()
}

func (cc *Client) DeleteConnector() (*Response, error) {
	req := cc.Client.R().SetDoNotParseResponse(true)
	res, err := req.Delete(cc.Config.api)
	if err != nil {
		klog.Error(err, "Failed to send http request")
		return nil, err
	}
	response := &Response{
		Code:   res.StatusCode(),
		Header: res.Header(),
		Body:   res.RawBody(),
	}

	return response, nil
}

func (cc *Client) PutConnector(jsonBody []byte) (*Response, error) {
	req := cc.Client.R().SetDoNotParseResponse(true).SetHeader("Content-Type", "application/json").SetBody(jsonBody)
	res, err := req.Put(cc.Config.api)
	if err != nil {
		klog.Error(err, "Failed to send http request")
		return nil, err
	}
	response := &Response{
		Code:   res.StatusCode(),
		Header: res.Header(),
		Body:   res.RawBody(),
	}

	return response, nil
}

func (cc *Client) PostConnector(jsonBody []byte) (*Response, error) {
	req := cc.Client.R().SetDoNotParseResponse(true).SetHeader("Content-Type", "application/json").SetBody(jsonBody)
	res, err := req.Post(cc.Config.api)
	if err != nil {
		klog.Error(err, "Failed to send http request")
		return nil, err
	}
	response := &Response{
		Code:   res.StatusCode(),
		Header: res.Header(),
		Body:   res.RawBody(),
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
	}(response.Body)

	var responseBody ResponseBody
	body, _ := io.ReadAll(response.Body)
	err := json.Unmarshal(body, &responseBody)
	if err != nil {
		return false, errors.Wrap(err, "Failed to parse response body")
	}

	return true, nil
}
