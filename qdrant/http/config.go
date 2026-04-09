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

package http

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"time"
)

const (
	defaultHost = "localhost"
	defaultPort = 6333

	defaultRequestTimeout  = 30 * time.Second
	defaultIdleConnTimeout = 90 * time.Second
)

// Config holds the configuration for a Qdrant HTTP client.
type Config struct {
	Host string
	Port int

	APIKey    string
	UseTLS    bool
	TLSConfig *tls.Config

	RequestTimeout  time.Duration
	IdleConnTimeout time.Duration
}

func (c *Config) getBaseURL() string {
	host := c.Host
	if host == "" {
		host = defaultHost
	}

	port := c.Port
	if port == 0 {
		port = defaultPort
	}

	scheme := "http"
	if c.UseTLS {
		scheme = "https"
	}

	return fmt.Sprintf("%s://%s:%d", scheme, host, port)
}

func (c *Config) getHTTPClient() *http.Client {
	reqTimeout := defaultRequestTimeout
	if c.RequestTimeout > 0 {
		reqTimeout = c.RequestTimeout
	}

	idleTimeout := defaultIdleConnTimeout
	if c.IdleConnTimeout > 0 {
		idleTimeout = c.IdleConnTimeout
	}

	transport := &http.Transport{
		IdleConnTimeout:     idleTimeout,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
	}

	if c.UseTLS {
		tlsCfg := c.TLSConfig
		if tlsCfg == nil {
			tlsCfg = &tls.Config{}
		}
		transport.TLSClientConfig = tlsCfg
	}

	return &http.Client{
		Transport: transport,
		Timeout:   reqTimeout,
	}
}
