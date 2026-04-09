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
