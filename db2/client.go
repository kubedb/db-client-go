package db2

import (
	"context"
	"fmt"
	"github.com/go-resty/resty/v2"
	"k8s.io/klog/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
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

// GetHadrStatus checks the HADR setup status for a database
// Returns: "notSet", "inProgress", "success", or error
func (cc *Client) GetHadrStatus(dbName string) (string, error) {
	req := cc.Client.R().
		SetQueryParam("dbName", dbName)

	res, err := req.Get("/hadr/status")
	if err != nil {
		klog.Errorf("Failed to send http request for HADR status: %v", err)
		return "", err
	}

	if res.RawResponse != nil && res.RawResponse.Body != nil {
		defer res.RawResponse.Body.Close()
	}

	if res.StatusCode() != http.StatusOK {
		return "", fmt.Errorf("status check failed with code %d: %s", res.StatusCode(), res.String())
	}

	// Response body should contain the status string
	status := strings.TrimSpace(res.String())
	return status, nil
}

// SetupHadr initiates the complete HADR setup for a database
// This endpoint handles all steps: create DB, configure primary, restore standby, start HADR
func (cc *Client) SetupHadr(dbName string, primaryPod int, standbyPod int, portNumber int) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // Short timeout for initiation only
	defer cancel()

	req := cc.Client.R().
		SetContext(ctx).
		SetQueryParam("dbName", dbName).
		SetQueryParam("primaryPod", strconv.Itoa(primaryPod)).
		SetQueryParam("standbyPod", strconv.Itoa(standbyPod)).
		SetQueryParam("portNumber", strconv.Itoa(portNumber))

	res, err := req.Get("/hadr/setup")
	if err != nil {
		// Check if it's a "already in progress" conflict (409)
		if res != nil && res.StatusCode() == http.StatusConflict {
			return false, fmt.Errorf("setup already in progress: %w", err)
		}
		return false, fmt.Errorf("failed to send setup request: %w", err)
	}

	if res.RawResponse != nil && res.RawResponse.Body != nil {
		defer res.RawResponse.Body.Close()
	}

	// Accept both 200 OK (shouldn't happen with async) and 202 Accepted
	if res.StatusCode() == http.StatusOK || res.StatusCode() == http.StatusAccepted {
		return true, nil
	}

	return false, fmt.Errorf("unexpected status code: %d", res.StatusCode())
}
