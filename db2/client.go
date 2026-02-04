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

func (cc *Client) CreateDB(dbName string, primaryPod int, standbyPod int) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	req := cc.Client.R().
		SetContext(ctx).
		SetQueryParam("dbName", dbName).
		SetQueryParam("primaryPod", strconv.Itoa(primaryPod)).
		SetQueryParam("standbyPod", strconv.Itoa(standbyPod))

	res, err := req.Get("/createDB")

	fmt.Printf("DEBUG CreateDB: dbName=%s, statusCode=%d, err=%v\n", dbName, res.StatusCode(), err)
	fmt.Printf("DEBUG CreateDB: response body=%s\n", res.String())

	if err != nil {
		// Check for timeout specifically (DB2 startup can be slow)
		if strings.Contains(err.Error(), "timeout") {
			klog.Warningf("CreateDB request timed out for %s, may still be processing", dbName)
			return true, nil // Assume success and verify later
		}
		klog.Errorf("Failed to send http request for create database %s: %v", dbName, err)
		return false, err
	}

	if res.RawResponse != nil && res.RawResponse.Body != nil {
		res.RawResponse.Body.Close()
	}
	// DEBUG: Check specific status codes
	if res.StatusCode() != http.StatusOK {
		fmt.Printf("DEBUG CreateDB: got non-200 status: %d\n", res.StatusCode())
	}

	return res.StatusCode() == http.StatusOK, nil
}

func (cc *Client) ConfigureHadrOnPrimary(dbName string, primaryPod int, standbyPod int, portNumber int) (bool, error) {
	req := cc.Client.R().
		SetQueryParam("dbName", dbName).
		SetQueryParam("primaryPod", strconv.Itoa(primaryPod)).
		SetQueryParam("standbyPod", strconv.Itoa(standbyPod)).
		SetQueryParam("portNumber", strconv.Itoa(portNumber))

	res, err := req.Get("/configureHadrOnPrimary")
	if err != nil {
		if strings.Contains(err.Error(), "timeout") {
			klog.Warningf("ConfigureHadrOnPrimary request timed out for %s, may still be processing", dbName)
			return true, nil
		}
		klog.Errorf("Failed to send http request for configure HADR on primary: %v", err)
		return false, err
	}

	if res.RawResponse != nil && res.RawResponse.Body != nil {
		res.RawResponse.Body.Close()
	}

	return res.StatusCode() == http.StatusOK, nil
}

func (cc *Client) RestoreToStandby(dbName string, primaryPod int, standbyPod int) (bool, error) {
	req := cc.Client.R().
		SetQueryParam("dbName", dbName).
		SetQueryParam("primaryPod", strconv.Itoa(primaryPod)).
		SetQueryParam("standbyPod", strconv.Itoa(standbyPod))

	res, err := req.Get("/restoreToStandby")
	fmt.Printf("DEBUG RestoreToStandby: response body=%s\n", res.String())

	if res.RawResponse != nil && res.RawResponse.Body != nil {
		defer res.RawResponse.Body.Close()
	}

	// HTTP 200 = definitely success
	if res.StatusCode() == http.StatusOK {
		return true, nil
	}

	// CRITICAL: Check if error indicates restore already done
	if res.StatusCode() == http.StatusInternalServerError {
		//body := res.String()
		//bodyUpper := strings.ToUpper(body)
		bodyUpper := strings.ToUpper(err.Error())

		// These errors mean restore is already done or in progress
		if strings.Contains(bodyUpper, "EXIT CODE 4") ||
			strings.Contains(bodyUpper, "SQL1005N") || // Database already exists
			strings.Contains(bodyUpper, "SQL1035N") || // Database in use
			strings.Contains(bodyUpper, "SQL6036N") || // DB2 starting
			strings.Contains(bodyUpper, "ALREADY EXISTS") ||
			strings.Contains(bodyUpper, "RESTORE IN PROGRESS") ||
			strings.Contains(bodyUpper, "COMMAND TERMINATED WITH EXIT CODE 4") { // Restore completed, waiting for logs
			klog.Infof("Restore of %s already done or in progress, treating as success", dbName)
			return true, nil
		}
	}

	if err != nil {
		return false, err
	}

	return false, fmt.Errorf("restore failed with status %d: %s", res.StatusCode(), res.String())
}

func (cc *Client) StartHadrOnPrimay(dbName string) (bool, error) {
	req := cc.Client.R().
		SetQueryParam("dbName", dbName)

	res, err := req.Get("/startHadrOnPrimary")
	if err != nil {
		if strings.Contains(err.Error(), "timeout") {
			klog.Warningf("StartHadrOnPrimary request timed out for %s, may still be processing", dbName)
			return true, nil
		}
		klog.Errorf("Failed to send http request for start HADR on primary: %v", err)
		return false, err
	}

	if res.RawResponse != nil && res.RawResponse.Body != nil {
		res.RawResponse.Body.Close()
	}

	return res.StatusCode() == http.StatusOK, nil
}
