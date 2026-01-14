package db2

import (
	"net/http"
	"strconv"

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

func (cc *Client) ConfigurePrimary(dbName string, primaryPod int, standbyPod int) (bool, error) {
	req := cc.Client.R().
		SetDoNotParseResponse(true).
		SetQueryParam("dbName", dbName).
		SetQueryParam("primaryPod", strconv.Itoa(primaryPod)).
		SetQueryParam("standbyPod", strconv.Itoa(standbyPod))

	res, err := req.Get("/primary")
	if err != nil {
		klog.Error(err, "Failed to send http request")
		return false, err
	}
	return res.StatusCode() == http.StatusOK, nil
}

func (cc *Client) ConfigureStandby(dbName string, primaryPod int, standbyPod int) (bool, error) {
	req := cc.Client.R().
		SetDoNotParseResponse(true).
		SetQueryParam("dbName", dbName).
		SetQueryParam("primaryPod", strconv.Itoa(primaryPod)).
		SetQueryParam("standbyPod", strconv.Itoa(standbyPod))

	res, err := req.Get("/standby")
	if err != nil {
		klog.Error(err, "Failed to send http request")
		return false, err
	}
	return res.StatusCode() == http.StatusOK, nil
}
func (cc *Client) StartHADR(dbName string) (bool, error) {
	req := cc.Client.R().
		SetDoNotParseResponse(true).
		SetQueryParam("dbName", dbName)

	res, err := req.Get("/starthadr")
	if err != nil {
		klog.Error(err, "Failed to send http request")
		return false, err
	}
	return res.StatusCode() == http.StatusOK, nil
}
