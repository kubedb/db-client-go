package solr

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

func (sc *SLClient) getResponseStatus(responseBody map[string]interface{}) (int, error) {
	err, ok := responseBody["error"].(map[string]interface{})
	if ok {
		msg, ok := err["msg"].(string)
		if !ok {
			return -1, errors.New("no msg found in error message while getting response status")

		}
		code, ok := err["code"].(float64)
		if !ok {
			return -1, errors.New("error occurred but didn't found error code while getting response status")

		}
		return -1, errors.New(fmt.Sprintf("Error: %v with code %d", msg, int(code)))
	}
	responseHeader, ok := responseBody["responseHeader"].(map[string]interface{})
	if !ok {
		return -1, errors.New("didn't find responseHeader")
	}

	status, ok := responseHeader["status"].(float64)
	if !ok {
		return -1, errors.New("didn't find status")
	}

	if int(status) != 0 {
		msg, ok := responseBody["message"].(string)
		if !ok {
			return -1, errors.New("no msg found in error message")

		}
		return -1, errors.New(fmt.Sprintf("Error: %v with code %d", msg, int(status)))
	}
	return int(status), nil
}

func (sc *SLClient) GetClusterStatus() (*Response, error) {
	sc.Config.log.V(5).Info("GETTING CLUSTER STATUS")
	req := sc.Client.R().SetDoNotParseResponse(true)
	req.SetQueryParam("action", "CLUSTERSTATUS")
	res, err := req.Get("/solr/admin/collections")
	if err != nil {
		sc.log.Error(err, "Failed to send http request")
		return nil, err
	}

	clusterResponse := &Response{
		Code:   res.StatusCode(),
		header: res.Header(),
		body:   res.RawBody(),
	}
	return clusterResponse, nil
}

func (sc *SLClient) GetStateFromClusterResponse(responseStatus *Response) (int, error) {
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			err1 := errors.Wrap(err, "failed to parse response body")
			if err1 != nil {
				return
			}
			return
		}
	}(responseStatus.body)

	responseBody := make(map[string]interface{})
	if err := json.NewDecoder(responseStatus.body).Decode(&responseBody); err != nil {
		return -1, fmt.Errorf("failed to deserialize the response: %v", err)
	}

	status, err := sc.getResponseStatus(responseBody)
	if err != nil {
		return status, err
	}

	clusterInfo, ok := responseBody["cluster"].(map[string]interface{})
	if !ok {
		return -1, errors.New("didn't find cluster")
	}
	collections, ok := clusterInfo["collections"].(map[string]interface{})
	if !ok {
		return -1, errors.New("didn't find collections")
	}
	for name, info := range collections {
		collectionInfo := info.(map[string]interface{})
		health, ok := collectionInfo["health"].(string)
		if !ok {
			return -1, errors.New("didn't find health")
		}
		if health != "GREEN" {
			sc.Config.log.Error(errors.New(""), fmt.Sprintf("STATUS IS %d AND HEALTH IS NOT GREEN", status))
			return -1, errors.New(fmt.Sprintf("health for collection %s is not green", name))
		}
	}
	return status, nil
}

func (sc *SLClient) SearchCollection() (*Response, error) {
	sc.Config.log.V(5).Info("SEARCHING COLLECTION: kubedb-collection")
	req := sc.Client.R().SetDoNotParseResponse(true)
	req.SetQueryParam("action", "LIST")
	res, err := req.Get("/solr/admin/collections")
	if err != nil {
		sc.log.Error(err, "Failed to send http request while getting colection list")
		return nil, err
	}
	response := &Response{
		Code:   res.StatusCode(),
		header: res.Header(),
		body:   res.RawBody(),
	}
	return response, nil
}

func (sc *SLClient) DecodeSearchCollectionResponse(response *Response) (bool, error) {
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

	responseBody := make(map[string]interface{})
	if err := json.NewDecoder(response.body).Decode(&responseBody); err != nil {
		return false, fmt.Errorf("failed to deserialize the response: %v", err)
	}

	status, errr := sc.getResponseStatus(responseBody)
	if errr != nil {
		return false, errr
	}

	if status != 0 {
		return false, errors.New("status is non zero")
	}

	collectionList, ok := responseBody["collections"].([]interface{})
	if !ok {
		return false, errors.New("didn't find collection list")
	}

	for idx := range collectionList {
		if collectionList[idx].(string) == "kubedb-collection" {
			return true, nil
		}
	}

	return false, nil
}

func (sc *SLClient) CreateCollection() (*Response, error) {
	sc.Config.log.V(5).Info("CREATING COLLECTION: kubedb-collection")
	req := sc.Client.R().SetDoNotParseResponse(true)
	params := map[string]string{
		"action":            "CREATE",
		"name":              "kubedb-collection",
		"numShards":         "1",
		"replicationFactor": "1",
	}

	req.SetQueryParams(params)
	res, err := req.Post("/solr/admin/collections")
	if err != nil {
		sc.log.Error(err, "Failed to send http request to create a collection")
		return nil, err
	}

	collectionResponse := &Response{
		Code:   res.StatusCode(),
		header: res.Header(),
		body:   res.RawBody(),
	}
	return collectionResponse, nil
}

type ADDList []ADD

func (sc *SLClient) WriteCollection() (*Response, error) {
	sc.Config.log.V(5).Info("WRITING COLLECTION: kubedb-collection")
	req := sc.Client.R().SetDoNotParseResponse(true)
	req.SetHeader("Content-Type", "application/json")
	data1 := &Data{
		CommitWithin: 5000,
		Overwrite:    true,
		Doc: &Doc{
			Id: 1,
			DB: "elasticsearch",
		},
	}
	add := ADD{
		Add: data1,
	}
	req.SetBody(add)
	res, err := req.Post("/solr/kubedb-collection/update")
	if err != nil {
		sc.log.Error(err, "Failed to send http request to add document in collect")
		return nil, err
	}

	writeResponse := &Response{
		Code:   res.StatusCode(),
		header: res.Header(),
		body:   res.RawBody(),
	}
	return writeResponse, nil
}

func (sc *SLClient) ReadCollection() (*Response, error) {
	sc.Config.log.V(5).Info("READING COLLECTION: kubedb-collection")
	req := sc.Client.R().SetDoNotParseResponse(true)
	//req.SetHeader("Content-Type", "application/json")
	//queryParams := QueryParams{
	//	Query: "*:*",
	//	Limit: 10,
	//}
	//req.SetBody(queryParams)
	req.SetQueryParam("q", "*:*")
	res, err := req.Get("/solr/kubedb-collection/select")
	if err != nil {
		sc.log.Error(err, "Failed to send http request to read a collection")
		return nil, err
	}

	writeResponse := &Response{
		Code:   res.StatusCode(),
		header: res.Header(),
		body:   res.RawBody(),
	}
	return writeResponse, nil
}

func (sc *SLClient) DecodeReadWriteResponse(response *Response) (int, error) {
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

	responseBody := make(map[string]interface{})
	if err := json.NewDecoder(response.body).Decode(&responseBody); err != nil {
		return -1, fmt.Errorf("failed to deserialize the response: %v", err)
	}

	status, err := sc.getResponseStatus(responseBody)

	return status, err
}
