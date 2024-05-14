package solr

import (
	"context"
	"fmt"
)

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

func (sc *SLClient) ListCollection() (*Response, error) {
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

func (sc *SLClient) BackupRestoreCollection(ctx context.Context, action string, collection string, backupName string, location string, repository string) (*Response, error) {
	sc.Config.log.V(5).Info(fmt.Sprintf("BACKUP COLLECTION: %s", collection))
	req := sc.Client.R().SetContext(ctx)
	params := map[string]string{
		"action":     action,
		"collection": collection,
		"name":       backupName,
		"location":   location,
		"repository": repository,
	}
	req.SetQueryParams(params)
	res, err := req.Get("/solr/admin/collections")
	if err != nil {
		sc.log.Error(err, "Failed to send http request to backup a collection")
		return nil, err
	}

	backupResponse := &Response{
		Code:   res.StatusCode(),
		header: res.Header(),
		body:   res.RawBody(),
	}
	return backupResponse, nil
}
