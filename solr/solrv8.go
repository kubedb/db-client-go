package solr

import (
	"context"
	"fmt"
	"github.com/go-logr/logr"
	"github.com/go-resty/resty/v2"
	"strconv"
)

type SLClientV8 struct {
	Client *resty.Client
	log    logr.Logger
	Config *Config
}

func (sc *SLClientV8) GetClusterStatus() (*Response, error) {
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

func (sc *SLClientV8) ListCollection() (*Response, error) {
	sc.Config.log.V(5).Info("SEARCHING COLLECTION: kubedb-system")
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

func (sc *SLClientV8) CreateCollection() (*Response, error) {
	sc.Config.log.V(5).Info("CREATING COLLECTION: kubedb-system")
	req := sc.Client.R().SetDoNotParseResponse(true)
	req.SetHeader("Content-Type", "application/json")
	params := map[string]string{
		"action":            "CREATE",
		"name":              "kubedb-system",
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

func (sc *SLClientV8) WriteCollection() (*Response, error) {
	sc.Config.log.V(5).Info("WRITING COLLECTION: kubedb-system")
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
	res, err := req.Post("/solr/kubedb-system/update")
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

func (sc *SLClientV8) ReadCollection() (*Response, error) {
	sc.Config.log.V(5).Info("READING COLLECTION: kubedb-system")
	req := sc.Client.R().SetDoNotParseResponse(true)
	req.SetQueryParam("q", "*:*")
	res, err := req.Get("/solr/kubedb-system/select")
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

func (sc *SLClientV8) BackupCollection(ctx context.Context, collection string, backupName string, location string, repository string) (*Response, error) {
	sc.Config.log.V(5).Info(fmt.Sprintf("BACKUP COLLECTION: %s", collection))
	req := sc.Client.R().SetDoNotParseResponse(true).SetContext(ctx)
	req.SetHeader("Content-Type", "application/json")
	params := map[string]string{
		"action":     "BACKUP",
		"name":       backupName,
		"collection": collection,
		"location":   location,
		"repository": repository,
		"async":      fmt.Sprintf("%s-backup", collection),
	}

	req.SetQueryParams(params)

	res, err := req.Post("/solr/admin/collection")
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

func (sc *SLClientV8) RestoreCollection(ctx context.Context, collection string, backupName string, location string, repository string, backupId int) (*Response, error) {
	sc.Config.log.V(5).Info(fmt.Sprintf("RESTORE COLLECTION: %s", collection))
	req := sc.Client.R().SetDoNotParseResponse(true).SetContext(ctx)
	req.SetHeader("Content-Type", "application/json")
	params := map[string]string{
		"action":     "Restore",
		"name":       backupName,
		"collection": collection,
		"location":   location,
		"repository": repository,
		"backupId":   strconv.Itoa(backupId),
		"async":      fmt.Sprintf("%s-restore", collection),
	}

	req.SetQueryParams(params)

	res, err := req.Post("/solr/admin/collection")
	if err != nil {
		sc.log.Error(err, "Failed to send http request to restore a collection")
		return nil, err
	}

	backupResponse := &Response{
		Code:   res.StatusCode(),
		header: res.Header(),
		body:   res.RawBody(),
	}
	return backupResponse, nil
}

func (sc *SLClientV8) FlushStatus(asyncId string) (*Response, error) {
	sc.Config.log.V(5).Info("Flush Status")
	req := sc.Client.R().SetDoNotParseResponse(true)
	req.SetHeader("Content-Type", "application/json")

	res, err := req.Delete(fmt.Sprintf("/api/cluster/command-status/%s", asyncId))
	if err != nil {
		sc.log.Error(err, "Failed to send http request to flush status")
		return nil, err
	}

	backupResponse := &Response{
		Code:   res.StatusCode(),
		header: res.Header(),
		body:   res.RawBody(),
	}
	return backupResponse, nil
}

func (sc *SLClientV8) RequestStatus(asyncId string) (*Response, error) {
	sc.Config.log.V(5).Info("Request Status")
	req := sc.Client.R().SetDoNotParseResponse(true)
	req.SetHeader("Content-Type", "application/json")
	res, err := req.Get(fmt.Sprintf("/api/cluster/command-status/%s", asyncId))
	if err != nil {
		sc.log.Error(err, "Failed to send http request to request status")
		return nil, err
	}
	backupResponse := &Response{
		Code:   res.StatusCode(),
		header: res.Header(),
		body:   res.RawBody(),
	}
	return backupResponse, nil
}

func (sc *SLClientV8) DeleteBackup(ctx context.Context, backupName string, location string, repository string, backupId int) (*Response, error) {
	sc.Config.log.V(5).Info(fmt.Sprintf("DELETE BACKUP ID %d of BACKUP %s", backupId, backupName))
	req := sc.Client.R().SetDoNotParseResponse(true).SetContext(ctx)
	req.SetHeader("Content-Type", "application/json")

	params := map[string]string{
		"action":      "DELETEBACKUP",
		"name":        backupName,
		"location":    location,
		"repository":  repository,
		"backupId":    strconv.Itoa(backupId),
		"purgeUnused": "true",
	}
	req.SetQueryParams(params)

	res, err := req.Delete("/solr/admin/collections")
	if err != nil {
		sc.log.Error(err, "Failed to send http request to restore a collection")
		return nil, err
	}

	backupResponse := &Response{
		Code:   res.StatusCode(),
		header: res.Header(),
		body:   res.RawBody(),
	}
	return backupResponse, nil
}

func (sc *SLClientV8) PurgeBackup(ctx context.Context, backupName string, location string, repository string) (*Response, error) {
	sc.Config.log.V(5).Info(fmt.Sprintf("PURGE BACKUP ID %s", backupName))
	req := sc.Client.R().SetDoNotParseResponse(true).SetContext(ctx)
	req.SetHeader("Content-Type", "application/json")

	params := map[string]string{
		"action":      "DELETEBACKUP",
		"name":        backupName,
		"location":    location,
		"repository":  repository,
		"purgeUnused": "true",
	}
	req.SetQueryParams(params)
	res, err := req.Put("/solr/admin/collections")
	if err != nil {
		sc.log.Error(err, "Failed to send http request to restore a collection")
		return nil, err
	}

	backupResponse := &Response{
		Code:   res.StatusCode(),
		header: res.Header(),
		body:   res.RawBody(),
	}
	return backupResponse, nil
}

func (sc *SLClientV8) GetConfig() *Config {
	return sc.Config
}

func (sc *SLClientV8) GetClient() *resty.Client {
	return sc.Client
}

func (sc *SLClientV8) GetLog() logr.Logger {
	return sc.log
}
