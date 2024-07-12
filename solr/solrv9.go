package solr

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/go-logr/logr"
	"github.com/go-resty/resty/v2"
	"k8s.io/klog/v2"
)

type SLClientV9 struct {
	Client *resty.Client
	log    logr.Logger
	Config *Config
}

func (sc *SLClientV9) GetClusterStatus() (*Response, error) {
	sc.Config.log.V(5).Info("GETTING CLUSTER STATUS")
	req := sc.Client.R().SetDoNotParseResponse(true)
	res, err := req.Get("/api/cluster")
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

func (sc *SLClientV9) ListCollection() (*Response, error) {
	sc.Config.log.V(5).Info("SEARCHING COLLECTION: kubedb-system")
	req := sc.Client.R().SetDoNotParseResponse(true)
	res, err := req.Get("/api/collections")
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

func (sc *SLClientV9) CreateCollection() (*Response, error) {
	sc.Config.log.V(5).Info("CREATING COLLECTION: kubedb-system")
	req := sc.Client.R().SetDoNotParseResponse(true)
	req.SetHeader("Content-Type", "application/json")
	createParams := &CreateParams{
		Name:              "kubedb-system",
		NumShards:         1,
		ReplicationFactor: 1,
	}

	req.SetBody(createParams)
	res, err := req.Post("/api/collections")
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

func (sc *SLClientV9) WriteCollection() (*Response, error) {
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

func (sc *SLClientV9) ReadCollection() (*Response, error) {
	sc.Config.log.V(5).Info("READING COLLECTION: kubedb-system")
	req := sc.Client.R().SetDoNotParseResponse(true)
	req.SetHeader("Content-Type", "application/json")
	queryParams := QueryParams{
		Query: "*:*",
		Limit: 10,
	}
	req.SetBody(queryParams)
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

func (sc *SLClientV9) BackupCollection(ctx context.Context, collection string, backupName string, location string, repository string) (*Response, error) {
	sc.Config.log.V(5).Info(fmt.Sprintf("BACKUP COLLECTION v11111111111111111: %s", collection))
	req := sc.Client.R().SetDoNotParseResponse(true).SetContext(ctx)
	req.SetHeader("Content-Type", "application/json")
	backupParams := map[string]string{
		"action":     "BACKUP",
		"name":       backupName,
		"location":   location,
		"repository": repository,
		"async":      fmt.Sprintf("%s-backup", collection),
	}
	req.SetQueryParams(backupParams)

	res, err := req.Post("/solr/admin/collections")
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

func (sc *SLClientV9) RestoreCollection(ctx context.Context, collection string, backupName string, location string, repository string, backupId int) (*Response, error) {
	sc.Config.log.V(5).Info(fmt.Sprintf("RESTORE COLLECTION: %s", collection))
	req := sc.Client.R().SetDoNotParseResponse(true).SetContext(ctx)
	req.SetHeader("Content-Type", "application/json")
	restoreParams := map[string]string{
		"action":     "RESTORE",
		"name":       backupName,
		"location":   location,
		"repository": repository,
		"async":      fmt.Sprintf("%s-restore", collection),
	}
	req.SetQueryParams(restoreParams)

	res, err := req.Post("/solr/admin/collections")
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

func (sc *SLClientV9) FlushStatus(asyncId string) (*Response, error) {
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

func (sc *SLClientV9) RequestStatus(asyncId string) (*Response, error) {
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

func (sc *SLClientV9) DeleteBackup(ctx context.Context, backupName string, collection string, location string, repository string, backupId int, snap string) (*Response, error) {
	sc.Config.log.V(5).Info(fmt.Sprintf("DELETE BACKUP ID %d of BACKUP %s", backupId, backupName))
	req := sc.Client.R().SetDoNotParseResponse(true).SetContext(ctx)
	req.SetHeader("Content-Type", "application/json")
	async := fmt.Sprintf("%s-delete", collection)
	if snap != "" {
		async = fmt.Sprintf("%s-%s", async, snap)
	}
	params := map[string]string{
		"action":     "DELETEBACKUP",
		"name":       backupName,
		"location":   location,
		"repository": repository,
		"backupId":   strconv.Itoa(backupId),
		"async":      async,
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

func (sc *SLClientV9) PurgeBackup(ctx context.Context, backupName string, collection string, location string, repository string, snap string) (*Response, error) {
	sc.Config.log.V(5).Info(fmt.Sprintf("PURGE BACKUP ID %s", backupName))
	req := sc.Client.R().SetDoNotParseResponse(true).SetContext(ctx)
	req.SetHeader("Content-Type", "application/json")
	async := fmt.Sprintf("%s-purge", collection)
	if snap != "" {
		async = fmt.Sprintf("%s-%s", async, snap)
	}
	params := map[string]string{
		"action":      "DELETEBACKUP",
		"name":        backupName,
		"location":    location,
		"repository":  repository,
		"purgeUnused": "true",
		"async":       async,
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

func (sc *SLClientV9) GetConfig() *Config {
	return sc.Config
}

func (sc *SLClientV9) GetClient() *resty.Client {
	return sc.Client
}

func (sc *SLClientV9) GetLog() logr.Logger {
	return sc.log
}

func (sc *SLClientV9) DecodeBackupResponse(data map[string]interface{}, collection string) ([]byte, error) {
	sc.Config.log.V(5).Info("Decode Backup Data")
	backupResponse, ok := data["response"].(map[string]interface{})
	if !ok {
		err := fmt.Errorf("didn't find status for collection %s\n", collection)
		return nil, err
	}
	klog.Info("backup response ", backupResponse)
	b, err := json.Marshal(backupResponse)
	if err != nil {
		klog.Error(fmt.Sprintf("Could not format response for collection %s into json", collection))
		return nil, err
	}
	klog.Info(fmt.Sprintf("Response for collection %s\n%v", collection, string(b)))
	return b, nil
}
