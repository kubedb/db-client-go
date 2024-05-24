package solr

import (
	"context"
	"fmt"
)

func (sc *SLClient) GetClusterStatus() (*Response, error) {
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

func (sc *SLClient) ListCollection() (*Response, error) {
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

func (sc *SLClient) CreateCollection() (*Response, error) {
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

func (sc *SLClient) WriteCollection() (*Response, error) {
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

func (sc *SLClient) ReadCollection() (*Response, error) {
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

func (sc *SLClient) BackupCollection(ctx context.Context, collection string, backupName string, location string, repository string) (*Response, error) {
	sc.Config.log.V(5).Info(fmt.Sprintf("BACKUP COLLECTION: %s", collection))
	req := sc.Client.R().SetDoNotParseResponse(true).SetContext(ctx)
	req.SetHeader("Content-Type", "application/json")
	backupParams := &BackupParams{
		Location:   location,
		Repository: repository,
		//Async:      fmt.Sprintf("%s-backup", collection),
	}
	req.SetBody(backupParams)

	res, err := req.Post(fmt.Sprintf("/api/collections/%s/backups/%s/versions", collection, backupName))
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

func (sc *SLClient) RestoreCollection(ctx context.Context, collection string, backupName string, location string, repository string) (*Response, error) {
	sc.Config.log.V(5).Info(fmt.Sprintf("RESTORE COLLECTION: %s", collection))
	req := sc.Client.R().SetDoNotParseResponse(true).SetContext(ctx)
	req.SetHeader("Content-Type", "application/json")
	restoreParams := &RestoreParams{
		Location:   location,
		Repository: repository,
		Collection: collection,
		//Async:      fmt.Sprintf("%s-restore", collection),
	}
	req.SetBody(restoreParams)

	res, err := req.Post(fmt.Sprintf("/api/backups/%s/restore", backupName))
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

func (sc *SLClient) FlushStatus(asyncId string) (*Response, error) {
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

func (sc *SLClient) RequestStatus(asyncId string) (*Response, error) {
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
