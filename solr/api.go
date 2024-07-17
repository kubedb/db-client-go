package solr

import (
	"context"

	"github.com/go-logr/logr"
	"github.com/go-resty/resty/v2"
)

const (
	writeCollectionName = "kubedb-system"
	Action              = "action"
	ActionBackup        = "BACKUP"
	ActionRestore       = "RESTORE"
	ActionDeleteBackup  = "DELETEBACKUP"
	BackupName          = "name"
	Location            = "location"
	Repository          = "repository"
	Collection          = "collection"
	Async               = "async"
	PurgeUnused         = "purgeUnused"
	BackupId            = "backupId"
	DeleteStatus        = "DELETESTATUS"
	RequestStatus       = "REQUESTSTATUS"
	RequestId           = "requestid"
)

type SLClient interface {
	GetClusterStatus() (*Response, error)
	ListCollection() (*Response, error)
	CreateCollection() (*Response, error)
	WriteCollection() (*Response, error)
	ReadCollection() (*Response, error)
	BackupCollection(ctx context.Context, collection string, backupName string, location string, repository string) (*Response, error)
	RestoreCollection(ctx context.Context, collection string, backupName string, location string, repository string, backupId int) (*Response, error)
	FlushStatus(asyncId string) (*Response, error)
	RequestStatus(asyncId string) (*Response, error)
	DeleteBackup(ctx context.Context, backupName string, collection string, location string, repository string, backupId int, snap string) (*Response, error)
	PurgeBackup(ctx context.Context, backupName string, collection string, location string, repository string, snap string) (*Response, error)
	GetConfig() *Config
	GetClient() *resty.Client
	GetLog() logr.Logger
	DecodeBackupResponse(data map[string]interface{}, collection string) ([]byte, error)
}
