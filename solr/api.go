/*
Copyright AppsCode Inc. and Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
	ActionDelete        = "DELETE"
	ActionCreate        = "CREATE"
	ActionDeleteBackup  = "DELETEBACKUP"
	AddRole             = "ADDROLE"
	RemoveRole          = "REMOVEROLE"
	Name                = "name"
	Role                = "role"
	Node                = "node"
	Location            = "location"
	Repository          = "repository"
	Collection          = "collection"
	Async               = "async"
	Replica             = "replica"
	MoveReplica         = "MOVEREPLICA"
	PurgeUnused         = "purgeUnused"
	TargetNode          = "targetNode"
	BackupId            = "backupId"
	DeleteStatus        = "DELETESTATUS"
	RequestStatus       = "REQUESTSTATUS"
	RequestId           = "requestid"
	NumShards           = "numShards"
	ReplicationFactor   = "replicationFactor"
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
	MoveReplica(target string, replica string, collection string, async string) (*Response, error)
	BalanceReplica(async string) (*Response, error)
	AddRole(role, node string) (*Response, error)
	RemoveRole(role, node string) (*Response, error)
	DeleteCollection(name string) (*Response, error)
}
