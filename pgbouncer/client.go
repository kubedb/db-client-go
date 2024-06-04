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

package pgbouncer

import (
	"context"
	"database/sql"
	"sync"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	"xorm.io/xorm"
)

type Client struct {
	*sql.DB
}

type XormClient struct {
	*xorm.Engine
}

type XormClientList struct {
	List  []*XormClient
	Mutex sync.Mutex
	WG    sync.WaitGroup

	context context.Context
	pb      *api.PgBouncer
	auth    *Auth
	dbName  string
}
