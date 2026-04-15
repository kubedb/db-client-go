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

package neo4j

import (
	"context"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func (c *Client) ExecuteQuery(ctx context.Context, query string, params map[string]any, dbName string) (*neo4j.EagerResult, error) {
	return neo4j.ExecuteQuery(ctx, c, query, params, neo4j.EagerResultTransformer,
		neo4j.ExecuteQueryWithDatabase(dbName))
}
