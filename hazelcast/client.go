/*
Copyright AppsCode Inc. and Contributors

Licensed under the AppsCode Community License 1.0.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://github.com/appscode/licenses/raw/1.0.0/AppsCode-Community-1.0.0.md

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package hazelcast

import (
	"github.com/go-resty/resty/v2"
	hazelcast "github.com/hazelcast/hazelcast-go-client"
)

type Client struct {
	*hazelcast.Client
}

type HZRestyClient struct {
	Client   *resty.Client
	Config   *RestyConfig
	password string
}
