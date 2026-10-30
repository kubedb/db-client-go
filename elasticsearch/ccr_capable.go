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

package elasticsearch

// CCR returns the cross-cluster replication client for this cluster.
func (es *ESClientV7) CCR() CCRClient { return NewCCRClient(es.client) }

// CCR returns the cross-cluster replication client for this cluster.
func (es *ESClientV8) CCR() CCRClient { return NewCCRClient(es.client) }

// CCR returns the cross-cluster replication client for this cluster.
func (es *ESClientV9) CCR() CCRClient { return NewCCRClient(es.client) }

var (
	_ CCRCapable = &ESClientV7{}
	_ CCRCapable = &ESClientV8{}
	_ CCRCapable = &ESClientV9{}
)
