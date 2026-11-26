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

// This file holds the DC-DR index protocol: the documents the hub orchestrator writes into, and
// the spoke fence agent reads from, each DC's own cluster (in index PermitIndex). Both the KubeDB
// Elasticsearch operator and the es-coordinator (which runs the fence) read and write these, so
// the contract lives here beside the RestClient that carries it.

import "time"

const (
	// PermitIndex is the index, on each DC's own cluster, where the hub records whether that DC
	// is the promoted write cluster (doc id PermitDocID), the fence agent records what it blocked
	// (FenceStateDocID), and the hub records a planned switchover (SwitchoverStateDocID).
	PermitIndex = "kubedb-dcdr"
	PermitDocID = "permit"
	// FenceStateDocID records which indices the fence agent blocked, so it only lifts its own
	// blocks.
	FenceStateDocID = "fence-state"
	// SwitchoverStateDocID is the doc id of SwitchoverState in PermitIndex.
	SwitchoverStateDocID = "switchover"

	// FenceAutoCreateValue is the action.auto_create_index value a fenced DC uses: KubeDB's own
	// indices may still be created (health check), every other index may not.
	FenceAutoCreateValue = "+kubedb-*,-*"
)

// IsProtectedIndex reports whether a user index name takes part in DC-DR replication: it is not a
// system/hidden index (leading ".") and not a per-cluster KubeDB index ("kubedb-" prefix).
func IsProtectedIndex(name string) bool {
	if len(name) == 0 {
		return false
	}
	if name[0] == '.' {
		return false
	}
	const kubedb = "kubedb-"
	if len(name) >= len(kubedb) && name[:len(kubedb)] == kubedb {
		return false
	}
	return true
}

// Permit is written by the hub into a DC's own cluster (index PermitIndex, doc PermitDocID)
// once that DC's indices have been promoted and verified as the write cluster. The spoke fence
// agent keeps the DC's leader indices write-blocked unless the marker freshly names the DC AND
// this permit names the DC for the same primary Lease.
//
// A permit is not enough on its own: a DC that returns after being replaced sees a marker that
// names the other DC and stays fenced. The hub also deletes the permit from every non-active DC
// it can reach. LeaseUID/LeaseTransitions identify the ownership term the permit was issued for,
// so a stale permit can be rejected once the marker carries the same fields.
type Permit struct {
	DC               string    `json:"dc"`
	LeaseName        string    `json:"leaseName"`
	LeaseUID         string    `json:"leaseUID,omitempty"`
	LeaseTransitions int32     `json:"leaseTransitions"`
	IssuedAt         time.Time `json:"issuedAt"`
}

// FenceState is the fence agent's own record (index PermitIndex, doc FenceStateDocID): which
// indices it write-blocked and the auto-create setting it replaced, so it lifts only its own
// blocks when it opens writes again.
type FenceState struct {
	Fenced         bool      `json:"fenced"`
	Reason         string    `json:"reason,omitempty"`
	BlockedIndices []string  `json:"blockedIndices,omitempty"`
	AutoCreateSet  bool      `json:"autoCreateSet,omitempty"`
	PrevAutoCreate string    `json:"prevAutoCreate,omitempty"`
	UpdatedAt      time.Time `json:"updatedAt"`
}

// SwitchoverState is written by the hub into the active DC's cluster while a planned switchover
// holds its leader indices write-blocked, so an abort lifts exactly those blocks.
type SwitchoverState struct {
	TargetDC       string    `json:"targetDC"`
	BlockedIndices []string  `json:"blockedIndices,omitempty"`
	Drained        bool      `json:"drained"`
	UpdatedAt      time.Time `json:"updatedAt"`
}
