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

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type routeReply struct {
	status int
	body   string
}

// routePerformer answers by "METHOD path" and records every call.
type routePerformer struct {
	routes map[string]routeReply
	calls  []fakeCall
}

func (r *routePerformer) Perform(req *http.Request) (*http.Response, error) {
	var b string
	if req.Body != nil {
		raw, _ := io.ReadAll(req.Body)
		b = string(raw)
	}
	r.calls = append(r.calls, fakeCall{req.Method, req.URL.Path, req.URL.RawQuery, b})
	rep, ok := r.routes[req.Method+" "+req.URL.Path]
	if !ok {
		rep = routeReply{http.StatusNotFound, `{"error":{"type":"not_found","reason":"no route"}}`}
	}
	if rep.status == 0 {
		rep.status = http.StatusOK
	}
	return &http.Response{StatusCode: rep.status, Body: io.NopCloser(strings.NewReader(rep.body))}, nil
}

func (r *routePerformer) called(methodPath string) []fakeCall {
	var out []fakeCall
	for _, c := range r.calls {
		if c.method+" "+c.path == methodPath {
			out = append(out, c)
		}
	}
	return out
}

var osRoles = OpenSearchReplicationOptions{LeaderRole: "leader_role", FollowerRole: "follower_role"}

// Bodies recorded from OpenSearch 2.19.2 (see the DC-DR POC notes).
const (
	osSettings = `{"orders":{"settings":{"index.uuid":"u1","index.plugins.replication.follower.leader_index":"dc-a:orders","index.replication.type":"DOCUMENT"}},
"app-1":{"settings":{"index.uuid":"u2","index.blocks.write":"true"}},
"security-auditlog-2026.11.02":{"settings":{"index.uuid":"u3"}},
"top_queries-2026.11.02-89252":{"settings":{"index.uuid":"u4"}},
".opendistro_security":{"settings":{"index.uuid":"u5"}}}`
	osFollowerStats = `{"num_syncing_indices":1,"follower_checkpoint":3,"leader_checkpoint":5,
"index_stats":{"orders":{"operations_written":5,"operations_read":5,"failed_read_requests":0,"follower_checkpoint":3,"leader_checkpoint":5}}}`
	osOrdersStats = `{"indices":{"orders":{"shards":{"0":[{"routing":{"primary":true},"seq_no":{"global_checkpoint":3}}],"1":[{"routing":{"primary":true},"seq_no":{"global_checkpoint":0}}]}}}}`
)

func TestOpenSearchFollowSendsUseRoles(t *testing.T) {
	r := &routePerformer{routes: map[string]routeReply{"PUT /_plugins/_replication/orders/_start": {body: `{"acknowledged":true}`}}}
	if err := NewOpenSearchReplicationClient(r, osRoles).Follow(context.TODO(), "orders", "dc-a", "orders"); err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	_ = json.Unmarshal([]byte(r.calls[0].body), &body)
	roles, _ := body["use_roles"].(map[string]any)
	if body["leader_alias"] != "dc-a" || body["leader_index"] != "orders" || roles["leader_cluster_role"] != "leader_role" || roles["follower_cluster_role"] != "follower_role" {
		t.Fatalf("unexpected _start body %s", r.calls[0].body)
	}
}

func TestOpenSearchFollowWithoutSecurityOmitsUseRoles(t *testing.T) {
	r := &routePerformer{routes: map[string]routeReply{"PUT /_plugins/_replication/orders/_start": {}}}
	if err := NewOpenSearchReplicationClient(r, OpenSearchReplicationOptions{}).Follow(context.TODO(), "orders", "dc-a", "orders"); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(r.calls[0].body, "use_roles") {
		t.Fatalf("use_roles sent without security: %s", r.calls[0].body)
	}
}

func TestOpenSearchListUserIndicesDetectsFollowersAndSkipsSystem(t *testing.T) {
	r := &routePerformer{routes: map[string]routeReply{
		"GET /*/_settings":  {body: osSettings},
		"GET /_cat/indices": {body: `[{"index":"orders","status":"open"},{"index":"app-1","status":"open"}]`},
	}}
	got, err := NewOpenSearchReplicationClient(r, osRoles).ListUserIndices(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Name != "app-1" || got[0].Follower || !got[0].WriteBlocked || got[1].Name != "orders" || !got[1].Follower {
		t.Fatalf("unexpected indices %+v", got)
	}
}

func TestOpenSearchFollowInfoAndStats(t *testing.T) {
	r := &routePerformer{routes: map[string]routeReply{
		"GET /*/_settings":                          {body: osSettings},
		"GET /_plugins/_replication/follower_stats": {body: osFollowerStats},
		"GET /orders/_stats":                        {body: osOrdersStats},
	}}
	c := NewOpenSearchReplicationClient(r, osRoles)
	info, err := c.FollowInfo(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if len(info) != 1 || info[0] != (FollowerInfo{FollowerIndex: "orders", RemoteCluster: "dc-a", LeaderIndex: "orders", Status: "active"}) {
		t.Fatalf("unexpected follow info %+v", info)
	}
	stats, err := c.FollowStats(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 1 || len(stats[0].Shards) != 2 {
		t.Fatalf("unexpected stats %+v", stats)
	}
	s0, s1 := stats[0].Shards[0], stats[0].Shards[1]
	if s0.FollowerGlobalCheckpoint != 3 || s0.Lag() != 2 || s1.FollowerGlobalCheckpoint != 0 || s1.Lag() != 0 || s0.RemoteCluster != "dc-a" {
		t.Fatalf("unexpected shard stats %+v %+v", s0, s1)
	}
}

func TestOpenSearchPausedFollowerIsReportedPaused(t *testing.T) {
	r := &routePerformer{routes: map[string]routeReply{
		"GET /*/_settings":                          {body: osSettings},
		"GET /_plugins/_replication/follower_stats": {body: `{"num_syncing_indices":0,"index_stats":{}}`},
	}}
	info, err := NewOpenSearchReplicationClient(r, osRoles).FollowInfo(context.TODO())
	if err != nil || len(info) != 1 || info[0].Status != "paused" {
		t.Fatalf("expected paused follower, got %+v %v", info, err)
	}
}

func TestOpenSearchPromoteStopsEachFollower(t *testing.T) {
	r := &routePerformer{routes: map[string]routeReply{
		"POST /_plugins/_replication/a/_stop": {body: `{"acknowledged":true}`},
		"POST /_plugins/_replication/b/_stop": {status: http.StatusBadRequest, body: `{"error":{"type":"illegal_argument_exception","reason":"No replication in progress for index:b"},"status":400}`},
	}}
	failed, err := NewOpenSearchReplicationClient(r, osRoles).Promote(context.TODO(), []string{"a", "b"})
	if err != nil || len(failed) != 0 {
		t.Fatalf("promote: %v %v", failed, err)
	}
	if len(r.called("POST /_plugins/_replication/a/_stop")) != 1 || len(r.calls) != 2 {
		t.Fatalf("unexpected calls %+v", r.calls)
	}
}

func TestOpenSearchAutoFollowRefusesPatternThatMatchesExclusions(t *testing.T) {
	r := &routePerformer{}
	err := NewOpenSearchReplicationClient(r, osRoles).PutAutoFollowPattern(context.TODO(), "search-from-dc-a", AutoFollowPattern{
		RemoteCluster: "dc-a", LeaderIndexPatterns: []string{"*"}, LeaderIndexExclusionPatterns: []string{".*", "kubedb-*"},
	})
	if !errors.Is(err, ErrAutoFollowUnsupported) || len(r.calls) != 0 {
		t.Fatalf("expected ErrAutoFollowUnsupported without calls, got %v %+v", err, r.calls)
	}
	for _, p := range []string{"kubedb-dcdr*", "security-auditlog-*", "top_*"} {
		if err := NewOpenSearchReplicationClient(r, osRoles).PutAutoFollowPattern(context.TODO(), "x", AutoFollowPattern{RemoteCluster: "dc-a", LeaderIndexPatterns: []string{p}, LeaderIndexExclusionPatterns: []string{"kubedb-*"}}); !errors.Is(err, ErrAutoFollowUnsupported) {
			t.Fatalf("pattern %q should be refused, got %v", p, err)
		}
	}
}

func TestOpenSearchAutoFollowPutAndGet(t *testing.T) {
	r := &routePerformer{routes: map[string]routeReply{
		"GET /_plugins/_replication/autofollow_stats": {body: `{"autofollow_stats":[]}`},
		"POST /_plugins/_replication/_autofollow":     {body: `{"acknowledged":true}`},
	}}
	c := NewOpenSearchReplicationClient(r, osRoles)
	if err := c.PutAutoFollowPattern(context.TODO(), "search-from-dc-a", AutoFollowPattern{RemoteCluster: "dc-a", LeaderIndexPatterns: []string{"app-*"}, LeaderIndexExclusionPatterns: []string{".*", "kubedb-*"}}); err != nil {
		t.Fatal(err)
	}
	post := r.called("POST /_plugins/_replication/_autofollow")
	if len(post) != 1 || !strings.Contains(post[0].body, `"pattern":"app-*"`) || !strings.Contains(post[0].body, `"leader_alias":"dc-a"`) || !strings.Contains(post[0].body, "use_roles") {
		t.Fatalf("unexpected autofollow call %+v", post)
	}
	r.routes["GET /_plugins/_replication/autofollow_stats"] = routeReply{body: `{"autofollow_stats":[{"name":"search-from-dc-a","pattern":"app-*","num_success_start_replication":3}]}`}
	p, active, found, err := c.GetAutoFollowPattern(context.TODO(), "search-from-dc-a")
	if err != nil || !found || !active || p.LeaderIndexPatterns[0] != "app-*" {
		t.Fatalf("unexpected get %+v %v %v %v", p, active, found, err)
	}
}

func TestOpenSearchDeleteAutoFollowTriesRegisteredRemotes(t *testing.T) {
	r := &routePerformer{routes: map[string]routeReply{
		"GET /_plugins/_replication/autofollow_stats": {body: `{"autofollow_stats":[{"name":"search-from-dc-a","pattern":"app-*"}]}`},
		"GET /_remote/info":                           {body: `{"dc-a":{"connected":true,"mode":"proxy"}}`},
		"DELETE /_plugins/_replication/_autofollow":   {body: `{"acknowledged":true}`},
	}}
	if err := NewOpenSearchReplicationClient(r, osRoles).DeleteAutoFollowPattern(context.TODO(), "search-from-dc-a"); err != nil {
		t.Fatal(err)
	}
	del := r.called("DELETE /_plugins/_replication/_autofollow")
	if len(del) != 1 || !strings.Contains(del[0].body, `"leader_alias":"dc-a"`) {
		t.Fatalf("unexpected delete %+v", del)
	}
	// Missing rules are not an error and cost no delete call.
	r2 := &routePerformer{routes: map[string]routeReply{"GET /_plugins/_replication/autofollow_stats": {body: `{"autofollow_stats":[]}`}}}
	if err := NewOpenSearchReplicationClient(r2, osRoles).DeleteAutoFollowPattern(context.TODO(), "nope"); err != nil || len(r2.calls) != 1 {
		t.Fatalf("missing rule: %v %+v", err, r2.calls)
	}
}

func TestElasticPromoteClosesUnfollowsAndOpens(t *testing.T) {
	r := &routePerformer{routes: map[string]routeReply{
		"POST /a,b/_close":      {},
		"POST /a/_ccr/unfollow": {},
		"POST /b/_ccr/unfollow": {status: http.StatusInternalServerError, body: `{"error":{"type":"failed_to_remove_retention_leases","reason":"leader unreachable"}}`},
		"POST /a,b/_open":       {},
	}}
	failed, err := NewCCRClient(r).Promote(context.TODO(), []string{"a", "b"})
	if err != nil || len(failed) != 1 || failed[0] != "b" {
		t.Fatalf("promote: %v %v", failed, err)
	}
	if r.calls[0].path != "/a,b/_close" || r.calls[len(r.calls)-1].path != "/a,b/_open" {
		t.Fatalf("unexpected order %+v", r.calls)
	}
}
