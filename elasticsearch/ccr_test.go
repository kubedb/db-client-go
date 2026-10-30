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
	"io"
	"net/http"
	"strings"
	"testing"
)

type fakeCall struct {
	method, path, query, body string
}

type fakePerformer struct {
	calls  []fakeCall
	status int
	body   string
}

func (f *fakePerformer) Perform(req *http.Request) (*http.Response, error) {
	var b string
	if req.Body != nil {
		raw, _ := io.ReadAll(req.Body)
		b = string(raw)
	}
	f.calls = append(f.calls, fakeCall{req.Method, req.URL.Path, req.URL.RawQuery, b})
	status := f.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(f.body))}, nil
}

func TestPutRemoteClusterUsesProxyMode(t *testing.T) {
	f := &fakePerformer{body: `{"acknowledged":true}`}
	if err := NewCCRClient(f).PutRemoteCluster(context.TODO(), "dc-a", "10.2.0.146:30930"); err != nil {
		t.Fatal(err)
	}
	c := f.calls[0]
	if c.method != http.MethodPut || c.path != "/_cluster/settings" {
		t.Fatalf("unexpected call %+v", c)
	}
	var body struct {
		Persistent map[string]string `json:"persistent"`
	}
	if err := json.Unmarshal([]byte(c.body), &body); err != nil {
		t.Fatal(err)
	}
	if body.Persistent["cluster.remote.dc-a.mode"] != "proxy" || body.Persistent["cluster.remote.dc-a.proxy_address"] != "10.2.0.146:30930" {
		t.Fatalf("unexpected settings %v", body.Persistent)
	}
}

// The body Elasticsearch 8.19 returned in the two-VM CCR test when unfollow ran while the
// leader cluster was down: the index is unfollowed, only the leader-side lease removal failed.
const unfollowLeaderDown = `{"error":{"root_cause":[{"type":"no_seed_node_left_exception","reason":"Unable to open any proxy connections to cluster [dc-a] at address [10.2.0.146:30930]"}],"type":"exception","reason":"org.elasticsearch.transport.NoSeedNodeLeftException: Unable to open any proxy connections to cluster [dc-a] at address [10.2.0.146:30930]","failed_to_remove_retention_leases":"es-dc-b/orders/x-following-dc-a/orders/y"},"status":500}`

func TestUnfollowLeaderDownIsLeaseRemovalFailure(t *testing.T) {
	f := &fakePerformer{status: http.StatusInternalServerError, body: unfollowLeaderDown}
	res, err := NewCCRClient(f).Unfollow(context.TODO(), "orders")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !res.LeaseRemovalFailed {
		t.Fatal("expected LeaseRemovalFailed")
	}
	if f.calls[0].path != "/orders/_ccr/unfollow" {
		t.Fatalf("unexpected path %s", f.calls[0].path)
	}
}

func TestUnfollowOtherErrorIsReturned(t *testing.T) {
	f := &fakePerformer{status: http.StatusBadRequest, body: `{"error":{"type":"illegal_argument_exception","reason":"cannot convert the follower index [orders] to a non-follower, because it has not been paused"},"status":400}`}
	_, err := NewCCRClient(f).Unfollow(context.TODO(), "orders")
	if err == nil || !strings.Contains(err.Error(), "has not been paused") {
		t.Fatalf("expected the API error, got %v", err)
	}
}

func TestFollowStatsLag(t *testing.T) {
	f := &fakePerformer{body: `{"follow_stats":{"indices":[{"index":"orders","shards":[
		{"shard_id":0,"leader_global_checkpoint":343,"follower_global_checkpoint":340,"time_since_last_read_millis":12,"read_exceptions":[]},
		{"shard_id":1,"leader_global_checkpoint":355,"follower_global_checkpoint":355,"time_since_last_read_millis":30000,"read_exceptions":[{"from_seq_no":356}]}]}]}}`}
	st, err := NewCCRClient(f).FollowStats(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if len(st) != 1 || len(st[0].Shards) != 2 {
		t.Fatalf("unexpected stats %+v", st)
	}
	if st[0].Shards[0].Lag() != 3 || st[0].Shards[1].Lag() != 0 {
		t.Fatalf("unexpected lag %d %d", st[0].Shards[0].Lag(), st[0].Shards[1].Lag())
	}
	if len(st[0].Shards[1].ReadExceptions) != 1 {
		t.Fatal("expected a read exception")
	}
}

func TestDeleteAutoFollowPatternMissingIsNotError(t *testing.T) {
	f := &fakePerformer{status: http.StatusNotFound, body: `{"error":{"type":"resource_not_found_exception","reason":"auto-follow pattern [x] is missing"},"status":404}`}
	if err := NewCCRClient(f).DeleteAutoFollowPattern(context.TODO(), "x"); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestSetWriteBlockOffSendsNull(t *testing.T) {
	f := &fakePerformer{body: `{"acknowledged":true}`}
	if err := NewCCRClient(f).SetWriteBlock(context.TODO(), []string{"a", "b"}, false); err != nil {
		t.Fatal(err)
	}
	if f.calls[0].path != "/a,b/_settings" || f.calls[0].body != `{"index.blocks.write":null}` {
		t.Fatalf("unexpected call %+v", f.calls[0])
	}
}

func TestListUserIndicesSkipsDotIndices(t *testing.T) {
	f := &multiPerformer{responses: []string{
		`{"orders":{"settings":{"index.uuid":"u1","index.xpack.ccr.following_index":"true"}},".kibana":{"settings":{"index.uuid":"u2"}},"logs":{"settings":{"index.uuid":"u3","index.blocks.write":"true"}}}`,
		`[{"index":"orders","status":"open"},{"index":"logs","status":"close"}]`,
	}}
	got, err := NewCCRClient(f).ListUserIndices(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Name != "logs" || !got[0].WriteBlocked || !got[0].Closed || got[1].Name != "orders" || !got[1].Follower {
		t.Fatalf("unexpected indices %+v", got)
	}
}

type multiPerformer struct {
	responses []string
	n         int
}

func (m *multiPerformer) Perform(*http.Request) (*http.Response, error) {
	b := m.responses[m.n]
	m.n++
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(b))}, nil
}

func TestPrimaryGlobalCheckpoints(t *testing.T) {
	f := &fakePerformer{body: `{"indices":{"orders":{"shards":{"0":[{"routing":{"primary":true},"seq_no":{"global_checkpoint":343}},{"routing":{"primary":false},"seq_no":{"global_checkpoint":340}}],"1":[{"routing":{"primary":true},"seq_no":{"global_checkpoint":355}}]}}}}`}
	got, err := NewCCRClient(f).PrimaryGlobalCheckpoints(context.TODO(), []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}
	if got["orders"][0] != 343 || got["orders"][1] != 355 {
		t.Fatalf("unexpected checkpoints %v", got)
	}
}
