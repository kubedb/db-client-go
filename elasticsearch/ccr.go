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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

// CCRCapable is implemented by the Elastic Stack clients (7.x, 8.x, 9.x) that support
// Elastic cross-cluster replication (CCR). Callers type-assert an ESClient to it:
//
//	if c, ok := client.ESClient.(CCRCapable); ok { ccr := c.CCR() }
//
// OpenSearch replicates through a different plugin API and does not implement it.
type CCRCapable interface {
	CCR() CCRClient
}

// CCRClient is the REST surface used to set up, observe and promote cross-cluster
// replication between two Elasticsearch clusters. Every call is issued against the cluster
// the client was built for.
type CCRClient interface {
	// ClusterUUID returns the cluster UUID reported by GET /.
	ClusterUUID(ctx context.Context) (string, error)

	// PutRemoteCluster registers (or updates) a remote cluster in proxy mode. Proxy mode
	// connects through a single address, so it works across Kubernetes clusters where node
	// publish addresses are not routable (sniff mode would dial pod IPs).
	PutRemoteCluster(ctx context.Context, alias, proxyAddress string) error
	// DeleteRemoteCluster removes a remote cluster registration.
	DeleteRemoteCluster(ctx context.Context, alias string) error
	// RemoteInfo returns the connection state of every registered remote cluster. Connections
	// are opened lazily, so Connected may be false until the remote is first used.
	RemoteInfo(ctx context.Context) (map[string]RemoteClusterInfo, error)

	// PutAutoFollowPattern creates or replaces an auto-follow pattern. Auto-follow only follows
	// leader indices created after the pattern exists; existing indices need Follow.
	PutAutoFollowPattern(ctx context.Context, name string, p AutoFollowPattern) error
	// GetAutoFollowPattern returns the pattern, or found=false when it does not exist.
	GetAutoFollowPattern(ctx context.Context, name string) (p *AutoFollowPattern, active bool, found bool, err error)
	PauseAutoFollowPattern(ctx context.Context, name string) error
	ResumeAutoFollowPattern(ctx context.Context, name string) error
	// DeleteAutoFollowPattern deletes a pattern; deleting a missing pattern is not an error.
	DeleteAutoFollowPattern(ctx context.Context, name string) error

	// Follow creates the follower index `index` replicating `leaderIndex` from `remote`.
	Follow(ctx context.Context, index, remote, leaderIndex string) error
	PauseFollow(ctx context.Context, index string) error
	ResumeFollow(ctx context.Context, index string) error
	// Unfollow converts a paused, closed follower into a regular index. It is irreversible.
	//
	// When the leader cluster is unreachable Elasticsearch still converts the index but answers
	// HTTP 500 with failed_to_remove_retention_leases, because it could not remove the follower's
	// retention lease on the leader. That case is reported as LeaseRemovalFailed with a nil
	// error; callers should still confirm the conversion through FollowInfo.
	Unfollow(ctx context.Context, index string) (UnfollowResult, error)
	// FollowInfo returns the follower indices of this cluster with their status.
	FollowInfo(ctx context.Context) ([]FollowerInfo, error)
	// FollowStats returns per-shard follow statistics for every follower index.
	FollowStats(ctx context.Context) ([]FollowerIndexStats, error)
	// PrimaryGlobalCheckpoints returns, for each index, the global checkpoint of every primary
	// shard on this cluster. Once a leader's writes are blocked these are the exact targets a
	// follower must reach before a zero-loss switchover.
	PrimaryGlobalCheckpoints(ctx context.Context, indices []string) (map[string]map[int]int64, error)

	// ListUserIndices lists the open and closed non-hidden indices, with whether each is a CCR
	// follower and whether it carries a write block.
	ListUserIndices(ctx context.Context) ([]IndexInfo, error)
	CloseIndices(ctx context.Context, indices []string) error
	OpenIndices(ctx context.Context, indices []string) error
	// SetWriteBlock sets (on=true) or removes (on=false) index.blocks.write on the indices.
	SetWriteBlock(ctx context.Context, indices []string, on bool) error
	// PutPersistentClusterSetting sets a persistent cluster setting; a nil value resets it.
	PutPersistentClusterSetting(ctx context.Context, key string, value any) error
	// GetPersistentClusterSetting returns a persistent cluster setting as a string, or "" if unset.
	GetPersistentClusterSetting(ctx context.Context, key string) (string, error)
}

// RemoteClusterInfo is one entry of GET /_remote/info.
type RemoteClusterInfo struct {
	Connected                bool   `json:"connected"`
	Mode                     string `json:"mode"`
	ProxyAddress             string `json:"proxy_address,omitempty"`
	NumProxySocketsConnected int    `json:"num_proxy_sockets_connected,omitempty"`
}

// AutoFollowPattern is the body of PUT /_ccr/auto_follow/<name>.
type AutoFollowPattern struct {
	RemoteCluster                string         `json:"remote_cluster"`
	LeaderIndexPatterns          []string       `json:"leader_index_patterns"`
	LeaderIndexExclusionPatterns []string       `json:"leader_index_exclusion_patterns,omitempty"`
	FollowIndexPattern           string         `json:"follow_index_pattern,omitempty"`
	Settings                     map[string]any `json:"settings,omitempty"`
}

// UnfollowResult reports how an unfollow completed.
type UnfollowResult struct {
	// LeaseRemovalFailed is true when the index was unfollowed but its retention lease on the
	// (unreachable) leader could not be removed. The lease expires on its own after
	// index.soft_deletes.retention_lease.period (12h by default).
	LeaseRemovalFailed bool
	// Message carries the leader-side error for LeaseRemovalFailed.
	Message string
}

// FollowerInfo is one entry of GET /_all/_ccr/info.
type FollowerInfo struct {
	FollowerIndex string `json:"follower_index"`
	RemoteCluster string `json:"remote_cluster"`
	LeaderIndex   string `json:"leader_index"`
	// Status is "active" or "paused".
	Status string `json:"status"`
}

// FollowerIndexStats is the follow statistics of one follower index.
type FollowerIndexStats struct {
	Index  string               `json:"index"`
	Shards []FollowerShardStats `json:"shards"`
}

// FollowerShardStats is the follow statistics of one follower shard.
type FollowerShardStats struct {
	RemoteCluster string `json:"remote_cluster"`
	LeaderIndex   string `json:"leader_index"`
	ShardID       int    `json:"shard_id"`
	// LeaderGlobalCheckpoint is the leader checkpoint as last known to the follower task. It
	// stops advancing when the leader becomes unreachable, so a zero gap is only evidence
	// together with a recent TimeSinceLastReadMillis and no ReadExceptions.
	LeaderGlobalCheckpoint   int64 `json:"leader_global_checkpoint"`
	FollowerGlobalCheckpoint int64 `json:"follower_global_checkpoint"`
	TimeSinceLastReadMillis  int64 `json:"time_since_last_read_millis"`
	ReadExceptions           []any `json:"read_exceptions"`
	FatalException           any   `json:"fatal_exception,omitempty"`
}

// Lag is the shard's operation gap as known to the follower.
func (s FollowerShardStats) Lag() int64 {
	if d := s.LeaderGlobalCheckpoint - s.FollowerGlobalCheckpoint; d > 0 {
		return d
	}
	return 0
}

// IndexInfo describes one index of ListUserIndices.
type IndexInfo struct {
	Name         string
	UUID         string
	Follower     bool
	WriteBlocked bool
	Closed       bool
}

// APIError is a non-2xx Elasticsearch response.
type APIError struct {
	StatusCode int
	Type       string
	Reason     string
	Body       string
}

func (e *APIError) Error() string {
	if e.Type != "" || e.Reason != "" {
		return fmt.Sprintf("elasticsearch returned %d: %s: %s", e.StatusCode, e.Type, e.Reason)
	}
	return fmt.Sprintf("elasticsearch returned %d: %s", e.StatusCode, truncate(e.Body, 512))
}

// IsNotFound reports whether err is an Elasticsearch 404.
func IsNotFound(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.StatusCode == http.StatusNotFound
}

// Performer is the transport the CCR client sends requests through. *esv7.Client,
// *esv8.Client and *esv9.Client implement it.
type Performer interface {
	Perform(*http.Request) (*http.Response, error)
}

// NewCCRClient returns a CCRClient over a go-elasticsearch transport.
func NewCCRClient(t Performer) CCRClient {
	return &ccrClient{t: t}
}

type ccrClient struct {
	t Performer
}

var _ CCRClient = &ccrClient{}

func (c *ccrClient) do(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(buf)
	}
	u := &url.URL{Path: path}
	if len(query) > 0 {
		u.RawQuery = query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := c.t.Perform(req)
	if err != nil {
		return err
	}
	defer res.Body.Close() // nolint:errcheck
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return newAPIError(res.StatusCode, raw)
	}
	if out != nil && len(raw) > 0 {
		return json.Unmarshal(raw, out)
	}
	return nil
}

func newAPIError(status int, raw []byte) *APIError {
	e := &APIError{StatusCode: status, Body: string(raw)}
	var parsed struct {
		Error struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"error"`
	}
	if json.Unmarshal(raw, &parsed) == nil {
		e.Type, e.Reason = parsed.Error.Type, parsed.Error.Reason
	}
	return e
}

func indexPath(index, suffix string) string {
	return "/" + url.PathEscape(index) + suffix
}

func joinIndices(indices []string) string {
	esc := make([]string, len(indices))
	for i, ix := range indices {
		esc[i] = url.PathEscape(ix)
	}
	return strings.Join(esc, ",")
}

func (c *ccrClient) ClusterUUID(ctx context.Context) (string, error) {
	var out struct {
		ClusterUUID string `json:"cluster_uuid"`
	}
	if err := c.do(ctx, http.MethodGet, "/", nil, nil, &out); err != nil {
		return "", err
	}
	return out.ClusterUUID, nil
}

func (c *ccrClient) PutRemoteCluster(ctx context.Context, alias, proxyAddress string) error {
	body := map[string]any{"persistent": map[string]any{
		"cluster.remote." + alias + ".mode":          "proxy",
		"cluster.remote." + alias + ".proxy_address": proxyAddress,
	}}
	return c.do(ctx, http.MethodPut, "/_cluster/settings", nil, body, nil)
}

func (c *ccrClient) DeleteRemoteCluster(ctx context.Context, alias string) error {
	body := map[string]any{"persistent": map[string]any{
		"cluster.remote." + alias + ".mode":          nil,
		"cluster.remote." + alias + ".proxy_address": nil,
		"cluster.remote." + alias + ".seeds":         nil,
	}}
	return c.do(ctx, http.MethodPut, "/_cluster/settings", nil, body, nil)
}

func (c *ccrClient) RemoteInfo(ctx context.Context) (map[string]RemoteClusterInfo, error) {
	out := map[string]RemoteClusterInfo{}
	if err := c.do(ctx, http.MethodGet, "/_remote/info", nil, nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *ccrClient) PutAutoFollowPattern(ctx context.Context, name string, p AutoFollowPattern) error {
	return c.do(ctx, http.MethodPut, "/_ccr/auto_follow/"+url.PathEscape(name), nil, p, nil)
}

func (c *ccrClient) GetAutoFollowPattern(ctx context.Context, name string) (*AutoFollowPattern, bool, bool, error) {
	var out struct {
		Patterns []struct {
			Name    string `json:"name"`
			Pattern struct {
				AutoFollowPattern
				Active bool `json:"active"`
			} `json:"pattern"`
		} `json:"patterns"`
	}
	err := c.do(ctx, http.MethodGet, "/_ccr/auto_follow/"+url.PathEscape(name), nil, nil, &out)
	if IsNotFound(err) {
		return nil, false, false, nil
	}
	if err != nil {
		return nil, false, false, err
	}
	for _, p := range out.Patterns {
		if p.Name == name {
			afp := p.Pattern.AutoFollowPattern
			return &afp, p.Pattern.Active, true, nil
		}
	}
	return nil, false, false, nil
}

func (c *ccrClient) PauseAutoFollowPattern(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodPost, "/_ccr/auto_follow/"+url.PathEscape(name)+"/pause", nil, nil, nil)
}

func (c *ccrClient) ResumeAutoFollowPattern(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodPost, "/_ccr/auto_follow/"+url.PathEscape(name)+"/resume", nil, nil, nil)
}

func (c *ccrClient) DeleteAutoFollowPattern(ctx context.Context, name string) error {
	err := c.do(ctx, http.MethodDelete, "/_ccr/auto_follow/"+url.PathEscape(name), nil, nil, nil)
	if IsNotFound(err) {
		return nil
	}
	return err
}

func (c *ccrClient) Follow(ctx context.Context, index, remote, leaderIndex string) error {
	q := url.Values{"wait_for_active_shards": {"1"}}
	body := map[string]any{"remote_cluster": remote, "leader_index": leaderIndex}
	return c.do(ctx, http.MethodPut, indexPath(index, "/_ccr/follow"), q, body, nil)
}

func (c *ccrClient) PauseFollow(ctx context.Context, index string) error {
	return c.do(ctx, http.MethodPost, indexPath(index, "/_ccr/pause_follow"), nil, nil, nil)
}

func (c *ccrClient) ResumeFollow(ctx context.Context, index string) error {
	return c.do(ctx, http.MethodPost, indexPath(index, "/_ccr/resume_follow"), nil, map[string]any{}, nil)
}

func (c *ccrClient) Unfollow(ctx context.Context, index string) (UnfollowResult, error) {
	err := c.do(ctx, http.MethodPost, indexPath(index, "/_ccr/unfollow"), nil, nil, nil)
	if err == nil {
		return UnfollowResult{}, nil
	}
	var ae *APIError
	if errors.As(err, &ae) && ae.StatusCode >= 500 && strings.Contains(ae.Body, "failed_to_remove_retention_leases") {
		return UnfollowResult{LeaseRemovalFailed: true, Message: ae.Reason}, nil
	}
	return UnfollowResult{}, err
}

func (c *ccrClient) FollowInfo(ctx context.Context) ([]FollowerInfo, error) {
	var out struct {
		FollowerIndices []FollowerInfo `json:"follower_indices"`
	}
	if err := c.do(ctx, http.MethodGet, "/_all/_ccr/info", nil, nil, &out); err != nil {
		return nil, err
	}
	return out.FollowerIndices, nil
}

func (c *ccrClient) FollowStats(ctx context.Context) ([]FollowerIndexStats, error) {
	var out struct {
		FollowStats struct {
			Indices []FollowerIndexStats `json:"indices"`
		} `json:"follow_stats"`
	}
	if err := c.do(ctx, http.MethodGet, "/_ccr/stats", nil, nil, &out); err != nil {
		return nil, err
	}
	return out.FollowStats.Indices, nil
}

func (c *ccrClient) PrimaryGlobalCheckpoints(ctx context.Context, indices []string) (map[string]map[int]int64, error) {
	res := map[string]map[int]int64{}
	if len(indices) == 0 {
		return res, nil
	}
	q := url.Values{
		"level":       {"shards"},
		"filter_path": {"indices.*.shards.*.routing.primary,indices.*.shards.*.seq_no.global_checkpoint"},
	}
	var out struct {
		Indices map[string]struct {
			Shards map[string][]struct {
				Routing struct {
					Primary bool `json:"primary"`
				} `json:"routing"`
				SeqNo struct {
					GlobalCheckpoint int64 `json:"global_checkpoint"`
				} `json:"seq_no"`
			} `json:"shards"`
		} `json:"indices"`
	}
	if err := c.do(ctx, http.MethodGet, "/"+joinIndices(indices)+"/_stats", q, nil, &out); err != nil {
		return nil, err
	}
	for name, ix := range out.Indices {
		res[name] = map[int]int64{}
		for sid, copies := range ix.Shards {
			var n int
			if _, err := fmt.Sscanf(sid, "%d", &n); err != nil {
				continue
			}
			for _, cp := range copies {
				if cp.Routing.Primary {
					res[name][n] = cp.SeqNo.GlobalCheckpoint
				}
			}
		}
	}
	return res, nil
}

func (c *ccrClient) ListUserIndices(ctx context.Context) ([]IndexInfo, error) {
	// Settings of every non-hidden index, open or closed. Dot-prefixed system indices are
	// hidden on supported versions; they are filtered out again below to be safe.
	q := url.Values{
		"flat_settings":      {"true"},
		"expand_wildcards":   {"open,closed"},
		"ignore_unavailable": {"true"},
		"filter_path":        {"*.settings.index.uuid,*.settings.index.xpack.ccr.following_index,*.settings.index.blocks.write,*.settings.index.verified_before_close"},
	}
	out := map[string]struct {
		Settings map[string]string `json:"settings"`
	}{}
	if err := c.do(ctx, http.MethodGet, "/*/_settings", q, nil, &out); err != nil {
		return nil, err
	}
	closed := map[string]bool{}
	var cat []struct {
		Index  string `json:"index"`
		Status string `json:"status"`
	}
	cq := url.Values{"format": {"json"}, "h": {"index,status"}, "expand_wildcards": {"open,closed"}}
	if err := c.do(ctx, http.MethodGet, "/_cat/indices", cq, nil, &cat); err != nil {
		return nil, err
	}
	for _, ci := range cat {
		closed[ci.Index] = ci.Status == "close"
	}
	res := make([]IndexInfo, 0, len(out))
	for name, v := range out {
		if strings.HasPrefix(name, ".") {
			continue
		}
		res = append(res, IndexInfo{
			Name:         name,
			UUID:         v.Settings["index.uuid"],
			Follower:     v.Settings["index.xpack.ccr.following_index"] == "true",
			WriteBlocked: v.Settings["index.blocks.write"] == "true",
			Closed:       closed[name],
		})
	}
	sort.Slice(res, func(i, j int) bool { return res[i].Name < res[j].Name })
	return res, nil
}

func (c *ccrClient) CloseIndices(ctx context.Context, indices []string) error {
	if len(indices) == 0 {
		return nil
	}
	return c.do(ctx, http.MethodPost, "/"+joinIndices(indices)+"/_close", url.Values{"expand_wildcards": {"open,closed"}}, nil, nil)
}

func (c *ccrClient) OpenIndices(ctx context.Context, indices []string) error {
	if len(indices) == 0 {
		return nil
	}
	return c.do(ctx, http.MethodPost, "/"+joinIndices(indices)+"/_open", url.Values{"wait_for_active_shards": {"1"}}, nil, nil)
}

func (c *ccrClient) SetWriteBlock(ctx context.Context, indices []string, on bool) error {
	if len(indices) == 0 {
		return nil
	}
	var v any
	if on {
		v = true
	}
	return c.do(ctx, http.MethodPut, "/"+joinIndices(indices)+"/_settings", nil, map[string]any{"index.blocks.write": v}, nil)
}

func (c *ccrClient) PutPersistentClusterSetting(ctx context.Context, key string, value any) error {
	return c.do(ctx, http.MethodPut, "/_cluster/settings", nil, map[string]any{"persistent": map[string]any{key: value}}, nil)
}

func (c *ccrClient) GetPersistentClusterSetting(ctx context.Context, key string) (string, error) {
	var out struct {
		Persistent map[string]any `json:"persistent"`
	}
	if err := c.do(ctx, http.MethodGet, "/_cluster/settings", url.Values{"flat_settings": {"true"}}, nil, &out); err != nil {
		return "", err
	}
	if v, ok := out.Persistent[key]; ok && v != nil {
		return fmt.Sprint(v), nil
	}
	return "", nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
