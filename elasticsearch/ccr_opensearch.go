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
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

// osFollowerSetting is the index setting the OpenSearch replication plugin puts on a follower:
// "<leader alias>:<leader index>".
const osFollowerSetting = "index.plugins.replication.follower.leader_index"

// osSystemIndexPrefixes are OpenSearch plugin indices that are neither dot-prefixed nor hidden
// (security audit log, query insights). They are local to each cluster and never replicated.
var osSystemIndexPrefixes = []string{"security-auditlog-", "top_queries-"}

// ErrAutoFollowUnsupported is returned when the backend cannot express an auto-follow pattern.
// OpenSearch auto-follow rules take one include pattern and no exclusions, so a pattern that
// could match an excluded index (such as "*") is refused; callers then follow new indices
// explicitly instead.
var ErrAutoFollowUnsupported = errors.New("auto-follow pattern cannot be expressed by this backend")

// OpenSearchReplicationOptions configures the OpenSearch replication plugin client.
type OpenSearchReplicationOptions struct {
	// LeaderRole and FollowerRole are sent as use_roles when replication starts. Both are
	// required when the security plugin is enabled and must be omitted when it is not.
	LeaderRole   string
	FollowerRole string
}

// NewOpenSearchReplicationClient returns a CCRClient that replicates through the OpenSearch
// cross-cluster-replication plugin (_plugins/_replication). Remote cluster, checkpoint, index
// and settings calls are the same REST as Elasticsearch and are shared.
func NewOpenSearchReplicationClient(t Performer, o OpenSearchReplicationOptions) CCRClient {
	base := newElasticCCR(t)
	base.isFollower = func(settings map[string]string) bool { return settings[osFollowerSetting] != "" }
	base.isSystem = isOpenSearchSystemIndex
	return &osReplicationClient{ccrClient: base, opts: o}
}

type osReplicationClient struct {
	*ccrClient
	opts OpenSearchReplicationOptions
}

var _ CCRClient = &osReplicationClient{}

func isOpenSearchSystemIndex(name string) bool {
	for _, p := range osSystemIndexPrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

func (c *osReplicationClient) useRoles() map[string]string {
	if c.opts.LeaderRole == "" && c.opts.FollowerRole == "" {
		return nil
	}
	return map[string]string{"leader_cluster_role": c.opts.LeaderRole, "follower_cluster_role": c.opts.FollowerRole}
}

func replicationPath(index, action string) string {
	return "/_plugins/_replication/" + url.PathEscape(index) + "/" + action
}

func (c *osReplicationClient) Follow(ctx context.Context, index, remote, leaderIndex string) error {
	body := map[string]any{"leader_alias": remote, "leader_index": leaderIndex}
	if r := c.useRoles(); r != nil {
		body["use_roles"] = r
	}
	return c.do(ctx, http.MethodPut, replicationPath(index, "_start"), nil, body, nil)
}

func (c *osReplicationClient) PauseFollow(ctx context.Context, index string) error {
	return c.do(ctx, http.MethodPost, replicationPath(index, "_pause"), nil, map[string]any{}, nil)
}

func (c *osReplicationClient) ResumeFollow(ctx context.Context, index string) error {
	return c.do(ctx, http.MethodPost, replicationPath(index, "_resume"), nil, map[string]any{}, nil)
}

// Unfollow stops replication, which turns the follower into a regular index in place.
func (c *osReplicationClient) Unfollow(ctx context.Context, index string) (UnfollowResult, error) {
	err := c.do(ctx, http.MethodPost, replicationPath(index, "_stop"), nil, map[string]any{}, nil)
	if err != nil && !replicationNotInProgress(err) {
		return UnfollowResult{}, err
	}
	return UnfollowResult{}, nil
}

// Promote stops replication of every follower in parallel. Unlike Elasticsearch no pause,
// close or reopen is needed. Against a leader that drops packets _stop waits on the remote
// connection, so callers detach an unreachable leader's remote cluster first.
func (c *osReplicationClient) Promote(ctx context.Context, indices []string) ([]string, error) {
	_, err := forEachIndex(ctx, indices, func(ctx context.Context, ix string) (bool, error) {
		_, err := c.Unfollow(ctx, ix)
		return false, err
	})
	if err != nil {
		return nil, fmt.Errorf("stop replication: %w", err)
	}
	return nil, nil
}

func replicationNotInProgress(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.StatusCode < 500 && strings.Contains(strings.ToLower(ae.Body), "no replication in progress")
}

// followerSettings returns, for every follower index, its leader alias and leader index.
func (c *osReplicationClient) followerSettings(ctx context.Context) (map[string][2]string, error) {
	q := url.Values{"flat_settings": {"true"}, "expand_wildcards": {"open,closed"}, "ignore_unavailable": {"true"}, "filter_path": {"*.settings"}}
	out := map[string]struct {
		Settings map[string]string `json:"settings"`
	}{}
	if err := c.do(ctx, http.MethodGet, "/*/_settings", q, nil, &out); err != nil {
		return nil, err
	}
	res := map[string][2]string{}
	for name, v := range out {
		if alias, leader, ok := strings.Cut(v.Settings[osFollowerSetting], ":"); ok {
			res[name] = [2]string{alias, leader}
		}
	}
	return res, nil
}

type osIndexStats struct {
	LeaderCheckpoint     int64 `json:"leader_checkpoint"`
	FollowerCheckpoint   int64 `json:"follower_checkpoint"`
	FailedReadRequests   int64 `json:"failed_read_requests"`
	OperationsRead       int64 `json:"operations_read"`
	OperationsWritten    int64 `json:"operations_written"`
	TotalWriteTimeMillis int64 `json:"total_write_time_millis"`
}

// followerStats returns the plugin's per-index statistics of the syncing followers. It is
// answered locally, so it stays responsive when the leader is unreachable (unlike _status).
func (c *osReplicationClient) followerStats(ctx context.Context) (map[string]osIndexStats, error) {
	var out struct {
		IndexStats map[string]osIndexStats `json:"index_stats"`
	}
	if err := c.do(ctx, http.MethodGet, "/_plugins/_replication/follower_stats", nil, nil, &out); err != nil {
		return nil, err
	}
	return out.IndexStats, nil
}

// FollowInfo lists the followers from their index settings. A follower the plugin reports
// as syncing is "active"; any other (paused, bootstrapping, failed) is "paused".
func (c *osReplicationClient) FollowInfo(ctx context.Context) ([]FollowerInfo, error) {
	fs, err := c.followerSettings(ctx)
	if err != nil {
		return nil, err
	}
	stats, err := c.followerStats(ctx)
	if err != nil {
		return nil, err
	}
	res := make([]FollowerInfo, 0, len(fs))
	for name, l := range fs {
		status := "paused"
		if _, ok := stats[name]; ok {
			status = "active"
		}
		res = append(res, FollowerInfo{FollowerIndex: name, RemoteCluster: l[0], LeaderIndex: l[1], Status: status})
	}
	sort.Slice(res, func(i, j int) bool { return res[i].FollowerIndex < res[j].FollowerIndex })
	return res, nil
}

// FollowStats builds per-shard statistics from the followers' own primary global checkpoints
// (exact, per shard) and the plugin's index-level lag. The plugin reports no per-shard read
// time or read errors, so TimeSinceLastReadMillis is 0 and ReadExceptions empty; compare
// against the leader's PrimaryGlobalCheckpoints for a lag that does not depend on the
// follower having heard from the leader.
func (c *osReplicationClient) FollowStats(ctx context.Context) ([]FollowerIndexStats, error) {
	fs, err := c.followerSettings(ctx)
	if err != nil || len(fs) == 0 {
		return nil, err
	}
	names := make([]string, 0, len(fs))
	for n := range fs {
		names = append(names, n)
	}
	sort.Strings(names)
	cps, err := c.PrimaryGlobalCheckpoints(ctx, names)
	if err != nil {
		return nil, err
	}
	stats, err := c.followerStats(ctx)
	if err != nil {
		return nil, err
	}
	res := make([]FollowerIndexStats, 0, len(names))
	for _, n := range names {
		shardIDs := make([]int, 0, len(cps[n]))
		for id := range cps[n] {
			shardIDs = append(shardIDs, id)
		}
		sort.Ints(shardIDs)
		var indexLag int64
		if st, ok := stats[n]; ok && st.LeaderCheckpoint > st.FollowerCheckpoint {
			indexLag = st.LeaderCheckpoint - st.FollowerCheckpoint
		}
		out := FollowerIndexStats{Index: n}
		for i, id := range shardIDs {
			sh := FollowerShardStats{
				RemoteCluster:            fs[n][0],
				LeaderIndex:              fs[n][1],
				ShardID:                  id,
				FollowerGlobalCheckpoint: cps[n][id],
				LeaderGlobalCheckpoint:   cps[n][id],
			}
			if i == 0 {
				sh.LeaderGlobalCheckpoint += indexLag
			}
			out.Shards = append(out.Shards, sh)
		}
		res = append(res, out)
	}
	return res, nil
}

type osAutoFollowRule struct {
	Name    string `json:"name"`
	Pattern string `json:"pattern"`
}

func (c *osReplicationClient) autoFollowRules(ctx context.Context) ([]osAutoFollowRule, error) {
	var out struct {
		Rules []osAutoFollowRule `json:"autofollow_stats"`
	}
	if err := c.do(ctx, http.MethodGet, "/_plugins/_replication/autofollow_stats", nil, nil, &out); err != nil {
		return nil, err
	}
	return out.Rules, nil
}

// PutAutoFollowPattern creates the rule. OpenSearch rules have one include pattern and no
// exclusions and always keep the leader index name, so a pattern that could match an excluded
// (or OpenSearch system) index returns ErrAutoFollowUnsupported. An existing rule of the same
// name is replaced.
func (c *osReplicationClient) PutAutoFollowPattern(ctx context.Context, name string, p AutoFollowPattern) error {
	if len(p.LeaderIndexPatterns) != 1 {
		return fmt.Errorf("%w: OpenSearch takes exactly one include pattern, got %d", ErrAutoFollowUnsupported, len(p.LeaderIndexPatterns))
	}
	pattern := p.LeaderIndexPatterns[0]
	excl := append(append([]string(nil), p.LeaderIndexExclusionPatterns...), ".*")
	for _, s := range osSystemIndexPrefixes {
		excl = append(excl, s+"*")
	}
	for _, e := range excl {
		if patternsOverlap(pattern, e) {
			return fmt.Errorf("%w: %q could match excluded indices %q", ErrAutoFollowUnsupported, pattern, e)
		}
	}
	if err := c.DeleteAutoFollowPattern(ctx, name); err != nil {
		return err
	}
	body := map[string]any{"leader_alias": p.RemoteCluster, "name": name, "pattern": pattern}
	if r := c.useRoles(); r != nil {
		body["use_roles"] = r
	}
	return c.do(ctx, http.MethodPost, "/_plugins/_replication/_autofollow", nil, body, nil)
}

// patternsOverlap reports whether two index patterns could match a common name, judged by
// their literal prefixes before the first wildcard.
func patternsOverlap(a, b string) bool {
	pa, _, _ := strings.Cut(a, "*")
	pb, _, _ := strings.Cut(b, "*")
	return strings.HasPrefix(pa, pb) || strings.HasPrefix(pb, pa)
}

// GetAutoFollowPattern reports a rule by name. The plugin does not return a rule's leader
// alias, so RemoteCluster is empty; the rule is always active.
func (c *osReplicationClient) GetAutoFollowPattern(ctx context.Context, name string) (*AutoFollowPattern, bool, bool, error) {
	rules, err := c.autoFollowRules(ctx)
	if err != nil {
		return nil, false, false, err
	}
	for _, r := range rules {
		if r.Name == name {
			return &AutoFollowPattern{LeaderIndexPatterns: []string{r.Pattern}, FollowIndexPattern: "{{leader_index}}"}, true, true, nil
		}
	}
	return nil, false, false, nil
}

// PauseAutoFollowPattern and ResumeAutoFollowPattern have no OpenSearch equivalent; rules are
// always active and are deleted instead.
func (c *osReplicationClient) PauseAutoFollowPattern(context.Context, string) error {
	return fmt.Errorf("%w: OpenSearch rules cannot be paused", ErrAutoFollowUnsupported)
}

func (c *osReplicationClient) ResumeAutoFollowPattern(context.Context, string) error { return nil }

// DeleteAutoFollowPattern deletes a rule by name. The plugin needs the rule's leader alias,
// which it does not report, so every registered remote cluster is tried. A missing rule is
// not an error.
func (c *osReplicationClient) DeleteAutoFollowPattern(ctx context.Context, name string) error {
	_, _, found, err := c.GetAutoFollowPattern(ctx, name)
	if err != nil || !found {
		return err
	}
	remotes, err := c.RemoteInfo(ctx)
	if err != nil {
		return err
	}
	lastErr := fmt.Errorf("auto-follow rule %s: no registered remote cluster owns it", name)
	for alias := range remotes {
		err := c.do(ctx, http.MethodDelete, "/_plugins/_replication/_autofollow", nil, map[string]any{"leader_alias": alias, "name": name}, nil)
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return lastErr
}
