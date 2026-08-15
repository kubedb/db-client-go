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

package etcd

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

const (
	// DefaultDialTimeout is the connect deadline used for every etcd dial.
	DefaultDialTimeout = 5 * time.Second
	// HealthCheckTimeout bounds a single per-member health probe so that one
	// unreachable member can not stall the whole quorum check.
	HealthCheckTimeout = 2 * time.Second
)

// Client is a thin wrapper around clientv3.Client. It keeps the config it was
// dialed with, so that per-member (single endpoint) clients can be derived
// without re-reading the TLS/auth secrets.
type Client struct {
	*clientv3.Client

	cfg clientv3.Config
}

// Config returns a copy of the config this client was dialed with.
func (c *Client) Config() clientv3.Config {
	return c.cfg
}

// Close releases the underlying etcd connections. It is safe to call on a
// zero valued or partially constructed Client.
func (c *Client) Close() error {
	if c == nil || c.Client == nil {
		return nil
	}
	return c.Client.Close()
}

// ---------------------------------------------------------------------------
// Maintenance
// ---------------------------------------------------------------------------

// Status fans out the Status RPC over every configured endpoint and returns the
// responses keyed by endpoint. A failure against a single member is reported
// through the error; the successfully collected statuses are still returned so
// that callers can reason about a partially healthy cluster.
func (c *Client) Status(ctx context.Context) (map[string]*clientv3.StatusResponse, error) {
	endpoints := c.Endpoints()
	if len(endpoints) == 0 {
		// Callers report the returned error verbatim; returning (empty, nil)
		// here would hand them a nil error together with no statuses at all.
		return nil, errors.New("the etcd client has no endpoints configured")
	}
	statuses := make(map[string]*clientv3.StatusResponse, len(endpoints))

	var errs []error
	for _, ep := range endpoints {
		resp, err := c.Client.Status(ctx, ep)
		if err != nil {
			errs = append(errs, errors.Wrapf(err, "failed to get status of etcd member %s", ep))
			continue
		}
		statuses[ep] = resp
	}
	if len(errs) > 0 {
		return statuses, utilerrors.NewAggregate(errs)
	}
	return statuses, nil
}

// EndpointStatus returns the Status of a single etcd member.
func (c *Client) EndpointStatus(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error) {
	resp, err := c.Client.Status(ctx, endpoint)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get status of etcd member %s", endpoint)
	}
	return resp, nil
}

// Defragment defragments the backend of a single member. Defragmentation is
// blocking and expensive, so it must be run against one member at a time.
func (c *Client) Defragment(ctx context.Context, endpoint string) (*clientv3.DefragmentResponse, error) {
	resp, err := c.Client.Defragment(ctx, endpoint)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to defragment etcd member %s", endpoint)
	}
	return resp, nil
}

// Compact discards all the history up to the given revision.
func (c *Client) Compact(ctx context.Context, revision int64, opts ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	resp, err := c.Client.Compact(ctx, revision, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to compact etcd upto revision %d", revision)
	}
	return resp, nil
}

// AlarmList returns the alarms (NOSPACE, CORRUPT) currently raised on any member.
func (c *Client) AlarmList(ctx context.Context) (*clientv3.AlarmResponse, error) {
	resp, err := c.Client.AlarmList(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list etcd alarms")
	}
	return resp, nil
}

// MoveLeader transfers the Raft leadership to the given member. It must be
// called against the current leader.
func (c *Client) MoveLeader(ctx context.Context, transfereeID uint64) (*clientv3.MoveLeaderResponse, error) {
	resp, err := c.Client.MoveLeader(ctx, transfereeID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to move etcd leadership to member %d", transfereeID)
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Membership
// ---------------------------------------------------------------------------

// MemberList lists the current members of the cluster.
//
// The RPC is linearizable by default, so it needs a quorum to answer. Pass
// clientv3.WithSerializable() to have the member that is dialed answer from its
// local store instead -- that is the only way to read the membership of a
// cluster that has already lost its quorum.
func (c *Client) MemberList(ctx context.Context, opts ...clientv3.OpOption) (*clientv3.MemberListResponse, error) {
	resp, err := c.Client.MemberList(ctx, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list etcd members")
	}
	return resp, nil
}

// MemberAddAsLearner adds a new member as a non voting learner. A learner does
// not count towards the quorum, so scaling up never risks the existing quorum.
func (c *Client) MemberAddAsLearner(ctx context.Context, peerURLs []string) (*clientv3.MemberAddResponse, error) {
	resp, err := c.Client.MemberAddAsLearner(ctx, peerURLs)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to add etcd learner with peer urls %v", peerURLs)
	}
	return resp, nil
}

// MemberPromote promotes a learner to a voting member. It fails until the
// learner has caught up with the leader.
func (c *Client) MemberPromote(ctx context.Context, memberID uint64) (*clientv3.MemberPromoteResponse, error) {
	resp, err := c.Client.MemberPromote(ctx, memberID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to promote etcd learner %d", memberID)
	}
	return resp, nil
}

// MemberRemove removes a member from the cluster.
func (c *Client) MemberRemove(ctx context.Context, memberID uint64) (*clientv3.MemberRemoveResponse, error) {
	resp, err := c.Client.MemberRemove(ctx, memberID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to remove etcd member %d", memberID)
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

// IsQuorumHealthy probes every endpoint concurrently, each through its own
// short lived single endpoint client, and reports whether a Raft quorum
// (voters/2 + 1, i.e. a strict majority of the voting members) is answering.
// The returned map holds the error of every member that failed the probe, keyed
// by endpoint.
func (c *Client) IsQuorumHealthy(ctx context.Context) (bool, map[string]error) {
	endpoints := c.Endpoints()
	if len(endpoints) == 0 {
		return false, nil
	}

	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		failed   = map[string]error{}
		learners int
	)
	for _, ep := range endpoints {
		wg.Add(1)
		go func(endpoint string) {
			defer wg.Done()
			isLearner, err := c.memberHealth(ctx, endpoint)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failed[endpoint] = err
				return
			}
			if isLearner {
				learners++
			}
		}(ep)
	}
	wg.Wait()

	// A learner is a non voting member: it neither raises the size of the
	// quorum nor helps to reach it, so it drops out of both sides of the
	// arithmetic. Counting it in would understate the quorum whenever the
	// number of voters is even -- 4 voters plus 1 learner would ask for 3 of 5
	// answers instead of 3 of 4, and two answering voters plus the learner
	// would pass. A member that did not answer can not be classified, so it
	// keeps counting as a voter, which is what the pre-learner behaviour was.
	voters := len(endpoints) - learners
	if len(failed) == 0 {
		failed = nil
	}
	if voters <= 0 {
		return false, failed
	}
	quorum := voters/2 + 1
	healthy := voters - len(failed)
	return healthy >= quorum, failed
}

// memberHealth dials a single member, asks for its status and reports whether
// that member is a learner.
//
// Answering the Status RPC is necessary but not sufficient: etcd serves it out
// of the member's local state, so a member that has lost contact with the rest
// of the cluster still answers, it just reports leader 0 (raft.None) and adds
// "etcdserver: no leader" to StatusResponse.Errors. Without the leader check a
// cluster whose pods are all up but which can no longer elect a leader (a
// partition between the members, say) would be reported as having a healthy
// quorum even though it can not serve a single write -- and the quorum loss
// recovery would refuse to run on exactly the cluster it exists for.
func (c *Client) memberHealth(ctx context.Context, endpoint string) (bool, error) {
	cfg := c.cfg
	cfg.Endpoints = []string{endpoint}
	cfg.DialTimeout = HealthCheckTimeout

	cl, err := clientv3.New(cfg)
	if err != nil {
		return false, errors.Wrapf(err, "failed to dial etcd member %s", endpoint)
	}
	defer func() {
		_ = cl.Close()
	}()

	callCtx, cancel := context.WithTimeout(ctx, HealthCheckTimeout)
	defer cancel()

	resp, err := cl.Status(callCtx, endpoint)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get status of etcd member %s", endpoint)
	}
	if resp.IsLearner {
		// A learner never votes, so its Raft state is irrelevant to the quorum;
		// it is only reported back so that it can be left out of the count.
		return true, nil
	}
	if resp.Leader == 0 {
		reason := "it does not know a raft leader"
		if len(resp.Errors) > 0 {
			reason = strings.Join(resp.Errors, "; ")
		}
		return false, errors.Errorf("etcd member %s is not part of a working quorum: %s", endpoint, reason)
	}
	return false, nil
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

// UserAdd creates a new etcd user.
func (c *Client) UserAdd(ctx context.Context, user, password string) error {
	if _, err := c.Client.UserAdd(ctx, user, password); err != nil {
		return errors.Wrapf(err, "failed to add etcd user %s", user)
	}
	return nil
}

// UserChangePassword updates the password of an existing etcd user.
func (c *Client) UserChangePassword(ctx context.Context, user, newPassword string) error {
	if _, err := c.Client.UserChangePassword(ctx, user, newPassword); err != nil {
		return errors.Wrapf(err, "failed to change password of etcd user %s", user)
	}
	return nil
}

// UserGrantRole grants a role to an etcd user.
func (c *Client) UserGrantRole(ctx context.Context, user, role string) error {
	if _, err := c.Client.UserGrantRole(ctx, user, role); err != nil {
		return errors.Wrapf(err, "failed to grant role %s to etcd user %s", role, user)
	}
	return nil
}

// AuthEnable turns on etcd RBAC. It requires an existing root user.
func (c *Client) AuthEnable(ctx context.Context) error {
	if _, err := c.Client.AuthEnable(ctx); err != nil {
		return errors.Wrap(err, "failed to enable etcd auth")
	}
	return nil
}

// AuthDisable turns off etcd RBAC.
func (c *Client) AuthDisable(ctx context.Context) error {
	if _, err := c.Client.AuthDisable(ctx); err != nil {
		return errors.Wrap(err, "failed to disable etcd auth")
	}
	return nil
}
