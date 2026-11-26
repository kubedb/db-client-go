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

package rabbitmq

// This file holds the DC-DR management contract both the hub orchestrator and the rmq-coordinator
// fence use: a thin management API client over the global parameters the hub writes and the fence
// reads, plus the permit and fence-state documents stored in those parameters. It lives here beside
// the other RabbitMQ clients so the coordinator depends only on db-client-go.

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	rmqhttp "github.com/michaelklishin/rabbit-hole/v3"
)

const (
	// PermitParameter is the global parameter the hub writes on the active data center. It mirrors
	// apimachinery's RabbitMQDCDRPermitParameter (a fixed wire value, inlined so this client does
	// not pin a specific apimachinery version).
	PermitParameter = "kubedb-dcdr-permit"
	// FenceStateParameter holds the saved vhost permissions of a fenced data center. It mirrors
	// apimachinery's RabbitMQDCDRPermissionsParameter.
	FenceStateParameter = "kubedb-dcdr-permissions"
)

// ManagementDefaultTimeout bounds one management API request. A data center that black-holes
// traffic must fail a call quickly, or it holds the whole orchestrator pass.
const ManagementDefaultTimeout = 15 * time.Second

// ManagementClient is a management API client of one RabbitMQ cluster, used by the DC-DR hub
// orchestrator and the spoke fence agent for the global parameters that carry the permit and the
// saved permissions.
type ManagementClient struct {
	*rmqhttp.Client
}

// NewManagementClient returns a client for endpoint (http://host:15672) with the given credentials.
func NewManagementClient(endpoint, username, password string, timeout time.Duration) (*ManagementClient, error) {
	c, err := rmqhttp.NewClient(endpoint, username, password)
	if err != nil {
		return nil, err
	}
	if timeout <= 0 {
		timeout = ManagementDefaultTimeout
	}
	c.SetTimeout(timeout)
	return &ManagementClient{Client: c}, nil
}

// IsNotFound reports whether err is a management API 404.
func IsNotFound(err error) bool {
	var rme rmqhttp.ErrorResponse
	if errors.As(err, &rme) {
		return rme.StatusCode == http.StatusNotFound
	}
	var prme *rmqhttp.ErrorResponse
	if errors.As(err, &prme) {
		return prme.StatusCode == http.StatusNotFound
	}
	return false
}

// GetParameter decodes the global parameter name into out. It reports false when the parameter
// does not exist.
func (c *ManagementClient) GetParameter(name string, out any) (bool, error) {
	p, err := c.GetGlobalParameter(name)
	if err != nil {
		if IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	raw, err := json.Marshal(p.Value)
	if err != nil {
		return false, err
	}
	return true, json.Unmarshal(raw, out)
}

// PutParameter stores value as the global parameter name.
func (c *ManagementClient) PutParameter(name string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var v map[string]any
	if err := json.Unmarshal(raw, &v); err != nil {
		return err
	}
	resp, err := c.PutGlobalParameter(name, v)
	return closeBody(resp, err)
}

// DeleteParameter deletes the global parameter name; a missing parameter is not an error.
func (c *ManagementClient) DeleteParameter(name string) error {
	resp, err := c.DeleteGlobalParameter(name)
	if IsNotFound(err) {
		return nil
	}
	return closeBody(resp, err)
}

// closeBody closes a response body the rabbit-hole call handed back.
func closeBody(resp *http.Response, err error) error {
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	return err
}

// Done turns a (response, error) pair of a rabbit-hole mutation into its error, closing the body.
func Done(resp *http.Response, err error) error { return closeBody(resp, err) }

// Permit is written by the hub as a global parameter of the active data center's cluster once that
// data center may take client traffic. The spoke fence agent keeps the application users revoked
// unless the marker freshly names the data center AND this permit names it for the same primary
// Lease. The hub deletes the permit from every non-active data center it can reach.
type Permit struct {
	DC               string    `json:"dc"`
	LeaseName        string    `json:"leaseName"`
	LeaseUID         string    `json:"leaseUID,omitempty"`
	LeaseTransitions int32     `json:"leaseTransitions"`
	IssuedAt         time.Time `json:"issuedAt"`
}

// FenceState is the fence agent's own record: the vhost permissions it removed (and the hub's copy
// of the active data center's permissions on a standby), so opening restores exactly them.
type FenceState struct {
	Fenced      bool             `json:"fenced"`
	Reason      string           `json:"reason,omitempty"`
	Permissions []PermissionRule `json:"permissions,omitempty"`
	UpdatedAt   time.Time        `json:"updatedAt"`
}

// PermissionRule is one user's permissions in one vhost.
type PermissionRule struct {
	User      string `json:"user"`
	Vhost     string `json:"vhost"`
	Configure string `json:"configure"`
	Write     string `json:"write"`
	Read      string `json:"read"`
}

// Key identifies the rule's user and vhost.
func (p PermissionRule) Key() string { return p.Vhost + "\x00" + p.User }
