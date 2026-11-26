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

// RestClient is a minimal authenticated REST transport to one Elasticsearch or OpenSearch
// cluster, shared by the hub DC-DR orchestrator and the spoke fence agent. It deliberately
// avoids the version-specific go-elasticsearch clients: the CCR and index-admin APIs used here
// are the same on 7.x, 8.x and 9.x. It satisfies Performer, so it also backs the CCRClient.

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// dialTimeout bounds the TCP connect to a cluster.
const dialTimeout = 5 * time.Second

// DefaultRequestTimeout bounds one REST request to a DC's cluster. It is generous: a promotion
// detaches unreachable leaders first, so no single call legitimately runs longer, while a DC that
// black-holes traffic now fails a call instead of holding it until the pass deadline.
const DefaultRequestTimeout = 60 * time.Second

// Distribution names the engine behind a RestClient, as in ElasticsearchVersion.spec.distribution
// for the two engines DC-DR supports.
type Distribution string

const (
	DistributionElasticStack Distribution = "ElasticStack"
	DistributionOpenSearch   Distribution = "OpenSearch"
)

// RestClient talks to one Elasticsearch or OpenSearch cluster with basic auth.
type RestClient struct {
	base     *url.URL
	username string
	password string
	hc       *http.Client
	distro   Distribution
	osRoles  OpenSearchReplicationOptions
}

// RestOptions configures a RestClient.
type RestOptions struct {
	// Endpoint is scheme://host:port of the cluster's REST listener.
	Endpoint string
	Username string
	Password string
	// CACert, when set, is the PEM CA bundle that signed the cluster's HTTP certificate.
	CACert []byte
	// InsecureSkipVerify disables TLS verification (test environments only).
	InsecureSkipVerify bool
	// Timeout bounds each request; zero relies on the request context.
	Timeout time.Duration
	// Distribution selects the replication API; empty means Elastic Stack.
	Distribution Distribution
	// OpenSearchReplication sets the use_roles of OpenSearch replication; leave it empty when
	// the security plugin is disabled.
	OpenSearchReplication OpenSearchReplicationOptions
}

// NewRestClient builds a RestClient.
func NewRestClient(o RestOptions) (*RestClient, error) {
	u, err := url.Parse(strings.TrimRight(o.Endpoint, "/"))
	if err != nil {
		return nil, fmt.Errorf("invalid Elasticsearch endpoint %q: %w", o.Endpoint, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("invalid Elasticsearch endpoint %q: scheme must be http or https", o.Endpoint)
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	// A partitioned DC drops packets instead of refusing connections, so the default 30s dial
	// timeout made every hub pass wait 30s on it and delayed failover by several passes.
	tr.DialContext = (&net.Dialer{Timeout: dialTimeout, KeepAlive: 30 * time.Second}).DialContext
	if u.Scheme == "https" {
		cfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: o.InsecureSkipVerify} // nolint:gosec
		if len(o.CACert) > 0 {
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(o.CACert) {
				return nil, fmt.Errorf("invalid CA certificate for %s", o.Endpoint)
			}
			cfg.RootCAs = pool
		}
		tr.TLSClientConfig = cfg
	}
	// Timeout caps each request; context deadlines still apply below it.
	return &RestClient{
		base: u, username: o.Username, password: o.Password,
		hc:     &http.Client{Transport: tr, Timeout: o.Timeout},
		distro: o.Distribution, osRoles: o.OpenSearchReplication,
	}, nil
}

// Endpoint returns the cluster endpoint.
func (c *RestClient) Endpoint() string { return c.base.String() }

// Perform sends req to the cluster, resolving its path against the endpoint. It satisfies
// Performer, so a RestClient also backs the CCRClient.
func (c *RestClient) Perform(req *http.Request) (*http.Response, error) {
	u := *c.base
	u.Path = strings.TrimRight(c.base.Path, "/") + req.URL.Path
	u.RawQuery = req.URL.RawQuery
	req.URL = &u
	req.Host = u.Host
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}
	return c.hc.Do(req)
}

// CCR returns the cross-cluster replication client for this cluster: Elastic CCR, or the
// OpenSearch replication plugin.
func (c *RestClient) CCR() CCRClient {
	if c.distro == DistributionOpenSearch {
		return NewOpenSearchReplicationClient(c, c.osRoles)
	}
	return NewCCRClient(c)
}

// Distribution returns the engine this client was built for.
func (c *RestClient) Distribution() Distribution {
	if c.distro == "" {
		return DistributionElasticStack
	}
	return c.distro
}

// DetectDistribution asks the cluster which engine it runs (GET /, version.distribution).
func (c *RestClient) DetectDistribution(ctx context.Context) (Distribution, error) {
	var out struct {
		Version struct {
			Distribution string `json:"distribution"`
		} `json:"version"`
	}
	if _, err := c.do(ctx, http.MethodGet, "/", nil, &out); err != nil {
		return "", err
	}
	if out.Version.Distribution == "opensearch" {
		return DistributionOpenSearch, nil
	}
	return DistributionElasticStack, nil
}

func (c *RestClient) do(ctx context.Context, method, path string, body any, out any) (int, error) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, path, rdr)
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := c.Perform(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close() // nolint:errcheck
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return res.StatusCode, err
	}
	if res.StatusCode == http.StatusNotFound {
		return res.StatusCode, nil
	}
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return res.StatusCode, fmt.Errorf("%s %s: elasticsearch returned %d: %s", method, path, res.StatusCode, truncate(string(raw), 400))
	}
	if out != nil && len(raw) > 0 {
		return res.StatusCode, json.Unmarshal(raw, out)
	}
	return res.StatusCode, nil
}

// GetDoc reads a document's _source into out. found is false for a missing index or document.
func (c *RestClient) GetDoc(ctx context.Context, index, id string, out any) (bool, error) {
	var res struct {
		Found  bool            `json:"found"`
		Source json.RawMessage `json:"_source"`
	}
	status, err := c.do(ctx, http.MethodGet, "/"+url.PathEscape(index)+"/_doc/"+url.PathEscape(id), nil, &res)
	if err != nil || status == http.StatusNotFound || !res.Found {
		return false, err
	}
	return true, json.Unmarshal(res.Source, out)
}

// PutDoc writes a document and refreshes so the next GetDoc sees it.
func (c *RestClient) PutDoc(ctx context.Context, index, id string, v any) error {
	_, err := c.do(ctx, http.MethodPut, "/"+url.PathEscape(index)+"/_doc/"+url.PathEscape(id)+"?refresh=true", v, nil)
	return err
}

// DeleteDoc deletes a document; a missing document or index is not an error.
func (c *RestClient) DeleteDoc(ctx context.Context, index, id string) error {
	_, err := c.do(ctx, http.MethodDelete, "/"+url.PathEscape(index)+"/_doc/"+url.PathEscape(id)+"?refresh=true", nil, nil)
	return err
}

// DeleteIndices deletes the named indices. Missing indices are ignored.
func (c *RestClient) DeleteIndices(ctx context.Context, indices []string) error {
	if len(indices) == 0 {
		return nil
	}
	esc := make([]string, len(indices))
	for i, ix := range indices {
		esc[i] = url.PathEscape(ix)
	}
	_, err := c.do(ctx, http.MethodDelete, "/"+strings.Join(esc, ",")+"?ignore_unavailable=true", nil, nil)
	return err
}

// NumberOfNodes returns number_of_nodes from GET /_cluster/health.
func (c *RestClient) NumberOfNodes(ctx context.Context) (int32, error) {
	var out struct {
		NumberOfNodes int32 `json:"number_of_nodes"`
	}
	if _, err := c.do(ctx, http.MethodGet, "/_cluster/health", nil, &out); err != nil {
		return 0, err
	}
	return out.NumberOfNodes, nil
}

// ClusterHealth returns the cluster's health status (green, yellow, red) and node count.
func (c *RestClient) ClusterHealth(ctx context.Context) (string, int32, error) {
	var out struct {
		Status        string `json:"status"`
		NumberOfNodes int32  `json:"number_of_nodes"`
	}
	if _, err := c.do(ctx, http.MethodGet, "/_cluster/health", nil, &out); err != nil {
		return "", 0, err
	}
	return out.Status, out.NumberOfNodes, nil
}

// StateRecovered reports whether the cluster has recovered its cluster state. After a full
// restart of its master-eligible nodes a cluster carries the global "state not recovered" block
// (id 1) until the state is restored; until then index listings come back empty instead of
// failing, which must not be read as "the cluster has no indices".
func (c *RestClient) StateRecovered(ctx context.Context) (bool, error) {
	var out struct {
		Blocks struct {
			Global map[string]json.RawMessage `json:"global"`
		} `json:"blocks"`
	}
	if _, err := c.do(ctx, http.MethodGet, "/_cluster/state/blocks", nil, &out); err != nil {
		return false, err
	}
	_, blocked := out.Blocks.Global["1"]
	return !blocked, nil
}

// ProbeWrite writes and deletes a probe document in index, proving the cluster accepts writes
// there.
func (c *RestClient) ProbeWrite(ctx context.Context, index string) error {
	if err := c.PutDoc(ctx, index, "write-probe", map[string]any{"at": time.Now().UTC().Format(time.RFC3339)}); err != nil {
		return err
	}
	return c.DeleteDoc(ctx, index, "write-probe")
}
