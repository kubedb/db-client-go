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

package yugabytedb

import (
	"testing"

	dbapi "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestAlterRolePassword(t *testing.T) {
	if got, want := AlterRolePasswordSQL("yugabyte", "it's"), `ALTER ROLE "yugabyte" PASSWORD 'it''s'`; got != want {
		t.Errorf("SQL: got %s, want %s", got, want)
	}
	if got, want := AlterRolePasswordCQL("yugabyte", "it's"), `ALTER ROLE "yugabyte" WITH PASSWORD = 'it''s'`; got != want {
		t.Errorf("CQL: got %s, want %s", got, want)
	}
}

func TestHost(t *testing.T) {
	db := &dbapi.YugabyteDB{ObjectMeta: metav1.ObjectMeta{Name: "yb", Namespace: "demo"}}
	cases := []struct {
		name string
		b    *KubeDBClientBuilder
		want string
	}{
		{"service", NewKubeDBClientBuilder(nil, db), "yb.demo.svc"},
		{"pod", NewKubeDBClientBuilder(nil, db).WithPod("yb-tserver-1"), "yb-tserver-1.yb-pods.demo.svc"},
		{"url", NewKubeDBClientBuilder(nil, db).WithURL("h.example"), "h.example"},
	}
	for _, c := range cases {
		got, err := c.b.host()
		if err != nil || got != c.want {
			t.Errorf("%s: got %q, %v; want %q", c.name, got, err, c.want)
		}
	}
	if _, err := NewKubeDBClientBuilder(nil, nil).host(); err == nil {
		t.Error("no database and no URL must be an error")
	}
}

// With explicit credentials and TLS, the builder needs no Kubernetes client.
func TestYSQLConfigWithOverrides(t *testing.T) {
	cfg, err := NewKubeDBClientBuilder(nil, nil).
		WithURL("h.example").
		WithPort(6433).
		WithCredentials("yugabyte", "p@ss word'").
		WithTLSConfig(nil).
		ysqlConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Host != "h.example" || cfg.Port != 6433 || cfg.User != "yugabyte" || cfg.Password != "p@ss word'" || cfg.Database != "yugabyte" {
		t.Errorf("unexpected config: host=%s port=%d user=%s password=%s database=%s", cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database)
	}
	if cfg.TLSConfig != nil {
		t.Error("WithTLSConfig(nil) must connect in plain text")
	}
}

func TestTLSConfigFromPEM(t *testing.T) {
	if _, err := TLSConfigFromPEM("h", []byte("not a certificate"), nil, nil); err == nil {
		t.Error("a CA bundle without a certificate must be an error")
	}
}
