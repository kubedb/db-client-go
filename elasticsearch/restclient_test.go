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
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRestClientStateRecovered(t *testing.T) {
	for name, tc := range map[string]struct {
		body string
		want bool
	}{
		"recovered":     {`{}`, true},
		"other blocks":  {`{"blocks":{"global":{"2":{"description":"no master"}}}}`, true},
		"not recovered": {`{"blocks":{"global":{"1":{"description":"state not recovered / initialized"}}}}`, false},
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/_cluster/state/blocks" {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("content-type", "application/json")
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			c, err := NewRestClient(RestOptions{Endpoint: srv.URL})
			if err != nil {
				t.Fatal(err)
			}
			got, err := c.StateRecovered(context.Background())
			if err != nil || got != tc.want {
				t.Fatalf("StateRecovered = %v, %v; want %v", got, err, tc.want)
			}
		})
	}
}
