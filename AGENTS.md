# AGENTS.md - db-client-go

This file provides instructions for AI coding agents working in this Go library repository.

## Project Overview

`kubedb.dev/db-client-go` is a Go library that provides unified, KubeDB-aware client builders for connecting to the 30+ database engines managed by the KubeDB Kubernetes operator platform. Each subpackage wraps an upstream Go driver (e.g. `mongo-driver`, `go-elasticsearch`, `lib/pq`) with a `KubeDBClientBuilder` that resolves connection URLs, credentials (Kubernetes Secrets), and TLS certificates from KubeDB CRDs and `controller-runtime` clients.

This is a **library only** — there is no `main` package and no produced binary, despite the `BIN := db-client-go` line in the Makefile. Consumers are KubeDB operators (`kubedb/provisioner`, `kubedb/ops-manager`, `kubedb/autoscaler`, etc.).

## Build & Development Commands

All `make` targets run inside the `ghcr.io/appscode/golang-dev:1.25` Docker image and use vendored dependencies (`GOFLAGS=-mod=vendor`).

```bash
# Format Go sources (goimports + gofmt with interface{}->any rewrite)
make fmt

# Compile all packages
make build

# Run go test across all SRC_PKGS
make test

# Run golangci-lint (config: .golangci.yml)
make lint

# Full CI pipeline: verify + lint + build + test
make ci

# Verify go.mod/go.sum/vendor are tidy
make verify-modules

# License header management
make add-license
make check-license

# Clean build artifacts
make clean
```

To run commands directly on the host (outside Docker):

```bash
go build ./...
go test ./...
golangci-lint run
```

## Project Structure

The repository is a flat collection of per-database subpackages. Each top-level directory under the module root is an independent Go package.

```
cassandra/         ClickHouse-style driver wrapper (gocql)
clickhouse/        clickhouse-go/v2
db2/               IBM DB2 (uses go-ora / hanadb pattern)
druid/             grafadruid/go-druid HTTP client
elasticsearch/     Multi-version: ES v5/v6/v7/v8/v9 + OpenSearch v1/v2/v3
elasticsearchdashboard/  Kibana/OpenSearch-Dashboards REST client
hanadb/            SAP HANA (SAP/go-hdb)
hazelcast/         hazelcast-go-client
ignite/            Apache Ignite (amsokol/ignite-go-client)
kafka/             Sarama-based broker client
  kafka/connect/         Kafka Connect REST
  kafka/restproxy/       Confluent REST proxy
  kafka/schemaregistry/  Schema Registry REST
mariadb/           go-sql-driver/mysql
memcached/         kubedb/gomemcache fork
milvus/            milvus-io/milvus client/v2
mongodb/           go.mongodb.org/mongo-driver
mssqlserver/       microsoft/go-mssqldb
mysql/             go-sql-driver/mysql
neo4j/             neo4j-go-driver/v5 (also Cypher helpers in query.go)
oracle/            sijms/go-ora
perconaxtradb/     go-sql-driver/mysql
pgbouncer/         lib/pq via xorm
pgpool/            lib/pq
postgres/          lib/pq via xorm
proxysql/          go-sql-driver/mysql
qdrant/            qdrant/go-client (gRPC)
rabbitmq/          rabbit-hole/v3 (HTTP) + amqp091-go (AMQP)
redis/             redis/go-redis/v9 (+ clusterclient.go)
redissentinel/     redis/go-redis/v9 Sentinel mode
singlestore/       go-sql-driver/mysql
solr/              REST client (v8 + v9 variants)
weaviate/          weaviate-go-client/v5
zookeeper/         Shopify/zk

hack/              Build/test/format shell scripts and license header
  build.sh         Invoked by `make build`
  test.sh          Invoked by `make test`
  fmt.sh           Invoked by `make fmt`
  license/         License header template for ltag
vendor/            Vendored module dependencies
.github/workflows/ci.yml, release.yml, release-tracker.yml
```

The Makefile defines the canonical package list in `SRC_PKGS`:

```
cassandra clickhouse db2 druid elasticsearch elasticsearchdashboard
hack hazelcast ignite kafka mariadb memcached mongodb mssqlserver
mysql neo4j oracle perconaxtradb pgbouncer pgpool postgres proxysql
qdrant rabbitmq redis redissentinel singlestore solr zookeeper
```

When adding a new database package, append it to `SRC_PKGS` in `Makefile`.

## Key Packages / APIs

Every database subpackage follows the same builder pattern. The exemplar shape:

```go
type KubeDBClientBuilder struct {
    kc      client.Client       // controller-runtime client
    db      *dbapi.<DBKind>     // KubeDB CRD object
    url     string
    podName string
    ctx     context.Context
    // ...driver-specific fields (auth DB, replica set, TLS certs, etc.)
}

func NewKubeDBClientBuilder(kc client.Client, db *dbapi.<DBKind>) *KubeDBClientBuilder
func (o *KubeDBClientBuilder) WithURL(url string) *KubeDBClientBuilder
func (o *KubeDBClientBuilder) WithPod(podName string) *KubeDBClientBuilder
func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder
func (o *KubeDBClientBuilder) GetXXXClient() (*Client, error)
```

`Client` is typically a thin struct that embeds the upstream driver client and adds a `Close()` method that logs via `k8s.io/klog/v2`. Example (`mongodb/client.go`):

```go
type Client struct{ *mongo.Client }
func (c *Client) Close() { _ = c.Disconnect(context.TODO()) }
```

Multi-version drivers (e.g. `elasticsearch/`) keep one file per major version: `es_client_v5.go`, ..., `es_client_v9.go`, `os_client_v1.go`–`os_client_v3.go`. Version selection happens in `kubedb_client_builder.go` based on the CRD spec.

CRD types come from `kubedb.dev/apimachinery/apis/kubedb/v1` (aliased as `dbapi`). Credentials/TLS resolution uses `kmodules.xyz/client-go/tools/certholder` and Kubernetes `core.Secret` lookups.

## Testing

There are **no `_test.go` files in the repository**. `make test` runs `go test` across all `SRC_PKGS` but currently only validates that packages compile. Validation is therefore:

1. `make build` — all packages compile
2. `make lint` — golangci-lint passes with the rules in `.golangci.yml`
3. `make verify-modules` — `go mod tidy && go mod vendor` produces no diff
4. CI (`.github/workflows/ci.yml`) runs `make ci` on every PR

When adding a new client, real-world verification is performed by integrating it into the downstream KubeDB operator and running its e2e tests.

## Dependencies

Go version: **1.25.5** (see `go.mod`; CI uses Go 1.25).

Dependencies are **vendored** — always run `go mod vendor` after touching `go.mod`. `make verify-modules` enforces this.

Core direct dependencies (one per supported engine, plus shared infrastructure):

| Group | Modules |
|---|---|
| Kubernetes / KubeDB | `k8s.io/api`, `k8s.io/apimachinery`, `k8s.io/klog/v2`, `sigs.k8s.io/controller-runtime`, `kubedb.dev/apimachinery`, `kmodules.xyz/client-go`, `kmodules.xyz/custom-resources` |
| SQL | `github.com/go-sql-driver/mysql`, `github.com/lib/pq`, `xorm.io/xorm`, `github.com/microsoft/go-mssqldb`, `github.com/sijms/go-ora/v2`, `github.com/SAP/go-hdb` |
| NoSQL / Document | `go.mongodb.org/mongo-driver`, `github.com/redis/go-redis/v9`, `github.com/gocql/gocql` |
| Search / Analytics | `github.com/elastic/go-elasticsearch/v{5,6,7,8,9}`, `github.com/opensearch-project/opensearch-go{,v2,v3}`, `github.com/ClickHouse/clickhouse-go/v2`, `github.com/grafadruid/go-druid` |
| Streaming / Queue | `github.com/IBM/sarama`, `github.com/rabbitmq/amqp091-go`, `github.com/michaelklishin/rabbit-hole/v3` |
| Vector / Graph | `github.com/qdrant/go-client`, `github.com/milvus-io/milvus/client/v2`, `github.com/weaviate/weaviate-go-client/v5`, `github.com/neo4j/neo4j-go-driver/v5` |
| Misc | `github.com/Shopify/zk`, `github.com/hazelcast/hazelcast-go-client`, `github.com/amsokol/ignite-go-client`, `github.com/kubedb/gomemcache` |

Notable `replace` directives in `go.mod`:

- `sigs.k8s.io/controller-runtime` → `github.com/kmodules/controller-runtime` (KubeDB fork)
- `k8s.io/apiserver` → `github.com/kmodules/apiserver` (KubeDB fork)
- `github.com/imdario/mergo` pinned to `v0.3.6`
- `github.com/Masterminds/sprig/v3` → `github.com/gomodules/sprig/v3`

## Code Conventions

- Every Go file must carry the AppsCode Apache-2.0 license header. Enforced by `make check-license`; add via `make add-license`. Template lives in `hack/license/`.
- `make fmt` runs goimports + gofmt and rewrites `interface{}` to `any` (see `.golangci.yml`).
- Imports are grouped: stdlib, then `kubedb.dev/...` (project-internal), then third-party, then `k8s.io/...` / `kmodules.xyz/...` / `sigs.k8s.io/...`. Existing files demonstrate the convention.
- Builder methods follow `With<Field>(value) *KubeDBClientBuilder` and return the receiver for chaining.
- Logging uses `k8s.io/klog/v2` (not `log` or `fmt.Println`).
- Generated files (`generated.*.go`) and the `client/` directory are excluded from lint.
