module kubedb.dev/db-client-go

go 1.25.5

require (
	github.com/ClickHouse/clickhouse-go/v2 v2.23.2
	github.com/IBM/sarama v1.45.2
	github.com/Masterminds/semver/v3 v3.3.1
	github.com/SAP/go-hdb v1.12.0
	github.com/Shopify/zk v1.0.12
	github.com/amsokol/ignite-go-client v0.12.2
	github.com/elastic/go-elasticsearch/v5 v5.6.1
	github.com/elastic/go-elasticsearch/v6 v6.8.10
	github.com/elastic/go-elasticsearch/v7 v7.15.1
	github.com/elastic/go-elasticsearch/v8 v8.4.0
	github.com/elastic/go-elasticsearch/v9 v9.1.0
	github.com/go-logr/logr v1.4.3
	github.com/go-resty/resty/v2 v2.11.0
	github.com/go-sql-driver/mysql v1.9.1
	github.com/gocql/gocql v1.6.0
	github.com/grafadruid/go-druid v0.0.6
	github.com/hazelcast/hazelcast-go-client v1.4.2
	github.com/kubedb/gomemcache v0.0.0-20240930104810-7694d68c3e21
	github.com/lib/pq v1.10.7
	github.com/michaelklishin/rabbit-hole/v3 v3.1.0
	github.com/microsoft/go-mssqldb v1.6.0
	github.com/milvus-io/milvus/client/v2 v2.6.1
	github.com/neo4j/neo4j-go-driver/v5 v5.28.4
	github.com/opensearch-project/opensearch-go v1.1.0
	github.com/opensearch-project/opensearch-go/v2 v2.3.0
	github.com/pkg/errors v0.9.1
	github.com/qdrant/go-client v1.15.0
	github.com/rabbitmq/amqp091-go v1.10.0
	github.com/redis/go-redis/v9 v9.5.1
	github.com/sijms/go-ora/v2 v2.8.24
	github.com/weaviate/weaviate-go-client/v5 v5.2.1
	go.mongodb.org/mongo-driver v1.14.0
	go.virtual-secrets.dev/apimachinery v0.0.1
	gomodules.xyz/pointer v0.1.0
	google.golang.org/grpc v1.71.0
	k8s.io/api v0.32.8
	k8s.io/apimachinery v0.32.8
	k8s.io/client-go v0.32.8
	k8s.io/klog/v2 v2.130.1
	kmodules.xyz/client-go v0.32.10
	kmodules.xyz/custom-resources v0.32.2
	kubedb.dev/apimachinery v0.60.0-rc.0
	sigs.k8s.io/controller-runtime v0.20.4
	xorm.io/xorm v1.3.9
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/ClickHouse/ch-go v0.61.5 // indirect
	github.com/Masterminds/semver v1.4.2 // indirect
	github.com/andybalholm/brotli v1.1.0 // indirect
	github.com/asaskevich/govalidator v0.0.0-20230301143203-a9d515a09cc2 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/blang/semver/v4 v4.0.0 // indirect
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/cert-manager/cert-manager v1.18.2 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cilium/ebpf v0.11.0 // indirect
	github.com/cockroachdb/errors v1.9.1 // indirect
	github.com/cockroachdb/logtags v0.0.0-20211118104740-dabe8e521a4f // indirect
	github.com/cockroachdb/redact v1.1.3 // indirect
	github.com/containerd/cgroups/v3 v3.0.3 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/cyphar/filepath-securejoin v0.6.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/eapache/go-resiliency v1.7.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20230731223053-c322873962e3 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/elastic/elastic-transport-go/v8 v8.7.0 // indirect
	github.com/emicklei/go-restful/v3 v3.12.1 // indirect
	github.com/evanphx/json-patch v5.9.11+incompatible // indirect
	github.com/evanphx/json-patch/v5 v5.9.11 // indirect
	github.com/fatih/structs v1.1.0 // indirect
	github.com/fsnotify/fsnotify v1.8.0 // indirect
	github.com/fxamacker/cbor/v2 v2.7.0 // indirect
	github.com/getsentry/sentry-go v0.30.0 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.7.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-logr/zapr v1.3.0 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-openapi/analysis v0.23.0 // indirect
	github.com/go-openapi/errors v0.22.0 // indirect
	github.com/go-openapi/jsonpointer v0.21.0 // indirect
	github.com/go-openapi/jsonreference v0.21.0 // indirect
	github.com/go-openapi/loads v0.22.0 // indirect
	github.com/go-openapi/runtime v0.24.2 // indirect
	github.com/go-openapi/spec v0.21.0 // indirect
	github.com/go-openapi/strfmt v0.23.0 // indirect
	github.com/go-openapi/swag v0.23.0 // indirect
	github.com/go-openapi/validate v0.24.0 // indirect
	github.com/goccy/go-json v0.10.4 // indirect
	github.com/godbus/dbus/v5 v5.0.4 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt/v4 v4.5.2 // indirect
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/google/gnostic-models v0.6.9 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/go-containerregistry v0.20.3 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.25.1 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-retryablehttp v0.7.7 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/imdario/mergo v0.3.16 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jonboulle/clockwork v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.9 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20240909124753-873cd0166683 // indirect
	github.com/magefile/mage v1.11.0 // indirect
	github.com/mailru/easyjson v0.9.0 // indirect
	github.com/milvus-io/milvus-proto/go-api/v2 v2.6.3 // indirect
	github.com/milvus-io/milvus/pkg/v2 v2.6.3 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/montanaflynn/stats v0.0.0-20171201202039-1bf9dbcd8cbe // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/onsi/gomega v1.36.2 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/runtime-spec v1.0.2 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/panjf2000/ants/v2 v2.11.3 // indirect
	github.com/paulmach/orb v0.11.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring v0.81.0 // indirect
	github.com/prometheus/client_golang v1.20.5 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.62.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/rogpeppe/go-internal v1.13.1 // indirect
	github.com/samber/lo v1.27.0 // indirect
	github.com/segmentio/asm v1.2.0 // indirect
	github.com/sergi/go-diff v1.3.1 // indirect
	github.com/shirou/gopsutil/v3 v3.24.5 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/soheilhy/cmux v0.1.5 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/cast v1.7.0 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	github.com/syndtr/goleveldb v1.0.0 // indirect
	github.com/tidwall/gjson v1.17.1 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tklauser/go-sysconf v0.3.14 // indirect
	github.com/tklauser/numcpus v0.9.0 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20220101234140-673ab2c3ae75 // indirect
	github.com/uber/jaeger-client-go v2.30.0+incompatible // indirect
	github.com/weaviate/weaviate v1.30.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/xiang90/probing v0.0.0-20221125231312-a49e3df8f510 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240424034433-3c2c7870ae76 // indirect
	github.com/yudai/gojsondiff v1.0.0 // indirect
	github.com/yudai/golcs v0.0.0-20170316035057-ecda9a501e82 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	go.etcd.io/bbolt v1.3.11 // indirect
	go.etcd.io/etcd/api/v3 v3.5.17 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.17 // indirect
	go.etcd.io/etcd/client/v2 v2.305.16 // indirect
	go.etcd.io/etcd/client/v3 v3.5.17 // indirect
	go.etcd.io/etcd/pkg/v3 v3.5.16 // indirect
	go.etcd.io/etcd/raft/v3 v3.5.16 // indirect
	go.etcd.io/etcd/server/v3 v3.5.16 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.60.0 // indirect
	go.opentelemetry.io/otel v1.35.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.33.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.33.0 // indirect
	go.opentelemetry.io/otel/metric v1.35.0 // indirect
	go.opentelemetry.io/otel/sdk v1.35.0 // indirect
	go.opentelemetry.io/otel/trace v1.35.0 // indirect
	go.opentelemetry.io/proto/otlp v1.4.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/automaxprocs v1.5.3 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	golang.org/x/crypto v0.45.0 // indirect
	golang.org/x/exp v0.0.0-20241217172543-b2144cdd0a67 // indirect
	golang.org/x/net v0.47.0 // indirect
	golang.org/x/oauth2 v0.28.0 // indirect
	golang.org/x/sync v0.18.0 // indirect
	golang.org/x/sys v0.38.0 // indirect
	golang.org/x/term v0.37.0 // indirect
	golang.org/x/text v0.31.0 // indirect
	golang.org/x/time v0.11.0 // indirect
	gomodules.xyz/clock v0.0.0-20200817085942-06523dba733f // indirect
	gomodules.xyz/encoding v0.0.8 // indirect
	gomodules.xyz/jsonpatch/v2 v2.5.0 // indirect
	gomodules.xyz/mergo v0.3.13 // indirect
	gomodules.xyz/sets v0.2.1 // indirect
	gomodules.xyz/sync v0.1.0 // indirect
	gomodules.xyz/wait v0.2.0 // indirect
	gomodules.xyz/x v0.0.17 // indirect
	google.golang.org/genproto v0.0.0-20250324211829-b45e905df463 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250324211829-b45e905df463 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250324211829-b45e905df463 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
	gopkg.in/evanphx/json-patch.v4 v4.12.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/apiextensions-apiserver v0.32.8 // indirect
	k8s.io/apiserver v0.32.8 // indirect
	k8s.io/kube-openapi v0.0.0-20250318190949-c8a335a9a2ff // indirect
	k8s.io/utils v0.0.0-20250321185631-1f6e0b77f77e // indirect
	kmodules.xyz/apiversion v0.2.0 // indirect
	kmodules.xyz/monitoring-agent-api v0.32.4 // indirect
	kmodules.xyz/objectstore-api v0.32.2 // indirect
	kmodules.xyz/offshoot-api v0.32.0 // indirect
	kmodules.xyz/prober v0.32.0 // indirect
	kmodules.xyz/resource-metadata v0.32.1 // indirect
	kubeops.dev/operator-shard-manager v0.0.4 // indirect
	kubeops.dev/petset v0.0.15 // indirect
	kubeops.dev/sidekick v0.0.12 // indirect
	kubestash.dev/apimachinery v0.22.0-rc.0 // indirect
	open-cluster-management.io/api v1.0.0 // indirect
	sigs.k8s.io/gateway-api v1.1.0 // indirect
	sigs.k8s.io/json v0.0.0-20241014173422-cfa47c3a1cc8 // indirect
	sigs.k8s.io/randfill v1.0.0 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.6.0 // indirect
	sigs.k8s.io/yaml v1.4.0 // indirect
	stash.appscode.dev/apimachinery v0.41.0 // indirect
	xorm.io/builder v0.3.11-0.20220531020008-1bd24a7dc978 // indirect
)

replace github.com/Masterminds/sprig/v3 => github.com/gomodules/sprig/v3 v3.2.3-0.20220405051441-0a8a99bac1b8

replace sigs.k8s.io/controller-runtime => github.com/kmodules/controller-runtime v0.20.3-0.20250221050548-8eabe54e7dda

replace github.com/imdario/mergo => github.com/imdario/mergo v0.3.6

replace k8s.io/apiserver => github.com/kmodules/apiserver v0.32.3-0.20250221062720-35dc674c7dd6
