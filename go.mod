module kubedb.dev/db-client-go

go 1.18

require (
	github.com/Masterminds/semver/v3 v3.1.1
	github.com/elastic/go-elasticsearch/v6 v6.8.10
	github.com/elastic/go-elasticsearch/v7 v7.17.1
	github.com/go-redis/redis/v8 v8.11.5
	github.com/go-sql-driver/mysql v1.6.0
	github.com/pkg/errors v0.9.1
	go.mongodb.org/mongo-driver v1.9.1
	k8s.io/api v0.24.1
	k8s.io/klog/v2 v2.60.1
	kmodules.xyz/client-go v0.24.0
	kubedb.dev/apimachinery v0.27.1-0.20220604010336-666ce9c7cf9f
	sigs.k8s.io/controller-runtime v0.12.1
	xorm.io/xorm v1.3.1
)

require (
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/emicklei/go-restful v2.15.0+incompatible // indirect
	github.com/evanphx/json-patch v4.12.0+incompatible // indirect
	github.com/fatih/structs v1.1.0 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-openapi/jsonpointer v0.19.5 // indirect
	github.com/go-openapi/jsonreference v0.20.0 // indirect
	github.com/go-openapi/swag v0.21.1 // indirect
	github.com/go-stack/stack v1.8.0 // indirect
	github.com/goccy/go-json v0.8.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/gnostic v0.6.9 // indirect
	github.com/google/go-cmp v0.5.8 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/imdario/mergo v0.3.13 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.15.4 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/sergi/go-diff v1.2.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/syndtr/goleveldb v1.0.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.0.2 // indirect
	github.com/xdg-go/stringprep v1.0.2 // indirect
	github.com/youmark/pkcs8 v0.0.0-20181117223130-1be2e3e5546d // indirect
	github.com/yudai/gojsondiff v1.0.0 // indirect
	github.com/yudai/golcs v0.0.0-20170316035057-ecda9a501e82 // indirect
	golang.org/x/crypto v0.0.0-20220525230936-793ad666bf5e // indirect
	golang.org/x/net v0.0.0-20220531201128-c960675eff93 // indirect
	golang.org/x/oauth2 v0.0.0-20220524215830-622c5d57e401 // indirect
	golang.org/x/sync v0.0.0-20220513210516-0976fa681c29 // indirect
	golang.org/x/sys v0.0.0-20220520151302-bc2c85ada10a // indirect
	golang.org/x/term v0.0.0-20220526004731-065cf7ba2467 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/time v0.0.0-20220411224347-583f2d630306 // indirect
	gomodules.xyz/jsonpatch/v2 v2.2.0 // indirect
	gomodules.xyz/mergo v0.3.13 // indirect
	gomodules.xyz/pointer v0.1.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/protobuf v1.28.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.0 // indirect
	k8s.io/apiextensions-apiserver v0.24.1 // indirect
	k8s.io/apimachinery v0.24.1 // indirect
	k8s.io/client-go v0.24.1 // indirect
	k8s.io/kube-openapi v0.0.0-20220413171646-5e7f5fdc6da6 // indirect
	k8s.io/utils v0.0.0-20220210201930-3a6ce19ff2f9 // indirect
	kmodules.xyz/custom-resources v0.24.0 // indirect
	kmodules.xyz/monitoring-agent-api v0.24.0 // indirect
	kmodules.xyz/offshoot-api v0.24.1 // indirect
	sigs.k8s.io/json v0.0.0-20220525155127-227cbc7cc124 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.2.1 // indirect
	sigs.k8s.io/yaml v1.3.0 // indirect
	xorm.io/builder v0.3.11-0.20220531020008-1bd24a7dc978 // indirect
)

replace sigs.k8s.io/controller-runtime => github.com/kmodules/controller-runtime v0.12.2-0.20220603144237-6cd001896bf3

replace github.com/imdario/mergo => github.com/imdario/mergo v0.3.5

replace k8s.io/apimachinery => github.com/kmodules/apimachinery v0.24.2-rc.0.0.20220603191800-1c7484099dee

replace k8s.io/apiserver => github.com/kmodules/apiserver v0.0.0-20220603223637-59dad1716c43

replace k8s.io/kubernetes => github.com/kmodules/kubernetes v1.25.0-alpha.0.0.20220603172133-1c9d09d1b3b8
