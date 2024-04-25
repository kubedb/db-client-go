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

package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	"go.mongodb.org/mongo-driver/mongo"
	mgoptions "go.mongodb.org/mongo-driver/mongo/options"
	core "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"kmodules.xyz/client-go/tools/certholder"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubeDBClientBuilder struct {
	kc           client.Client
	db           *api.MongoDB
	url          string
	podName      string
	repSetName   string
	direct       bool
	certs        *certholder.ResourceCerts
	ctx          context.Context
	cred         string
	authDatabase string
}

func NewKubeDBClientBuilder(kc client.Client, db *api.MongoDB) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc:     kc,
		db:     db,
		direct: false,
	}
}

// To connect with a specific user.
// cred formet: "username:password"
func (o *KubeDBClientBuilder) WithCred(cred string) *KubeDBClientBuilder {
	o.cred = cred
	return o
}

func (o *KubeDBClientBuilder) WithAuthDatabase(authDatabase string) *KubeDBClientBuilder {
	o.authDatabase = authDatabase
	return o
}

func (o *KubeDBClientBuilder) WithURL(url string) *KubeDBClientBuilder {
	o.url = url
	return o
}

func (o *KubeDBClientBuilder) WithPod(podName string) *KubeDBClientBuilder {
	o.podName = podName
	o.direct = true
	return o
}

func (o *KubeDBClientBuilder) WithReplSet(replSetName string) *KubeDBClientBuilder {
	o.repSetName = replSetName
	return o
}

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}

func (o *KubeDBClientBuilder) WithDirect() *KubeDBClientBuilder {
	o.direct = true
	return o
}

func (o *KubeDBClientBuilder) WithCerts(certs *certholder.ResourceCerts) *KubeDBClientBuilder {
	o.certs = certs
	return o
}

func (o *KubeDBClientBuilder) GetMongoClient() (*Client, error) {
	db := o.db

	if o.ctx == nil {
		o.ctx = context.Background()
	}

	if o.podName != "" {
		o.url = o.getURL()
	}

	if o.podName == "" && o.url == "" {
		if db.Spec.ShardTopology != nil {
			// Shard
			o.url = strings.Join(db.MongosHosts(), ",")
		} else {
			// Standalone or ReplicaSet
			o.url = strings.Join(db.Hosts(), ",")
		}
	}

	clientOpts, err := o.getMongoDBClientOpts()
	if err != nil {
		return nil, err
	}

	client, err := mongo.Connect(o.ctx, clientOpts)
	if err != nil {
		return nil, err
	}

	err = client.Ping(o.ctx, nil)
	if err != nil {
		disconnectErr := client.Disconnect(o.ctx)
		if disconnectErr != nil {
			klog.Errorf("Failed to disconnect client. error: %v", disconnectErr)
		}
		return nil, err
	}

	return &Client{
		Client: client,
	}, nil
}

func (o *KubeDBClientBuilder) getURL() string {
	nodeType := o.podName[:strings.LastIndex(o.podName, "-")]
	if strings.HasSuffix(nodeType, api.NodeTypeArbiter) {
		// nodeType looks like <DB_NAME>-shard<SHARD_NUMBER>-arbiter for shard, <DB_NAME>-arbiter otherwise.
		// so excluding  '-arbiter' will give us the stsName where this arbiter belongs as a member of rs
		nodeType = nodeType[:strings.LastIndex(nodeType, "-")]
	}
	return fmt.Sprintf("%s.%s.%s.svc", o.podName, o.db.GoverningServiceName(nodeType), o.db.Namespace)
}

func (o *KubeDBClientBuilder) getMongoDBClientOpts() (*mgoptions.ClientOptions, error) {
	db := o.db
	repSetConfig := ""
	if o.repSetName != "" {
		repSetConfig = "replicaSet=" + o.repSetName + "&"
	}
	authDatabaseConfig := ""
	if o.authDatabase != "" {
		authDatabaseConfig = "authSource=" + o.authDatabase
	}

	cred := o.cred
	if cred == "" {
		user, pass, err := o.getMongoDBRootCredentials()
		if err != nil {
			return nil, err
		}
		cred = fmt.Sprintf("%s:%s", user, pass)
	}

	var clientOpts *mgoptions.ClientOptions
	if db.Spec.TLS != nil {
		if o.authDatabase != "" {
			authDatabaseConfig = "&" + authDatabaseConfig
		}
		secretName := db.GetCertSecretName(api.MongoDBClientCert, "")
		var (
			paths *certholder.Paths
			err   error
		)
		if o.certs == nil {
			var certSecret core.Secret
			err = o.kc.Get(o.ctx, client.ObjectKey{Namespace: db.Namespace, Name: secretName}, &certSecret)
			if err != nil {
				klog.Error(err, "failed to get certificate secret. ", secretName)
				return nil, err
			}

			certs, _ := certholder.DefaultHolder.
				ForResource(api.SchemeGroupVersion.WithResource(api.ResourcePluralMongoDB), db.ObjectMeta)
			_, err = certs.Save(&certSecret)
			if err != nil {
				klog.Error(err, "failed to save certificate")
				return nil, err
			}

			paths, err = certs.Get(secretName)
			if err != nil {
				return nil, err
			}
		} else {
			paths, err = o.certs.Get(secretName)
			if err != nil {
				return nil, err
			}
		}

		uri := fmt.Sprintf("mongodb://%s@%s/admin?%vtls=true&tlsCAFile=%v&tlsCertificateKeyFile=%v%v", cred, o.url, repSetConfig, paths.CACert, paths.Pem, authDatabaseConfig)
		clientOpts = mgoptions.Client().ApplyURI(uri)
	} else {
		clientOpts = mgoptions.Client().ApplyURI(fmt.Sprintf("mongodb://%s@%s/admin?%vauthSource=%v", cred, o.url, repSetConfig, authDatabaseConfig))
	}

	clientOpts.SetDirect(o.direct)
	clientOpts.SetConnectTimeout(5 * time.Second)

	return clientOpts, nil
}

func (o *KubeDBClientBuilder) getMongoDBRootCredentials() (string, string, error) {
	db := o.db
	if db.Spec.AuthSecret == nil {
		return "", "", errors.New("no database secret")
	}
	var secret core.Secret
	err := o.kc.Get(o.ctx, client.ObjectKey{Namespace: db.Namespace, Name: db.Spec.AuthSecret.Name}, &secret)
	if err != nil {
		return "", "", err
	}
	return string(secret.Data[core.BasicAuthUsernameKey]), string(secret.Data[core.BasicAuthPasswordKey]), nil
}
