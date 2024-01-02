package singlestore

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	core "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	api "kubedb.dev/singlestore/api/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"xorm.io/xorm"
)

type KubeDBClientBuilder struct {
	kc      client.Client
	db      *api.Singlestore
	url     string
	podName string
	ctx     context.Context
}

func NewKubeDBClientBuilder(kc client.Client, db *api.Singlestore) *KubeDBClientBuilder {
	return &KubeDBClientBuilder{
		kc: kc,
		db: db,
	}
}

func (o *KubeDBClientBuilder) WithURL(url string) *KubeDBClientBuilder {
	o.url = url
	return o
}

func (o *KubeDBClientBuilder) WithPod(podName string) *KubeDBClientBuilder {
	o.podName = podName
	return o
}

func (o *KubeDBClientBuilder) WithContext(ctx context.Context) *KubeDBClientBuilder {
	o.ctx = ctx
	return o
}

func (o *KubeDBClientBuilder) GetSinglestoreClient() (*Client, error) {
	if o.ctx == nil {
		o.ctx = context.Background()
	}

	connector, err := o.getConnectionString()
	if err != nil {
		return nil, err
	}

	// connect to database
	db, err := sql.Open("mysql", connector)
	if err != nil {
		return nil, err
	}

	// ping to database to check the connection
	if err := db.PingContext(o.ctx); err != nil {
		closeErr := db.Close()
		if closeErr != nil {
			klog.Errorf("Failed to close client. error: %v", closeErr)
		}
		return nil, err
	}

	return &Client{db}, nil
}

func (o *KubeDBClientBuilder) GetSinglestoreXormClient() (*XormClient, error) {
	if o.ctx == nil {
		o.ctx = context.Background()
	}
	connector, err := o.getConnectionString()
	klog.Infoln("-----------connector: ", connector)
	if err != nil {
		return nil, err
	}
	//klog.Infoln("------------ hi--------------------")
	engine, err := xorm.NewEngine("mysql", connector)
	if err != nil {
		return nil, err
	}
	_, err = engine.Query("SELECT 1")
	if err != nil {
		return nil, err
	}

	engine.SetDefaultContext(o.ctx)
	return &XormClient{
		engine,
	}, nil
}

func (o *KubeDBClientBuilder) getURL() string {
	return fmt.Sprintf("%s.%s.%s.svc", o.podName, o.db.GoverningServiceName(), o.db.Namespace)
}

func (o *KubeDBClientBuilder) getSinglestoreRootCredentials() (string, string, error) {
	db := o.db
	var secretName string
	if db.Spec.AuthSecret != nil {
		secretName = db.GetAuthSecretName()
	}
	var secret core.Secret
	err := o.kc.Get(o.ctx, client.ObjectKey{Namespace: db.Namespace, Name: secretName}, &secret)
	if err != nil {
		return "", "", err
	}
	user, ok := secret.Data[core.BasicAuthUsernameKey]
	if !ok {
		return "", "", fmt.Errorf("DB root user is not set")
	}
	pass, ok := secret.Data[core.BasicAuthPasswordKey]
	if !ok {
		return "", "", fmt.Errorf("DB root password is not set")
	}
	return string(user), string(pass), nil
}

func (o *KubeDBClientBuilder) getConnectionString() (string, error) {
	user, pass, err := o.getSinglestoreRootCredentials()
	if err != nil {
		return "", err
	}

	if o.podName != "" {
		o.url = o.getURL()
	}

	tlsConfig := ""
	/*if o.db.Spec.RequireSSL && o.db.Spec.TLS != nil {
		// get client-secret
		var clientSecret core.Secret
		err := o.kc.Get(o.ctx, client.ObjectKey{Namespace: o.db.GetNamespace(), Name: o.db.GetCertSecretName(api.MySQLClientCert)}, &clientSecret)
		if err != nil {
			return "", err
		}
		cacrt := clientSecret.Data["ca.crt"]
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(cacrt)

		crt := clientSecret.Data["tls.crt"]
		key := clientSecret.Data["tls.key"]
		cert, err := tls.X509KeyPair(crt, key)
		if err != nil {
			return "", err
		}
		var clientCert []tls.Certificate
		clientCert = append(clientCert, cert)

		// tls custom setup
		if o.db.Spec.RequireSSL {
			err = sql_driver.RegisterTLSConfig(api.MySQLTLSConfigCustom, &tls.Config{
				RootCAs:      certPool,
				Certificates: clientCert,
			})
			if err != nil {
				return "", err
			}
			tlsConfig = fmt.Sprintf("tls=%s", api.MySQLTLSConfigCustom)
		} else {
			tlsConfig = fmt.Sprintf("tls=%s", api.MySQLTLSConfigSkipVerify)
		}
	}*/

	connector := fmt.Sprintf("%v:%v@tcp(%s:%d)/%s?%s", user, pass, o.url, 3306, "memsql", tlsConfig)
	return connector, nil
}
