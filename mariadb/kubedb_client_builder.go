package mariadb

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"log"
	"time"

	sql_driver "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
)

const (
	rootCAKey = "ca.crt"
)

type kubeDBClientBuilder struct {
	kubeClient kubernetes.Interface
	db         *api.MariaDB
	url        string
	podName    string
}

func NewKubeDBClientBuilder(db *api.MariaDB, kubeClient kubernetes.Interface) *kubeDBClientBuilder {
	return &kubeDBClientBuilder{
		kubeClient: kubeClient,
		db:         db,
	}
}

func (o *kubeDBClientBuilder) WithURL(url string) *kubeDBClientBuilder {
	o.url = url
	return o
}

func (o *kubeDBClientBuilder) WithPod(podName string) *kubeDBClientBuilder {
	o.podName = podName
	return o
}

// GetMariaDBXormClient return xorm engine for MariaDB
func (o *kubeDBClientBuilder) GetMariaDBXormClient() (Client, error) {
	connector, err := o.getConnectionString()
	if err != nil {
		return nil, err
	}
	engine, err := xorm.NewEngine("mysql", connector)
	if err != nil {
		return nil, err
	}
	_, err = engine.Query("SELECT 1")
	if err != nil {
		return nil, err
	}
	return &xormClient{
		Engine: engine,
	}, nil
}

// GetMariaDBClient return MariaDB go-client backed by database/sql package
func (o *kubeDBClientBuilder) GetMariaDBClient() (Client, error) {
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		log.Fatal(err)
	}

	return &sqlClient{
		DB: db,
	}, nil
}

func (o *kubeDBClientBuilder) getMariaDBBasicAuth() (string, string, error) {
	var secretName string
	if o.db.Spec.AuthSecret != nil {
		secretName = o.db.GetAuthSecretName()
	}
	secret, err := o.kubeClient.CoreV1().Secrets(o.db.Namespace).Get(context.TODO(), secretName, metav1.GetOptions{})
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

func (o *kubeDBClientBuilder) SSLEnabledMariaDB() bool {
	return o.db.Spec.TLS != nil && o.db.Spec.RequireSSL
}

func (o *kubeDBClientBuilder) getURL() string {
	return fmt.Sprintf("%s.%s.%s.svc", o.podName, o.db.GoverningServiceName(), o.db.Namespace)
}

func (o *kubeDBClientBuilder) getConnectionString() (string, error) {
	user, pass, err := o.getMariaDBBasicAuth()
	if err != nil {
		return "", err
	}

	if o.podName != "" {
		o.url = o.getURL()
	}

	tlsConfig := ""
	if o.SSLEnabledMariaDB() {
		// get client-secret
		clientSecret, err := o.kubeClient.CoreV1().Secrets(o.db.GetNamespace()).Get(context.TODO(), o.db.GetCertSecretName(api.MariaDBClientCert), metav1.GetOptions{})
		if err != nil {
			return "", err
		}

		value, exists := clientSecret.Data[rootCAKey]
		if !exists {
			return "", fmt.Errorf("%v in not present in client secret", rootCAKey)
		}
		cacrt := value
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(cacrt)

		value, exists = clientSecret.Data[core.TLSCertKey]
		if !exists {
			return "", fmt.Errorf("%v in not present in client secret", core.TLSCertKey)
		}
		crt := value

		value, exists = clientSecret.Data[core.TLSPrivateKeyKey]
		if !exists {
			return "", fmt.Errorf("%v in not present in client secret", core.TLSPrivateKeyKey)
		}
		key := value

		cert, err := tls.X509KeyPair(crt, key)
		if err != nil {
			return "", err
		}
		var clientCert []tls.Certificate
		clientCert = append(clientCert, cert)
		err = sql_driver.RegisterTLSConfig(api.MariaDBTLSConfigCustom, &tls.Config{
			RootCAs:      certPool,
			Certificates: clientCert,
		})
		if err != nil {
			return "", err
		}
		tlsConfig = fmt.Sprintf("tls=%s", api.MariaDBTLSConfigCustom)
	}

	connector := fmt.Sprintf("%v:%v@tcp(%s:%d)/%s?%s", user, pass, o.url, 3306, "mysql", tlsConfig)
	return connector, nil
}
