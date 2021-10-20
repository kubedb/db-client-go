package mysql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"

	sql_driver "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
)

type kubeDBClientBuilder struct {
	kubeClient kubernetes.Interface
	db         *api.MySQL
	url        string
	podName    string
}

func NewKubeDBClientBuilder(db *api.MySQL, kubeClient kubernetes.Interface) *kubeDBClientBuilder {
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

func (o *kubeDBClientBuilder) GetMySQLClient() (*Client, error) {
	user, pass, err := o.getMySQLRootCredentials()
	if err != nil {
		return nil, err
	}

	if o.podName != "" {
		o.url = o.getURL()
	}

	tlsConfig := ""
	if o.db.Spec.RequireSSL && o.db.Spec.TLS != nil {
		// get client-secret
		clientSecret, err := o.kubeClient.CoreV1().Secrets(o.db.GetNamespace()).Get(context.TODO(), o.db.MustCertSecretName(api.MySQLClientCert), metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		cacrt := clientSecret.Data["ca.crt"]
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(cacrt)

		crt := clientSecret.Data["tls.crt"]
		key := clientSecret.Data["tls.key"]
		cert, err := tls.X509KeyPair(crt, key)
		if err != nil {
			return nil, err
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
				return nil, err
			}
			tlsConfig = fmt.Sprintf("tls=%s", api.MySQLTLSConfigCustom)
		} else {
			tlsConfig = fmt.Sprintf("tls=%s", api.MySQLTLSConfigSkipVerify)
		}
	}

	connector := fmt.Sprintf("%v:%v@tcp(%s:%d)/%s?%s", user, pass, o.url, 3306, "mysql", tlsConfig)
	engine, err := xorm.NewEngine("mysql", connector)
	if err != nil {
		return nil, err
	}
	_, err = engine.Query("SELECT 1")
	if err != nil {
		return nil, err
	}
	return &Client{
		Engine: engine,
	}, nil
}

func (o *kubeDBClientBuilder) getURL() string {
	return fmt.Sprintf("%s.%s.%s.svc", o.podName, o.db.GoverningServiceName(), o.db.Namespace)
}

func (o *kubeDBClientBuilder) getMySQLRootCredentials() (string, string, error) {
	db := o.db
	var secretName string
	if db.Spec.AuthSecret != nil {
		secretName = db.GetAuthSecretName()
	}
	secret, err := o.kubeClient.CoreV1().Secrets(db.Namespace).Get(context.Background(), secretName, metav1.GetOptions{})
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
