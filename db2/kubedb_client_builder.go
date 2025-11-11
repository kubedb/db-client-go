package db2

import (
	"context"
	"database/sql"
	"fmt"

	//olddbapi "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	dbapi "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	apiutils "kubedb.dev/apimachinery/pkg/utils"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type db2ClientBuilder struct {
	kc      client.Client
	db      *dbapi.DB2
	url     string
	port    int32
	service string
	ctx     context.Context
}

func NewDB2ClientBuilder(kc client.Client, db *dbapi.DB2) *db2ClientBuilder {
	return &db2ClientBuilder{
		kc: kc,
		db: db,
	}
}

func (d *db2ClientBuilder) WithURL(url string) *db2ClientBuilder {
	d.url = url
	return d
}

func (d *db2ClientBuilder) WithPort(port int32) *db2ClientBuilder {
	d.port = port
	return d
}

func (d *db2ClientBuilder) WithService(svc string) *db2ClientBuilder {
	d.service = svc
	return d
}

func (d *db2ClientBuilder) WithContext(ctx context.Context) *db2ClientBuilder {
	d.ctx = ctx
	return d
}

func (d *db2ClientBuilder) GetDB2Client() (*sql.DB, error) {
	if d.ctx == nil {
		d.ctx = context.Background()
	}

	connStr, err := d.getConnectionString()
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("DB2", connStr)
	if err != nil {
		return nil, fmt.Errorf("Failed to open db2 connection: %v", err)
	}

	if err := db.PingContext(d.ctx); err != nil {
		cerr := db.Close()
		if cerr != nil {
			err = errors.Wrapf(err, "Failed to close db2 connection: %v", cerr)
		}
		return nil, fmt.Errorf("Failed to ping db2 database: %v", err)
	}

	return db, nil
}

func (d *db2ClientBuilder) getConnectionString() (string, error) {
	// Get authentication credentials
	user, pass, err := d.getdb2AuthCredentials()
	if err != nil {
		return "", fmt.Errorf("failed to get auth credentials for DB2 %s/%s: %v", d.db.Namespace, d.db.Name, err)
	}

	url := d.url
	if url == "" {
		url = PrimaryServiceDNS(d.db)
	}
	// Use the provided URL (e.g., service DNS)
	host := fmt.Sprintf("%v:%v/%v", url, d.port, d.service)

	// Construct basic connection string
	connStr := fmt.Sprintf("DB2://%s:%s@%s", user, pass, host)

	return connStr, nil
}

func (d *db2ClientBuilder) getdb2AuthCredentials() (string, string, error) {
	if d.db.Spec.AuthSecret == nil {
		return "", "", errors.New("no database secret provided")
	}

	var secret core.Secret
	err := d.kc.Get(d.ctx, client.ObjectKey{Namespace: d.db.Namespace, Name: d.db.GetAuthSecretName()}, &secret)
	if err != nil {
		return "", "", err
	}
	username := string(secret.Data[core.BasicAuthUsernameKey])
	password := string(secret.Data[core.BasicAuthPasswordKey])

	if username == "" || password == "" {
		return "", "", errors.New("username or password missing in secret")
	}
	return username, password, nil
}

// PrimaryServiceDNS make primary host dns with require template
func PrimaryServiceDNS(db *dbapi.DB2) string {
	return fmt.Sprintf("%v.%v.svc.%s", db.ServiceName(), db.Namespace, apiutils.FindDomain())
}
