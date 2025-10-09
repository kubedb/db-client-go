package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	olddbapi "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	apiutils "kubedb.dev/apimachinery/pkg/utils"

	"github.com/pkg/errors"
	_ "github.com/sijms/go-ora/v2" // Oracle driver
	core "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type OracleClientBuilder struct {
	kc      client.Client
	db      *olddbapi.Oracle
	url     string
	port    int32
	service string
	ctx     context.Context
}

func NewOracleClientBuilder(kc client.Client, db *olddbapi.Oracle) *OracleClientBuilder {
	return &OracleClientBuilder{
		kc: kc,
		db: db,
	}
}

func (o *OracleClientBuilder) WithURL(url string) *OracleClientBuilder {
	o.url = url
	return o
}

func (o *OracleClientBuilder) WithPort(port int32) *OracleClientBuilder {
	o.port = port
	return o
}

func (o *OracleClientBuilder) WithService(svc string) *OracleClientBuilder {
	o.service = svc
	return o
}

func (o *OracleClientBuilder) WithContext(ctx context.Context) *OracleClientBuilder {
	o.ctx = ctx
	return o
}

func (o *OracleClientBuilder) GetOracleClient() (*sql.DB, error) {
	if o.ctx == nil {
		o.ctx = context.Background()
	}
	// go_ora.RegisterConnConfig()
	connStr, err := o.getConnectionString()
	// Print connection string for debugging
	fmt.Printf("[DEBUG] Oracle connection string: %s\n", connStr)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("oracle", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open Oracle connection: %v", err)
	}

	if err := db.PingContext(o.ctx); err != nil {
		cerr := db.Close()
		if cerr != nil {
			err = errors.Wrapf(err, "failed to close Oracle connection: %v", cerr)
		}
		return nil, fmt.Errorf("failed to ping Oracle database: %v", err)
	}

	return db, nil
}

func (o *OracleClientBuilder) getConnectionString() (string, error) {
	// Get authentication credentials
	user, pass, err := o.getOracleAuthCredentials()
	if err != nil {
		return "", fmt.Errorf("failed to get auth credentials for Oracle %s/%s: %v", o.db.Namespace, o.db.Name, err)
	}

	url := o.url
	if url == "" {
		url = PrimaryServiceDNS(o.db)
	}
	// Use the provided URL (e.g., service DNS)
	host := fmt.Sprintf("%v:%v/%v", url, o.port, o.service)

	// Construct basic connection string
	connStr := ""
	if o.db.Spec.TCPSConfig != nil && o.db.Spec.TCPSConfig.TLS != nil {
		dbname := o.db.ObjectMeta.Name
		dstDir := fmt.Sprintf("/tmp/%s/.tls-wallet", dbname)

		if err := os.MkdirAll(dstDir, 0755); err != nil {
			fmt.Printf("[ERROR] Failed to create wallet directory: %v\n", err)
		} else {
			fmt.Printf("[DEBUG] Created wallet directory: %s\n", dstDir)
		}

		// Read the TLS secret from Kubernetes
		var tlsSecret core.Secret
		secretName := "oracle-tls-wallet"
		if err := o.kc.Get(o.ctx, client.ObjectKey{Namespace: o.db.Namespace, Name: secretName}, &tlsSecret); err != nil {
			fmt.Printf("[ERROR] Failed to get TLS secret %s: %v\n", secretName, err)
		} else {
			// Write each key to a file inside dstDir
			for key, val := range tlsSecret.Data {
				filePath := filepath.Join(dstDir, key)
				if err := os.WriteFile(filePath, val, 0644); err != nil {
					fmt.Printf("[ERROR] Failed to write %s: %v\n", filePath, err)
				} else {
					fmt.Printf("[DEBUG] Written TLS file: %s\n", filePath)
				}
			}
		}

		connStr = fmt.Sprintf("oracle://%s:%s@%s", user, pass, host)
		// Need to change this accordingly
		urlOptions := map[string]string{
			"SSL":        "true",  // or enable
			"SSL VERIFY": "false", // stop ssl certificate verification
			"WALLET":     dstDir,
		}
		connStr += "?"
		fmt.Printf("[DEBUG] Connection string now: %s\n", connStr)
		for key, val := range urlOptions {
			val = strings.TrimSpace(val)
			fmt.Printf("[DEBUG] Setting key: %s\n", key)
			fmt.Printf("[DEBUG] Setting value: %s\n", val)
			for _, temp := range strings.Split(val, ",") {
				fmt.Printf("[DEBUG] Setting temp: %s\n", temp)
				temp = strings.TrimSpace(temp)
				if strings.ToUpper(key) == "SERVER" {
					connStr += fmt.Sprintf("%s=%s&", key, temp)
				} else {
					t := []byte(temp)
					for i := 0; i < len(temp); i++ {
						if temp[i] == ' ' {
							t[i] = '+'
						}
					}
					fmt.Printf("[DEBUG] Setting t: %s\n", string(t))
					connStr += fmt.Sprintf("%s=%s&", key, string(t))
				}
			}
		}
		connStr = strings.TrimRight(connStr, "&")

		fmt.Printf("Passed from TLS\n")
	} else {
		connStr = fmt.Sprintf("oracle://%s:%s@%s", user, pass, host)
		fmt.Printf("Without from TLS\n")
	}
	return connStr, nil
}

func (o *OracleClientBuilder) getOracleAuthCredentials() (string, string, error) {
	if o.db.Spec.AuthSecret == nil {
		return "", "", errors.New("no database secret provided")
	}

	var secret core.Secret
	err := o.kc.Get(o.ctx, client.ObjectKey{Namespace: o.db.Namespace, Name: o.db.GetAuthSecretName()}, &secret)
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
func PrimaryServiceDNS(db *olddbapi.Oracle) string {
	return fmt.Sprintf("%v.%v.svc.%s", db.ServiceName(), db.Namespace, apiutils.FindDomain())
}
