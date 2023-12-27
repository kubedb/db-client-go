package v1alpha2

// Environment variables
const (
	EnvPostgresUsername               = "POSTGRES_USERNAME"
	EnvPostgresPassword               = "POSTGRES_PASSWORD"
	EnvPgpoolPcpUser                  = "PGPOOL_PCP_USER"
	EnvPgpoolPcpPassword              = "PGPOOL_PCP_PASSWORD"
	EnvPgpoolPasswordEncryptionMethod = "PGPOOL_PASSWORD_ENCRYPTION_METHOD"
	EnvEnablePoolPasswd               = "PGPOOL_ENABLE_POOL_PASSWD"
	EnvSkipPasswdEncryption           = "PGPOOL_SKIP_PASSWORD_ENCRYPTION"
)

const DatabasePaused = "Paused"

// For pgpool statefulSet
const (
	ConfigSecretMountPath = "/config"
	ConfigVolumeName      = "pgpool-config"
	ContainerName         = "pgpool"
)

// Pgpool secret
const (
	PgpoolAuthUsername    = "pcp"
	DefaultPasswordLength = 16
)

// Sync users
const (
	SyncPeriod = 10
)
