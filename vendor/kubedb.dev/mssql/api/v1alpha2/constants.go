package v1alpha2

const (
	// DatabasePaused used for Databases that are paused
	DatabasePaused = "Paused"
)

const (
	// =========================== MsSQL Constants ============================

	MsSQLDatabasePodPrimary    = "primary"
	MsSQLDatabasePodSecondary  = "secondary"
	MsSQLSecondaryServiceAlias = "secondary"

	MsSQLDatabasePortName         = "db"
	MsSQLPrimaryServicePortName   = "primary"
	MsSQLSecondaryServicePortName = "secondary"

	MsSQLDatabasePort                  = 1433
	MsSQLDatabaseMirroringEndpointPort = 5022

	MsSQLUser          = "mssql"
	MsSQLUsernameKey   = "username"
	MsSQLSAUser        = "sa"
	MsSQLSAPasswordKey = "password"

	// --- Environment Variables
	EnvAcceptEula        = "ACCEPT_EULA"
	EnvMsSQLEnableHADR   = "MSSQL_ENABLE_HADR"
	EnvMsSQLAgentEnabled = "MSSQL_AGENT_ENABLED"
	EnvMsSQLSAUsername   = "MSSQL_SA_USERNAME"
	EnvMsSQLSAPassword   = "MSSQL_SA_PASSWORD"

	// --- Container related
	MsSQLContainerName            = "mssql"
	MsSQLCoordinatorContainerName = "mssql-coordinator"
	MsSQLInitContainerName        = "mssql-init"

	MsSQLVolmeNameInitScript       = "init-scripts"
	MsSQLVolumeMountPathInitScript = "/scripts"

	MsSQLImage            = "neajmorshad/sql22:tools-0.1"
	MsSQLCoordinatorImage = "neajmorshad/mssql-coordinator:mssql-coordinator_linux_amd64"
	MsSQLInitImage        = "neajmorshad/mssql-init-docker:0.1"

	ComponentDatabase = "database"

	// Volume related
	MsSQLVolumeNameData      = "data"
	MsSQLVolumeMountPathData = "/var/opt/mssql"
	// mssql volume and volume Mounts
	MsSQLVolumeNameTemp      = "tmp"
	MsSQLVolumeMountPathTemp = "/tmp"
)
