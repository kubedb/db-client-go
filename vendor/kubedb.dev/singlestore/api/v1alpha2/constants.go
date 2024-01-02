package v1alpha2

const (
	ComponentDatabase = "database"
)
const (
	DatabasePaused = "Paused"
)

// Database related

const (
	SinglestoreDatabasePortName       = "db"
	SinglestorePrimaryServicePortName = "primary"
	SinglestoreDatabasePort           = 3306
	SinglestoreRootUserName           = "ROOT_USERNAME"
	SinglestoreRootPassword           = "ROOT_PASSWORD"
	SinglestoreRootUser               = "root"
	SinglestoreRunAsUser              = 999

	DatabasePodMaster     = "Master"
	DatabasePodAggregator = "Aggregator"
	DatabasePodLeaf       = "Leaf"

	StatefulSetTypeMasterAggregator = "master-aggregator"
	StatefulSetTypeLeaf             = "leaf"

	SinglestoreCoordinatorContainerName = "singlestore-coordinator"
	SinglestoreContainerName            = "singlestore"
	SinglestoreInitContainerName        = "singlestore-init"

	KubeDBSinglestoreClusterRole        = "kubedb:singlestoreversion-reader"
	KubeDBSinglestoreClusterRoleBinding = "kubedb:singlestoreversion-reader"

	// singlestore volume and volume Mounts

	SinglestoreVolumeNameUserInitScript      = "initial-script"
	SinglestoreVolumeMountPathUserInitScript = "/docker-entrypoint-initdb.d"

	SinglestoreVolumeNameCustomConfig      = "custom-config"
	SinglestoreVolumeMountPathCustomConfig = "/config"

	SinglestoreVolmeNameInitScript       = "init-scripts"
	SinglestoreVolumeMountPathInitScript = "/scripts"

	SinglestoreVolumeNameData      = "data"
	SinglestoreVolumeMountPathData = "/var/lib/memsql"
)
