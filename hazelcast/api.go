package hazelcast

type HZClient interface {
	ChangeClusterState(state string) (string, error)
	GetClusterState() (string, error)
}
