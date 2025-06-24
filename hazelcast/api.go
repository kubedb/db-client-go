package hazelcast

type HZClient interface {
	ChangeClusterState(password, state string) (string, error)
}
