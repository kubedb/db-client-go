package elasticsearch

type ESClient interface {
	ClusterHealthInfo() (map[string]interface{}, error)
	NodesStats() (map[string]interface{}, error)
}
