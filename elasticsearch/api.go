package elasticsearch

var _ ESClient = &ESClientV6{}
var _ ESClient = &ESClientV7{}

type ESClient interface {
	ClusterHealthInfo() (map[string]interface{}, error)
	NodesStats() (map[string]interface{}, error)
}
