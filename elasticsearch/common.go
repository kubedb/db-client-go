package elasticsearch

var (
	writeRequestIndex = "kubedb-system"
	writeRequestID    = "info"
	writeRequestType  = "_doc"
)

type WriteRequestIndex struct {
	Index WriteRequestIndexBody `json:"index"`
}

type WriteRequestIndexBody struct {
	ID   string `json:"_id"`
	Type string `json:"_type,omitempty"`
}
