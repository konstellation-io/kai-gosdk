package prediction

type Prediction struct {
	CreationDate int64    `json:"creation_date"`
	LastModified int64    `json:"last_modified"`
	Payload      Payload  `json:"payload"`
	Metadata     Metadata `json:"metadata"`
}

type Payload map[string]any

type Metadata struct {
	Product      string `json:"product"`
	Version      string `json:"version"`
	Workflow     string `json:"workflow"`
	WorkflowType string `json:"workflow_type"`
	Process      string `json:"process"`
	RequestID    string `json:"request_id"`
}
