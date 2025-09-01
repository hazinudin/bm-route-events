package job

import (
	"encoding/json"
	"time"
)

type ValidationJob struct {
	JobID     string `json:"job_id"`
	DataType  string `json:"data_type"`
	CreatedAt int64  `json:"created_at"`
	Details   []byte `json:"details"`
}

func NewValidationJob(job_id string, data_type string, details any) (*ValidationJob, error) {
	details_string, err := json.Marshal(details)

	if err != nil {
		return nil, err
	}

	return &ValidationJob{
		JobID:     job_id,
		CreatedAt: time.Now().Unix(),
		Details:   details_string,
		DataType:  data_type,
	}, nil
}

func (job *ValidationJob) AsJobResponse() map[string]any {
	out := make(map[string]any)

	out["job_id"] = job.JobID
	out["submitted_at"] = job.SubmittedAt
	out["status"] = "Queued"

	return out
}

func (job *ValidationJob) AsJobMessage() ([]byte, error) {
	msg := make(map[string]any)

	msg["job_id"] = job.JobID
	msg["data_type"] = job.DataType
	msg["details"] = job.Details

	msg_string, err := json.Marshal(msg)

	if err != nil {
		return nil, err
	}

	return msg_string, nil
}
