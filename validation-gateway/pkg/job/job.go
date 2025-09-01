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

// Convert the ValidationJob to job response which is a struct containing Job ID and its created at timestamp.
func (job *ValidationJob) AsJobResponse() any {
	out := struct {
		JobID     string `json:"job_id"`
		CreatedAt int64  `json:"created_at"`
	}{
		JobID:     job.JobID,
		CreatedAt: job.CreatedAt,
	}

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
