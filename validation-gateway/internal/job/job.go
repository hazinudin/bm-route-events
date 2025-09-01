package job

import (
	"strings"
	"time"
	"validation-gateway/pkg/job"

	"github.com/google/uuid"
)

type SMDPayload struct {
	Filename       string    `json:"file_name" validate:"required"`
	Balai          string    `json:"balai,omitempty" validate:"required"`
	Year           int       `json:"year,omitempty" validate:"required"`
	Semester       int       `json:"semester,omitempty" validate:"required"`
	Routes         [1]string `json:"routes" validate:"required"`
	ShowAllMessage *bool     `json:"show_all_msg" validate:"required"`
}

type INVIJPayload map[string]any

type JobRequest[T INVIJPayload | SMDPayload] struct {
	InputJSON T      `json:"input_json"`
	DataType  string `json:"data_type"`
}

// Get job latest status
func (s *JobService) GetJobStatus(job_id string, data_type string) (map[string]any, error) {
	status, err := s.repo.GetJobStatus(job_id, data_type)
	out := make(map[string]any)

	if err != nil {
		return nil, err
	}

	out["job_id"] = job_id
	out["status"] = status

	return out, nil
}

// Get job result
func (s *JobService) GetJobResult(job_id string, data_type string) (*job.ValidationJobResult, error) {
	status, err := s.repo.GetJobResult(job_id, data_type)

	if err != nil {
		return nil, err
	}

	return status, nil
}

// CreateValidationJob will return a ValidationJob struct
// This function will also store the new ValidationJob to database
func (s *JobService) CreateValidationJob(data_type string, details any) (*job.ValidationJob, error) {
	job_id := data_type + "_" + uuid.New().String()

	job_, err := job.NewValidationJob(job_id, data_type, details)

	if err != nil {
		return nil, err
	}

	event := job.JobCreated{
		JobEvent: job.JobEvent{
			JobID:     job_.JobID,
			OccuredAt: time.Now().UnixMilli(),
		},
		Job: job_,
	}

	err = s.repo.InsertJob(job_)

	if err != nil {
		return nil, err
	}

	err = s.dispatcher.PublishEvent(&event)

	if err != nil {
		return nil, err
	}

	return job_, nil
}

// PublishSMDValidationJob accepts a validation request, create a new ValidationJob and publish it to message broker
func (s *JobService) PublishSMDValidationJob(request *JobRequest[SMDPayload], data_type string) (any, error) {
	job, err := s.CreateValidationJob(strings.ToUpper(data_type), request.InputJSON)

	if err != nil {
		return nil, err
	}

	return job.AsJobResponse(), nil
}

// PublishSMDValidationJob accepts a validation request, create a new ValidationJob and publish it to message broker
func (s *JobService) PublishINVIJValidationJob(request *JobRequest[INVIJPayload], data_type string) (any, error) {
	job, err := s.CreateValidationJob(strings.ToUpper(data_type), request.InputJSON)

	if err != nil {
		return nil, err
	}

	return job.AsJobResponse(), nil
}
