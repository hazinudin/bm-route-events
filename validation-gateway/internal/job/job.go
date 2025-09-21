package job

import (
	"context"
	"strings"
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
func (s *JobService) GetLatestJobResult(job_id string) (*job.ValidationJobResult, error) {
	attempt_id, err := s.repo.GetJobAttemptNumber(job_id)

	if err != nil {
		return nil, err
	}

	tx, err := s.repo.BeginTransaction()
	defer tx.Rollback(context.Background())

	if err != nil {
		return nil, err
	}

	status, err := s.repo.GetJobResult(job_id, attempt_id, tx)

	if err != nil {
		return nil, err
	}

	return status, nil
}

// Get job messsages
func (s *JobService) GetLatestJobResultMessages(job_id string) ([]job.ValidationJobResultMessage, error) {
	attempt_id, err := s.repo.GetJobAttemptNumber(job_id)

	if err != nil {
		return nil, err
	}

	messages, err := s.repo.GetJobResultMessages(job_id, attempt_id)

	if err != nil {
		return nil, err
	}

	return messages, nil
}

// Ignore disputed messages
func (s *JobService) AcceptDisputedMessages(job_id string) error {
	attempt_id, err := s.repo.GetJobAttemptNumber(job_id)

	if err != nil {
		return err
	}

	tx, err := s.repo.BeginTransaction()

	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())

	result, err := s.repo.GetJobResult(job_id, attempt_id, tx)

	if err != nil {
		return err
	}

	err = result.IgnoreDisputed()

	if err != nil {
		return err
	}

	err = s.repo.UpdateJobResult(result, tx)

	if err != nil {
		return err
	}

	return nil
}

// Ignore reviewed messages
func (s *JobService) AcceptReviewedMessages(job_id string) error {
	attempt_id, err := s.repo.GetJobAttemptNumber(job_id)

	if err != nil {
		return err
	}

	tx, err := s.repo.BeginTransaction()

	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())

	result, err := s.repo.GetJobResult(job_id, attempt_id, tx)

	if err != nil {
		return err
	}

	err = result.IgnoreReviewed()

	if err != nil {
		return err
	}

	err = s.repo.UpdateJobResult(result, tx)

	if err != nil {
		return err
	}

	return nil
}

// CreateValidationJob will return a ValidationJob struct
// This function will also store the new ValidationJob to database
func (s *JobService) CreateValidationJob(data_type string, details any) (*job.ValidationJob, error) {
	job_id := data_type + "_" + uuid.New().String()

	job_, err := job.NewValidationJob(job_id, data_type, details)

	if err != nil {
		return nil, err
	}

	err = s.repo.InsertJob(job_)

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
