package job

import (
	"context"
	"fmt"
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

// Get ValidationJob
func (s *JobService) GetValidationJob(job_id string) (*job.ValidationJob, error) {
	job, err := s.repo.GetValidationJob(job_id)

	if err != nil {
		return nil, err
	}

	return job, nil
}

// Get job latest status
func (s *JobService) GetJobStatus(job_id string) (map[string]any, error) {
	status, err := s.repo.GetJobStatus(job_id)
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

// Fetch SMD validation job's Job ID from its submitted file name and route
func (s *JobService) GetSMDJobID(file_name string, route_id string) ([]map[string]any, error) {
	job_ids, err := s.repo.FindSMDJobID(file_name, route_id)

	if err != nil {
		return nil, err
	}

	return job_ids, nil
}

// CreateValidationJob will return a ValidationJob struct
// This function will also store the new ValidationJob to database
func (s *JobService) CreateValidationJob(data_type string, details any, ctx context.Context) (*job.ValidationJob, error) {
	job_id, err := uuid.NewV7()

	if err != nil {
		return nil, err
	}

	job_, err := job.NewValidationJob(job_id, data_type, details)

	if err != nil {
		return nil, err
	}

	err = s.repo.InsertJob(job_, ctx)

	if err != nil {
		return nil, err
	}

	return job_, nil
}

// PublishSMDValidationJob accepts a validation request, create a new ValidationJob and publish it to message broker
func (s *JobService) PublishSMDValidationJob(request *JobRequest[SMDPayload], data_type string, ctx context.Context) (any, error) {
	job, err := s.CreateValidationJob(strings.ToUpper(data_type), request.InputJSON, ctx)

	if err != nil {
		return nil, err
	}

	return job.AsJobResponse(), nil
}

// PublishSMDValidationJob accepts a validation request, create a new ValidationJob and publish it to message broker
func (s *JobService) PublishINVIJValidationJob(request *JobRequest[INVIJPayload], data_type string, ctx context.Context) (any, error) {
	job, err := s.CreateValidationJob(strings.ToUpper(data_type), request.InputJSON, ctx)

	if err != nil {
		return nil, err
	}

	return job.AsJobResponse(), nil
}

func (s *JobService) RetryJob(job_id string, ctx context.Context) error {
	current_status, err := s.GetJobStatus(job_id)

	if err != nil {
		return fmt.Errorf("failed to fetch job status: %w", err)
	}

	if (*current_status["status"].(*string) != string(job.JOB_FAILED)) && (*current_status["status"].(*string) != string(job.JOB_SUCCEEDED)) {
		return fmt.Errorf("cannot retry job when job status is %s", current_status["status"])
	}

	event := job.JobRetried{
		JobEvent: job.JobEvent{
			JobID:     job_id,
			OccuredAt: time.Now().UnixMilli(),
		},
	}

	err = s.dispatcher.PublishEvent(&event, ctx)

	if err != nil {
		return fmt.Errorf("failed to publish retried event: %w", err)
	}

	return nil
}
