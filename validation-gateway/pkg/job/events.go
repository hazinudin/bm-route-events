package job

import (
	"encoding/json"

	"github.com/apache/arrow/go/v16/arrow"
)

type JobEventType string

const (
	JOB_CREATED   JobEventType = "created"
	JOB_SUBMITTED JobEventType = "submitted"
	JOB_SUCCEEDED JobEventType = "succeeded"
)

type EventEnvelope struct {
	Type    JobEventType    `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func serialize(event_type JobEventType, payload interface{}) ([]byte, error) {
	bytes, err := json.Marshal(payload)

	if err != nil {
		return nil, err
	}

	envelope := EventEnvelope{
		Type:    event_type,
		Payload: bytes,
	}

	return json.Marshal(envelope)
}

type JobEventInterface interface {
	GetEventType() JobEventType
	SerializeToEnvelope() ([]byte, error)
	GetJobID() string
	GetOccurredAt() int64
}

type JobEvent struct {
	JobID     string `json:"job_id"`
	OccuredAt int64  `json:"occured_at"`
}

// Actual Job Events
// Job Created event
type JobCreated struct {
	// Triggered when a job is created and already inserted to database.
	JobEvent
	Job *ValidationJob `json:"job"`
}

func (e *JobCreated) SerializeToEnvelope() ([]byte, error) {
	bytes, err := serialize(JOB_CREATED, e)

	if err != nil {
		return nil, err
	}

	return bytes, nil
}

func (e *JobCreated) GetEventType() JobEventType {
	return JOB_CREATED
}

func (e *JobCreated) GetJobID() string {
	return e.JobID
}

func (e *JobCreated) GetOccurredAt() int64 {
	return e.OccuredAt
}

// Job Submitted event
type JobSubmitted struct {
	// Triggered when a job is already submitted to validation job queue to be processed.
	JobEvent
}

func (e *JobSubmitted) SerializeToEnvelope() ([]byte, error) {
	bytes, err := serialize(JOB_SUBMITTED, e)

	if err != nil {
		return nil, err
	}

	return bytes, nil
}

func (e *JobSubmitted) GetEventType() JobEventType {
	return JOB_SUBMITTED
}

func (e *JobSubmitted) GetJobID() string {
	return e.JobID
}

func (e *JobSubmitted) GetOccurredAt() int64 {
	return e.OccuredAt
}

// Job Succeded event
type JobSucceded struct {
	// Triggered when a job is processed succesfully by the workers.
	JobEvent
	Results *arrow.Record `json:"-"` // The Arrow Record will not be included in the event store
}

func (e *JobSucceded) SerializeToEnvelope() ([]byte, error) {
	bytes, err := serialize(JOB_SUCCEEDED, e)

	if err != nil {
		return nil, err
	}

	return bytes, nil
}

func (e *JobSucceded) GetEventType() JobEventType {
	return JOB_SUBMITTED
}

func (e *JobSucceded) GetJobID() string {
	return e.JobID
}

func (e *JobSucceded) GetOccurredAt() int64 {
	return e.OccuredAt
}
