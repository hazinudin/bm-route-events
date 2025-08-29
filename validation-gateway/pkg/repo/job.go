package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"validation-gateway/infra"
	"validation-gateway/pkg/job"

	"github.com/jackc/pgx/v5"
)

type ValidationJobRepository struct {
	db                *infra.Database
	job_table         string
	event_store_table string
}

func NewValidationJobRepository(db *infra.Database) *ValidationJobRepository {
	return &ValidationJobRepository{
		db:                db,
		job_table:         "validation_jobs",
		event_store_table: "validation_jobs_event_store",
	}
}

func (r *ValidationJobRepository) InsertJob(job *job.ValidationJob) error {
	// Insert a new ValidationJob into database
	ctx := context.Background()

	tx, err := r.db.Pool.Begin(ctx)

	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	query := fmt.Sprintf("INSERT INTO %s (job_id, data_type, submitted_at, payload) VALUES ($1, $2, $3, $4)", r.job_table)

	_, err = tx.Exec(
		ctx,
		query,
		job.JobID,
		job.DataType,
		job.SubmittedAt,
		job.Details,
	)

	if err != nil {
		return fmt.Errorf("failed to insert job data to database: %w", err)
	}

	tx.Commit(ctx)

	return nil
}

func (r *ValidationJobRepository) GetJobStatus(job *job.ValidationJob) error {
	// Get job status
	return nil
}

func (r *ValidationJobRepository) AppendEvents(event job.JobEventInterface) error {
	// Append event to Validation Job event store table
	// Serialize the event and insert it to the database

	ser, err := json.Marshal(event)
	json_ser := json.RawMessage(ser)

	if err != nil {
		return err
	}

	ctx := context.Background()

	tx, err := r.db.Pool.Begin(ctx)

	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	query := fmt.Sprintf("INSERT INTO %s (job_id, event_name, occurred_at, event) VALUES ($1, $2, $3, $4)", r.event_store_table)

	_, err = tx.Exec(
		ctx,
		query,
		event.GetJobID(),
		event.GetEventType(),
		event.GetOccurredAt(),
		json_ser,
	)

	if err != nil {
		return fmt.Errorf("failed to insert job data to database: %w", err)
	}

	tx.Commit(ctx)

	return nil
}

func (r *ValidationJobRepository) InsertJobResults(rows [][]any) error {
	ctx := context.Background()

	_, err := r.db.Pool.CopyFrom(
		ctx,
		pgx.Identifier{"validation_job_results"},
		[]string{"job_id", "msg", "msg_status", "msg_status_idx", "ignore_in", "content_id"},
		pgx.CopyFromSlice(len(rows), func(i int) ([]any, error) {
			return rows[i], nil
		}),
	)

	if err != nil {
		return err
	}

	return nil
}
