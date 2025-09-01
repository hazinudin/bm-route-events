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
	result_table      string
	result_msg_table  string
	event_store_table string
}

func NewValidationJobRepository(db *infra.Database) *ValidationJobRepository {
	return &ValidationJobRepository{
		db:                db,
		job_table:         "validation_jobs",
		result_table:      "validation_job_results",
		result_msg_table:  "validation_job_results_msg",
		event_store_table: "validation_jobs_event_store",
	}
}

// Insert a new ValidationJob into database
func (r *ValidationJobRepository) InsertJob(job *job.ValidationJob) error {
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
		job.CreatedAt,
		job.Details,
	)

	if err != nil {
		return fmt.Errorf("failed to insert job data to database: %w", err)
	}

	tx.Commit(ctx)

	return nil
}

// Get Job latest status.
// Query direct from event store table to determine the latest status.
func (r *ValidationJobRepository) GetJobStatus(job_id string, data_type string) (*string, error) {
	var status string

	job_query := fmt.Sprintf("SELECT '%s' as status from %s where job_id = $1 AND data_type = $2", job.JOB_CREATED, r.job_table)

	err := r.db.Pool.QueryRow(
		context.Background(),
		job_query,
		job_id,
		data_type,
	).Scan(
		&status,
	)

	if err != nil {
		return nil, err
	}

	status_query := fmt.Sprintf("SELECT event_name from %s WHERE job_id = $1 ORDER BY occurred_at desc LIMIT 1", r.event_store_table)

	err = r.db.Pool.QueryRow(
		context.Background(),
		status_query,
		job_id,
	).Scan(
		&status,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			return &status, nil
		}

		return nil, err
	}

	return &status, nil
}

// Get job result
func (r *ValidationJobRepository) GetJobResult(job_id string, data_type string) (*job.ValidationJobResult, error) {
	var out job.ValidationJobResult

	query := fmt.Sprintf("SELECT job_id, status, message_count, all_msg_status, ignorables from %s WHERE job_id = $1 AND split_part(job_id, '_', 1) = $2", r.result_table)

	err := r.db.Pool.QueryRow(
		context.Background(),
		query,
		job_id,
		data_type,
	).Scan(
		&out.JobID,
		&out.Status,
		&out.MessageCount,
		&out.AllMessageStatus,
		&out.Ignorables,
	)

	if err != nil {
		return nil, err
	}

	return &out, nil
}

// Append event to Validation Job event store table
// Serialize the event and insert it to the database
func (r *ValidationJobRepository) AppendEvents(event job.JobEventInterface) error {
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

// Insert job result messages to database.
func (r *ValidationJobRepository) InsertJobResultMessages(rows [][]any) error {
	ctx := context.Background()

	_, err := r.db.Pool.CopyFrom(
		ctx,
		pgx.Identifier{r.result_msg_table},
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

// Insert job result
func (r *ValidationJobRepository) InsertJobResult(result *job.ValidationJobResult) error {
	ctx := context.Background()

	tx, err := r.db.Pool.Begin(ctx)

	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	query := fmt.Sprintf("INSERT INTO %s (job_id, status, message_count, all_msg_status, ignorables) VALUES ($1, $2, $3, $4, $5)", r.result_table)

	_, err = tx.Exec(
		ctx,
		query,
		result.JobID,
		result.Status,
		result.MessageCount,
		result.AllMessageStatus,
		result.Ignorables,
	)

	if err != nil {
		return fmt.Errorf("failed to insert job data to database: %w", err)
	}

	tx.Commit(ctx)

	return nil
}
