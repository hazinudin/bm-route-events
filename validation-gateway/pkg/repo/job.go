package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
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
	outbox_table      string
}

func NewValidationJobRepository(db *infra.Database) *ValidationJobRepository {
	return &ValidationJobRepository{
		db:                db,
		job_table:         "validation_jobs",
		result_table:      "validation_job_results",
		result_msg_table:  "validation_job_results_msg",
		event_store_table: "validation_jobs_event_store",
		outbox_table:      "validation_job_outbox",
	}
}

// Get ValidationJob from database
func (r *ValidationJobRepository) GetValidationJob(job_id string) (*job.ValidationJob, error) {
	ctx := context.Background()
	var job_ job.ValidationJob
	query := fmt.Sprintf("SELECT job_id, data_type, submitted_at, payload FROM %s WHERE job_id = $1;", r.job_table)

	err := r.db.Pool.QueryRow(
		ctx,
		query,
		job_id,
	).Scan(
		&job_.JobID,
		&job_.DataType,
		&job_.CreatedAt,
		&job_.Details,
	)

	if err != nil {
		return nil, err
	}

	return &job_, nil
}

// Insert a new ValidationJob into database
func (r *ValidationJobRepository) InsertJob(job_ *job.ValidationJob) error {
	ctx := context.Background()

	// Create JobCreated event for outbox table
	event := job.JobCreated{
		JobEvent: job.JobEvent{
			JobID:     job_.JobID.String(),
			OccuredAt: time.Now().UnixMilli(),
		},
		Job: job_,
	}

	event_json, err := json.Marshal(event)

	if err != nil {
		return fmt.Errorf("failed to serialize event: %w", err)
	}

	tx, err := r.db.Pool.Begin(ctx)

	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	job_insert_query := fmt.Sprintf("INSERT INTO %s (job_id, data_type, submitted_at, payload) VALUES ($1, $2, $3, $4);", r.job_table)

	_, err = tx.Exec(
		ctx,
		job_insert_query,
		job_.JobID,
		job_.DataType,
		job_.CreatedAt,
		job_.Details,
	)

	if err != nil {
		return fmt.Errorf("failed to insert job data to database: %w", err)
	}

	outbox_query := fmt.Sprintf("INSERT INTO %s (job_id, event_name, payload) VALUES ($1, $2, $3);", r.outbox_table)

	_, err = tx.Exec(
		ctx,
		outbox_query,
		event.GetJobID(),
		event.GetEventType(),
		event_json,
	)

	if err != nil {
		return fmt.Errorf("failed to insert events to outbox: %w", err)
	}

	tx.Commit(ctx)

	return nil
}

// Get Job latest status.
// Query direct from event store table to determine the latest status.
func (r *ValidationJobRepository) GetJobStatus(job_id string) (*string, error) {
	var status string

	job_query := fmt.Sprintf("SELECT '%s' as status from %s where job_id = $1", job.JOB_CREATED, r.job_table)

	err := r.db.Pool.QueryRow(
		context.Background(),
		job_query,
		job_id,
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
func (r *ValidationJobRepository) GetJobResult(job_id string, attempt_id int, tx pgx.Tx) (*job.ValidationJobResult, error) {
	var out job.ValidationJobResult
	var ignored_tags []job.MessageTag

	query := fmt.Sprintf(
		"SELECT job_id, status, message_count, all_msg_status, ignorables, ignored_tags, attempt_id from %s WHERE job_id = $1 AND attempt_id = $2 FOR UPDATE",
		r.result_table,
	)

	err := tx.QueryRow(
		context.Background(),
		query,
		job_id,
		attempt_id,
	).Scan(
		&out.JobID,
		&out.Status,
		&out.MessageCount,
		&out.AllMessageStatus,
		&out.Ignorables,
		&ignored_tags,
		&out.AttemptID,
	)

	if err != nil {
		return nil, err
	}

	new := job.NewJobResult(
		out.JobID,
		out.Status,
		out.MessageCount,
		out.AllMessageStatus,
		out.Ignorables,
		ignored_tags,
		out.AttemptID,
	)

	return new, nil
}

// Get job attempt numbers
func (r *ValidationJobRepository) GetJobAttemptNumber(job_id string) (int, error) {
	var attempt_count int

	query := fmt.Sprintf("SELECT count(*) FROM %s WHERE job_id = $1 and event_name = $2", r.event_store_table)

	err := r.db.Pool.QueryRow(
		context.Background(),
		query,
		job_id,
		job.JOB_SUBMITTED,
	).Scan(
		&attempt_count,
	)

	if err != nil {
		return -1, err
	}

	return attempt_count, err
}

// Update job result status and ignored tags
func (r *ValidationJobRepository) UpdateJobResult(result *job.ValidationJobResult, tx pgx.Tx) error {
	ctx := context.Background()

	query := fmt.Sprintf("UPDATE %s SET status = $1, ignorables = $2, ignored_tags = $3 WHERE job_id = $4", r.result_table)

	_, err := tx.Exec(
		ctx,
		query,
		result.Status,
		result.Ignorables,
		result.GetIgnoredTags(),
		result.JobID,
	)

	if err != nil {
		return fmt.Errorf("failed to update result for job %s: %w", result.JobID, err)
	}

	outbox_query := fmt.Sprintf("INSERT INTO %s (job_id, event_name, payload) VALUES ($1, $2, $3);", r.outbox_table)

	// Iterate through all events that might happen in the results.
	for _, event := range result.GetAllEvents() {
		event_json, err := json.Marshal(event)

		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}

		_, err = tx.Exec(
			ctx,
			outbox_query,
			event.GetJobID(),
			event.GetEventType(),
			event_json,
		)

		if err != nil {
			return fmt.Errorf("failed to insert events to outbox: %w", err)
		}
	}

	tx.Commit(ctx)

	return nil
}

func (r *ValidationJobRepository) GetJobResultMessages(job_id string, attempt_id int) ([]job.ValidationJobResultMessage, error) {
	var messages []job.ValidationJobResultMessage

	query := fmt.Sprintf("SELECT msg, msg_status, content_id, ignore_in FROM %s WHERE job_id = $1 and attempt_id = $2", r.result_msg_table)

	rows, err := r.db.Pool.Query(context.Background(), query, job_id, attempt_id)

	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var msg_row job.ValidationJobResultMessage
		err := rows.Scan(
			&msg_row.Message,
			&msg_row.MessageStatus,
			&msg_row.ContentID,
			&msg_row.IgnoreIn,
		)

		if err != nil {
			return nil, err
		}

		messages = append(messages, msg_row)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return messages, nil
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
		[]string{"job_id", "attempt_id", "msg", "msg_status", "msg_status_idx", "ignore_in", "content_id"},
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

	query := fmt.Sprintf("INSERT INTO %s (job_id, status, message_count, all_msg_status, ignorables, ignored_tags, attempt_id) VALUES ($1, $2, $3, $4, $5, $6, $7)", r.result_table)

	_, err = tx.Exec(
		ctx,
		query,
		result.JobID,
		result.Status,
		result.MessageCount,
		result.AllMessageStatus,
		result.Ignorables,
		result.GetIgnoredTags(),
		result.AttemptID,
	)

	if err != nil {
		return fmt.Errorf("failed to insert job data to database: %w", err)
	}

	tx.Commit(ctx)

	return nil
}

func (r *ValidationJobRepository) FindSMDJobID(file_name string, route_id string) ([]map[string]any, error) {
	var jobs []map[string]any

	cte_query := fmt.Sprintf("WITH jobs as (select job_id, data_type, submitted_at, payload from %s where (payload ->> 'file_name') = $1 and (payload -> 'routes' ->> 0) = $2) ", r.job_table)
	id_query := "SELECT job_id, submitted_at payload from jobs order by submitted_at desc"

	query := cte_query + id_query

	rows, err := r.db.Pool.Query(context.Background(), query, file_name, route_id)

	if err != nil {
		return nil, err
	}

	for rows.Next() {
		job := make(map[string]any)
		var job_id string
		var submitted_at int

		err := rows.Scan(
			&job_id,
			&submitted_at,
		)

		job["job_id"] = &job_id
		job["submitted_at"] = &submitted_at

		if err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return jobs, nil
}

func (r *ValidationJobRepository) BeginTransaction() (pgx.Tx, error) {
	tx, err := r.db.Pool.Begin(context.Background())

	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	return tx, nil
}
