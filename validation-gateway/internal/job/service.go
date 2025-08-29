package job

import (
	"fmt"
	"validation-gateway/infra"
	"validation-gateway/internal"
	"validation-gateway/pkg/repo"
)

type JobService struct {
	q          *JobQueue
	db         *infra.Database
	repo       *repo.ValidationJobRepository
	dispatcher *JobEventDispatcher
}

// NewJobService will return a JobService which already connected to database and message queue
func NewJobService(db *infra.Database, conf *internal.Config) *JobService {
	// Connect to message broker (RabbitMQ)
	rmq_url := fmt.Sprintf("amqp://%s:%s", conf.RMQHost, conf.RMQPort)
	q := NewJobQueueClient(rmq_url)

	// Create repo
	repo := repo.NewValidationJobRepository(db)

	// Create event dispatcher
	dispatcher := NewJobEventDispatcher(rmq_url)

	return &JobService{
		q:          q,
		db:         db,
		repo:       repo,
		dispatcher: dispatcher,
	}
}
