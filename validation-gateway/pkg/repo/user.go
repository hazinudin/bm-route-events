package repo

import (
	"context"
	"fmt"
	"validation-gateway/infra"
	"validation-gateway/pkg/user"

	"github.com/jackc/pgx/v5"
)

type UserRepository struct {
	db          *infra.Database
	users_table string
}

// Create a new UserRepository
func NewUserRepository(users_table string, db *infra.Database) *UserRepository {
	return &UserRepository{
		db:          db,
		users_table: users_table,
	}
}

// Create a new user and insert it into database
func (r *UserRepository) CreateNewUser(user *user.User) error {
	tx, err := r.db.Pool.Begin(context.Background())

	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(context.Background())

	query := fmt.Sprintf("INSERT INTO %s (username, password_hash, created_at) VALUES ($1, $2, $3)", r.users_table)

	_, err = tx.Exec(
		context.Background(),
		query,
		user.Username,
		user.PasswordHash,
		user.CreatedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to insert user data to database: %w", err)
	}

	return nil
}

// Fetch user hashed password from database
func (r *UserRepository) FindUser(username string) (*user.User, error) {
	var user user.User
	user.Username = username

	query := fmt.Sprintf("SELECT password_hash FROM %s WHERE username = $1", r.users_table)

	err := r.db.Pool.QueryRow(
		context.Background(),
		query,
		username,
	).Scan(
		&user.PasswordHash,
	)

	if err == pgx.ErrNoRows {
		return nil, fmt.Errorf("user does not exist: %w", err)
	}

	return &user, nil
}
