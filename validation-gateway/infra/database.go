package infra

import (
	"context"
	"fmt"
	"log"
	"time"

	"validation-gateway/internal"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Database struct {
	Pool *pgxpool.Pool
}

func NewDatabase(conf *internal.Config) (*Database, error) {
	var (
		host     = conf.DBHost
		user     = conf.DBUsername
		password = conf.DBPassword
		dbPort   = conf.DBPort
		dbName   = conf.DBName
	)

	config_str := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable", host, user, password, dbName, dbPort)
	config, err := pgxpool.ParseConfig(config_str)
	ctx := context.Background()

	if err != nil {
		return nil, fmt.Errorf("failed to parse database config : %w", err)
	}

	config.MaxConns = 25
	config.MinConns = 5
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = time.Minute * 30

	pool, err := pgxpool.NewWithConfig(ctx, config)

	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database : %w", err)
	}

	log.Printf("Connected to Postgres database at %s:%s", host, dbPort)

	return &Database{Pool: pool}, nil
}
