package internal

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	DBHost      string
	DBPort      string
	DBName      string
	DBUsername  string
	DBPassword  string
	TokenSecret string
	RMQHost     string
	RMQPort     string
}

func LoadConfig() *Config {
	err := godotenv.Load(".env")

	if err != nil {
		log.Fatalf("failed to load .env file: %v", err)
	}

	return &Config{
		DBHost:      os.Getenv("DB_HOST"),
		DBPort:      os.Getenv("DB_PORT"),
		DBName:      os.Getenv("DB_NAME"),
		DBUsername:  os.Getenv("DB_USERNAME"),
		DBPassword:  os.Getenv("DB_PASSWORD"),
		TokenSecret: os.Getenv("TOKEN_SECRET_KEY"),
		RMQHost:     os.Getenv("RMQ_HOST"),
		RMQPort:     os.Getenv("RMQ_PORT"),
	}
}
