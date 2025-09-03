package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"validation-gateway/internal"
	"validation-gateway/internal/job"

	"github.com/go-playground/validator/v10"
	"github.com/jackc/pgx/v5"
)

type ErrorResponse struct {
	Status  string
	Details map[string]string
}

type LoginRequest struct {
	Username string `json:"username" validate:"required"`
	Password string `json:"password,omitempty" validate:"required"`
}

type LoginResponse struct {
	Token      string `json:"token"`
	Expiration int64  `json:"expiration"`
}

func validateRequest[T any](r *http.Request) (*T, *ErrorResponse) {
	var input T

	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		return nil, &ErrorResponse{Status: "Invalid JSON"}
	}

	validate := validator.New()
	err := validate.Struct(input)

	if err != nil {
		validation_errors := make(map[string]string)

		for _, err := range err.(validator.ValidationErrors) {
			validation_errors[err.Field()] = fmt.Sprintf("Field '%s' failed validation: %s", err.Field(), err.Tag())
		}

		resp := &ErrorResponse{Status: "Invalid JSON Format", Details: validation_errors}

		return nil, resp
	}

	return &input, nil
}

func (s *Server) LoginHandler(w http.ResponseWriter, r *http.Request) {
	input, error_resp := validateRequest[internal.LoginRequest](r)

	if error_resp != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(error_resp)
		return
	}

	match_hash := s.user_service.ComparePasswordHash(input.Username, input.Password)

	if !match_hash {
		http.Error(w, "Users not found or invalid password.", http.StatusUnauthorized)
		return
	}

	login_resp, err := internal.GenerateToken(input, []byte(s.conf.TokenSecret))

	if err != nil {
		http.Error(w, "Internal server error, failed to generate token", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(login_resp)
}

func (s *Server) PublishSMDValidationHandler(w http.ResponseWriter, r *http.Request) {
	data_type := r.PathValue("data_type")
	valid_types := []string{"roughness", "rni", "pci", "defects"} // Supported end points

	if !slices.Contains(valid_types, data_type) {
		http.Error(w, "Invalid data type", http.StatusNotFound)
	}

	input, error_resp := validateRequest[job.JobRequest[job.SMDPayload]](r)

	if error_resp != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(error_resp)
		return
	}

	resp, err := s.job_service.PublishSMDValidationJob(input, data_type)

	if err != nil {
		http.Error(w, "Error when publishing job to queue", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) PublishINVIJValidationHandler(w http.ResponseWriter, r *http.Request) {
	data_type := r.PathValue("data_type")
	valid_types := []string{"master", "inventory", "popup_inventory"}

	if !slices.Contains(valid_types, data_type) {
		http.Error(w, "Invalid data type", http.StatusNotFound)
	}

	input, error_resp := validateRequest[job.JobRequest[job.INVIJPayload]](r)

	if error_resp != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(error_resp)
		return
	}

	resp, err := s.job_service.PublishINVIJValidationJob(input, data_type)

	if err != nil {
		http.Error(w, "Error when publishing job to queue", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// Handler for fetching the job status.
func (s *Server) GetJobStatusHandler(w http.ResponseWriter, r *http.Request) {
	job_id := r.PathValue("job_id")
	data_type := strings.ToUpper(r.PathValue("data_type"))

	if strings.Split(job_id, "_")[0] != data_type {
		out := make(map[string]string)
		out["error"] = "Data type does not match Job ID"

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(out)
		return
	}

	resp, err := s.job_service.GetJobStatus(job_id, data_type)

	if err != nil {
		if err == pgx.ErrNoRows {
			out := make(map[string]string)
			out["error"] = "Job ID not found."

			w.Header().Set("Content-Type", "application-json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(out)
			return
		}

		http.Error(w, "Error when submitting query", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// Handler for fetching the job result.
func (s *Server) GetJobResultHandler(w http.ResponseWriter, r *http.Request) {
	job_id := r.PathValue("job_id")
	data_type := strings.ToUpper(r.PathValue("data_type"))

	if strings.Split(job_id, "_")[0] != data_type {
		out := make(map[string]string)
		out["error"] = "Data type does not match Job ID"

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(out)
		return
	}

	resp, err := s.job_service.GetJobResult(job_id, data_type)

	if err != nil {
		if err == pgx.ErrNoRows {
			out := make(map[string]string)
			out["error"] = "Not available"

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(out)
			return
		}

		http.Error(w, "Error when submitting query", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// Handler for fetching the job result messages.
func (s *Server) GetJobResultMessages(w http.ResponseWriter, r *http.Request) {
	job_id := r.PathValue("job_id")
	data_type := strings.ToUpper(r.PathValue("data_type"))

	if strings.Split(job_id, "_")[0] != data_type {
		out := make(map[string]string)
		out["error"] = "Data type does not match Job ID"

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(out)
		return
	}

	messages, err := s.job_service.GetJobResultMessages(job_id)

	if err != nil {
		http.Error(w, "Error when submitting query", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(messages)
}
