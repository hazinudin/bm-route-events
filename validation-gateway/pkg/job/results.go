package job

type ValidationJobResult struct {
	JobID            string   `json:"job_id"`
	Status           string   `json:"status"`
	MessageCount     int      `json:"msg_count"`
	AllMessageStatus []string `json:"all_msg_status"`
	Ignorables       []string `json:"ignorables"`
}

type ValidationJobResultMessage struct {
	Message       string `json:"msg" db:"msg"`
	MessageStatus string `json:"msg_status" db:"msg_status"`
	ContentID     string `json:"id" db:"content_id"`
}
