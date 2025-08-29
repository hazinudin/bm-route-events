package job

type ValidationJobResults struct {
	Status           string   `json:"status"`
	MessageCount     int      `json:"msg_count"`
	AllMessageStatus []string `json:"all_msg_status"`
	Ignorables       []string `json:"ignorables"`
}
