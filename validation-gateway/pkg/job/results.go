package job

import "gorm.io/gorm"

type ValidationJobResults struct {
	gorm.Model
	JobID     string `json:"jobid" gorm:"index"`
	Msg       string `json:"msg"`
	Status    string `json:"status"`
	StatusIDX int    `json:"status_idx"`
	IgnoreIn  string `json:"ignore_in"`
	ContentID string `json:"content_id"`
}
