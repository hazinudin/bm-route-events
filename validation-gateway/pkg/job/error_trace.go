package job

import "gorm.io/gorm"

type ValidationJobErrorTrace struct {
	gorm.Model
	JobID string `json:"jobid" gorm:"index"`
	Trace []byte `json:"trace"`
}
