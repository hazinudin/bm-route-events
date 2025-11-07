package job

import (
	"fmt"
	"slices"
	"time"
)

type MessageTag string
type ResultStatus string

const (
	REVIEW_MSG_TAG  MessageTag = "review"
	DISPUTE_MSG_TAG MessageTag = "force"
)

const (
	REJECTED_STATUS ResultStatus = "rejected"
	ERROR_STATUS    ResultStatus = "error"
	REVIEW_STATUS   ResultStatus = "review"
	VERIFIED_STATUS ResultStatus = "verified"
)

// Validation job result, in SMD format
type validationJobResultSMD struct {
	JobID            string                           `json:"job_id"`
	Status           ResultStatus                     `json:"status"`
	MessageCount     int                              `json:"msg_count"`
	AllMessageStatus []string                         `json:"all_msg_status"`
	Ignorables       []string                         `json:"ignorables"`
	AttemptID        int                              `json:"attempt_id"`
	Messages         []*ValidationJobResultMessageSMD `json:"messages"`
}

// Validation job result, in INVIJ format
type validationJobResultINVIJ struct {
	JobID            string                           `json:"job_id"`
	Status           ResultStatus                     `json:"status"`
	MessageCount     int                              `json:"msg_count"`
	AllMessageStatus []string                         `json:"all_msg_status"`
	Ignorables       []string                         `json:"ignorables"`
	AttemptID        int                              `json:"attempt_id"`
	Messages         *ValidationJobResultMessageINVIJ `json:"messages"`
}

// Validation job result, contains the results and final status of the finished job.
type ValidationJobResult struct {
	JobID            string       `json:"job_id"`
	Status           ResultStatus `json:"status"`
	MessageCount     int          `json:"msg_count"`
	AllMessageStatus []string     `json:"all_msg_status"`
	Ignorables       []string     `json:"ignorables"`
	AttemptID        int          `json:"attempt_id"`
	messages         []ValidationJobResultMessage
	ignored_tag      []MessageTag
	events           []JobEventInterface
}

func NewJobResult(
	job_id string,
	status ResultStatus,
	msg_count int,
	all_msg_status []string,
	ignorables []string,
	ignored_tag []MessageTag,
	attempt_id int,
) *ValidationJobResult {
	return &ValidationJobResult{
		JobID:            job_id,
		Status:           status,
		MessageCount:     msg_count,
		AllMessageStatus: all_msg_status,
		Ignorables:       ignorables,
		ignored_tag:      ignored_tag,
		AttemptID:        attempt_id,
	}
}

func (j *ValidationJobResult) AddMessages(messages []ValidationJobResultMessage) {
	j.messages = messages
}

func (j *ValidationJobResult) ToSMDResponse() *validationJobResultSMD {
	var smd_messages []*ValidationJobResultMessageSMD

	for _, msg := range j.messages {
		smd_messages = append(smd_messages, msg.ToSMDMessages())
	}

	out := validationJobResultSMD{
		JobID:            j.JobID,
		Status:           j.Status,
		MessageCount:     j.MessageCount,
		AllMessageStatus: j.AllMessageStatus,
		Ignorables:       j.Ignorables,
		AttemptID:        j.AttemptID,
		Messages:         []*ValidationJobResultMessageSMD{},
	}

	if smd_messages != nil {
		out.Messages = smd_messages
	}

	return &out
}

func (j *ValidationJobResult) ToINVIJResponse() *validationJobResultINVIJ {
	var invij_msg ValidationJobResultMessageINVIJ
	var general_msgs []string
	status := "unverified"

	if j.Status == REJECTED_STATUS {
		invij_msg.Status = status
		invij_msg.General = make(map[string]any)
		invij_msg.General["status"] = "error"

		for _, msg := range j.messages {
			general_msgs = append(general_msgs, msg.Message)
		}

		invij_msg.General["error"] = general_msgs
	} else {
		invij_msg.Status = string(j.Status)
		invij_msg.General = make(map[string]any)
		invij_msg.General["status"] = "verified"
		invij_msg.General["error"] = general_msgs

		for _, msg := range j.messages {
			if msg.MessageStatus == string(ERROR_STATUS) {
				invij_msg.Errors = append(invij_msg.Errors, msg.Message)
			}

			if msg.MessageStatus == string(REVIEW_STATUS) {
				invij_msg.Reviews = append(invij_msg.Reviews, msg.Message)
			}
		}
	}

	out := validationJobResultINVIJ{
		JobID:            j.JobID,
		Status:           j.Status,
		MessageCount:     j.MessageCount,
		AllMessageStatus: j.AllMessageStatus,
		Ignorables:       j.Ignorables,
		AttemptID:        j.AttemptID,
		Messages:         &invij_msg,
	}

	return &out
}

// Get all the ignored tags
func (j *ValidationJobResult) GetIgnoredTags() []MessageTag {
	return j.ignored_tag
}

// Internal ignore message tag which used by IgnoreReviewed and IgnoreDisputed
func (j *ValidationJobResult) ignore_msg_tag(tag MessageTag) error {
	if j.Status == VERIFIED_STATUS {
		return fmt.Errorf("result is already verified")
	}

	if j.Status == REJECTED_STATUS {
		return fmt.Errorf("result with rejected status could not have its messages ignored")
	}

	if slices.Contains(j.ignored_tag, tag) {
		return fmt.Errorf("the result already ignored the '%s' message tag", tag)
	}

	// Check if the tag is in ignorables
	if slices.Contains(j.Ignorables, string(tag)) {
		j.ignored_tag = append(j.ignored_tag, tag)

		j.Ignorables = slices.DeleteFunc(
			j.Ignorables,
			func(_tag string) bool { return _tag == string(tag) },
		)

		// Remove 'error' if 'force' tag is ignored, if 'review' then remove 'review' in AllMessageStatus
		switch tag {
		case DISPUTE_MSG_TAG:
			j.AllMessageStatus = slices.DeleteFunc(
				j.AllMessageStatus,
				func(_status string) bool { return _status == string(ERROR_STATUS) },
			)
		case REVIEW_MSG_TAG:
			j.AllMessageStatus = slices.DeleteFunc(
				j.AllMessageStatus,
				func(_status string) bool { return _status == string(REVIEW_STATUS) },
			)
		}

		// If all ignorables all are ignored, then set the status to 'verified'
		if len(j.Ignorables) == 0 {
			j.Status = VERIFIED_STATUS

			event := AllMessagesAccepted{
				JobEvent: JobEvent{
					JobID:     j.JobID,
					OccuredAt: time.Now().UnixMilli() + 10, // Create offset for better logging
				},
			}

			j.events = append(j.events, &event)
			return nil
		}

		// If the ignorables is only 1 and the value is 'review', then set the status to 'review'
		if len(j.Ignorables) == 1 && slices.Contains(j.Ignorables, string(REVIEW_MSG_TAG)) {
			j.Status = REVIEW_STATUS
			return nil
		}

		return nil
	} else {
		return fmt.Errorf("the result does not have '%s' as ignorables", tag)
	}
}

// Add "review" tag to the ignored message list
func (j *ValidationJobResult) IgnoreReviewed() error {
	err := j.ignore_msg_tag(REVIEW_MSG_TAG)

	if err != nil {
		return err
	}

	event := ReviewedMessagesAccepted{
		JobEvent: JobEvent{
			JobID:     j.JobID,
			OccuredAt: time.Now().UnixMilli(),
		},
	}

	j.events = append(j.events, &event)

	return nil
}

// Add "force" tag to the ignored message list
func (j *ValidationJobResult) IgnoreDisputed() error {
	err := j.ignore_msg_tag(DISPUTE_MSG_TAG)

	if err != nil {
		return err
	}

	event := DisputedMessagesAccepted{
		JobEvent: JobEvent{
			JobID:     j.JobID,
			OccuredAt: time.Now().UnixMilli(),
		},
	}

	j.events = append(j.events, &event)

	return nil
}

func (j *ValidationJobResult) GetAllEvents() []JobEventInterface {
	return j.events
}

// Message in SMD format
type ValidationJobResultMessageSMD struct {
	Message       string `json:"msg"`
	MessageStatus string `json:"status"`
	ContentID     string `json:"linkid"`
}

// Message in INVIJ format
type ValidationJobResultMessageINVIJ struct {
	General map[string]any `json:"msg"`
	Status  string         `json:"status"`
	Errors  []string       `json:"error"`
	Reviews []string       `json:"reviews"`
}

// All messages generated from the validation process.
type ValidationJobResultMessage struct {
	Message       string `json:"msg"`
	MessageStatus string `json:"msg_status"`
	ContentID     string `json:"id"`
	IgnoreIn      string `json:"ignore_in"`
}

func (m *ValidationJobResultMessage) ToSMDMessages() *ValidationJobResultMessageSMD {
	var out ValidationJobResultMessageSMD

	out.ContentID = m.ContentID

	// Overwrite the error with "force" tag to "error_sanggah"
	if m.IgnoreIn == "force" {
		out.MessageStatus = "error_sanggah"
	} else {
		out.MessageStatus = m.MessageStatus
	}

	out.Message = m.Message

	return &out
}
