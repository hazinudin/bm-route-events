package job

import (
	"encoding/json"
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

func (j *ValidationJobResult) ToSMDResponse() (map[string]any, error) {
	var out map[string]any
	var smd_messages []map[string]string

	json_bytes, err := json.Marshal(j)

	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(json_bytes, &out)

	if err != nil {
		return nil, err
	}

	for _, msg := range j.messages {
		smd_messages = append(smd_messages, msg.ToSMDMessages())
	}

	out["messages"] = smd_messages

	return out, nil
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

// All messages generated from the validation process.
type ValidationJobResultMessage struct {
	Message       string `json:"msg"`
	MessageStatus string `json:"msg_status"`
	ContentID     string `json:"id"`
	IgnoreIn      string `json:"ignore_in"`
}

func (m *ValidationJobResultMessage) ToSMDMessages() map[string]string {
	out := make(map[string]string)

	out["linkid"] = m.ContentID

	// Overwrite the error with "force" tag to "error_sanggah"
	if m.IgnoreIn == "force" {
		out["status"] = "error_sanggah"
	} else {
		out["status"] = m.MessageStatus
	}

	out["msg"] = m.Message

	return out
}
