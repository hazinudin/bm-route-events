package job

import (
	"fmt"
	"slices"
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
	ignored_tag      []MessageTag
}

func NewJobResult(
	job_id string,
	status ResultStatus,
	msg_count int,
	all_msg_status []string,
	ignorables []string,
	ignored_tag []MessageTag,
) *ValidationJobResult {
	return &ValidationJobResult{
		JobID:            job_id,
		Status:           status,
		MessageCount:     msg_count,
		AllMessageStatus: all_msg_status,
		Ignorables:       ignorables,
		ignored_tag:      ignored_tag,
	}
}

// Get all the ignored tags
func (j *ValidationJobResult) GetIgnoredTags() []string {
	out := make([]string, len(j.ignored_tag))

	for _, tag := range j.ignored_tag {
		out = append(out, string(tag))
	}

	return out
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

	return nil
}

// Add "force" tag to the ignored message list
func (j *ValidationJobResult) IgnoreDisputed() error {
	err := j.ignore_msg_tag(DISPUTE_MSG_TAG)

	if err != nil {
		return err
	}

	return nil
}

// All messages generated from the validation process.
type ValidationJobResultMessage struct {
	Message       string `json:"msg"`
	MessageStatus string `json:"msg_status"`
	ContentID     string `json:"id"`
}
