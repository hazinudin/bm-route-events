package job

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"
)

func TestValidationJobResultUnmarshall(t *testing.T) {
	input := []byte(
		`{
			"job_id": "1234",
			"status": "error",
			"msg_count": 10,
			"all_msg_status": ["error", "review"],
			"ignorables": ["force", "review"]
		}`,
	)

	var result ValidationJobResult

	t.Run(
		"serialize json string", func(t *testing.T) {
			err := json.Unmarshal(input, &result)

			if err != nil {
				t.Error(err)
			}
		},
	)

	t.Run(
		"status check", func(t *testing.T) {
			if result.Status != ERROR_STATUS {
				t.Errorf("Status should be %s not, %s", ERROR_STATUS, result.Status)
			}
		},
	)

}

func TestValidationJobResult(t *testing.T) {
	input := ValidationJobResult{
		JobID:            "1234",
		Status:           ERROR_STATUS,
		MessageCount:     100,
		AllMessageStatus: []string{"error", "review"},
		Ignorables: []string{
			string(DISPUTE_MSG_TAG),
			string(REVIEW_MSG_TAG),
		},
	}

	t.Run(
		"empty ignored slice", func(t *testing.T) {
			if len(input.ignored_tag) != 0 {
				t.Errorf("Expected %d\nGot: %s", 0, input.ignored_tag)
			}
		},
	)

	t.Run(
		"ignore disputed message", func(t *testing.T) {
			err := input.IgnoreDisputed()

			if err != nil {
				t.Error(err)
			}
		},
	)

	t.Run(
		"ignorables is not empty", func(t *testing.T) {
			if len(input.Ignorables) == 0 {
				t.Error("Ignorables should not be empty after disputed message is ignored.")
			}
		},
	)

	t.Run(
		"ignorables still contains review", func(t *testing.T) {
			if !slices.Contains(input.Ignorables, string(REVIEW_MSG_TAG)) {
				t.Errorf("Ignorables does not have %s", string(REVIEW_MSG_TAG))
			}
		},
	)

	t.Run(
		"status set to review", func(t *testing.T) {
			if input.Status != REVIEW_STATUS {
				t.Errorf("Status should be equal to %s, not %s", REVIEW_STATUS, input.Status)
			}
		},
	)

	t.Run(
		"events is not empty", func(t *testing.T) {
			if len(input.GetAllEvents()) == 0 {
				t.Error("Events should not be empty, disputed message already ignored.")
			}

			event := DisputedMessagesAccepted{}

			if reflect.TypeOf(input.GetAllEvents()[0]) != reflect.TypeOf(&event) {
				t.Errorf("First event should be %T, not %T", event, input.GetAllEvents()[0])
			}
		},
	)

	t.Run(
		"ignore reviewed messages", func(t *testing.T) {
			err := input.IgnoreReviewed()

			if err != nil {
				t.Error(err)
			}
		},
	)

	t.Run(
		"status set to verified", func(t *testing.T) {
			if input.Status != VERIFIED_STATUS {
				t.Errorf("Status shold be equal to %s, not %s", VERIFIED_STATUS, input.Status)
			}
		},
	)
}
