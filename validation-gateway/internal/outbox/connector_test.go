package outbox

import (
	"testing"
	"validation-gateway/internal"
)

func TestOutboxConnector(t *testing.T) {
	conf := internal.LoadConfig()

	t.Run(
		"new connector", func(t *testing.T) {
			connector, err := NewOutboxConnector(conf)

			if err != nil {
				t.Error(err)
			}

			if connector.slotName != "outbox_slot" {
				t.Errorf("slot name should be %s", "outbox_slot")
			}

			connector.StartReplication()
		},
	)
}
