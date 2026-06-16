package models

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"time"
)

type MessageState string

const (
	StatePending  MessageState = "pending"
	StateInFlight MessageState = "in_flight"
	StateAcked    MessageState = "acked"
	StateDLQ      MessageState = "dlq"
	StateExpired  MessageState = "expired"
)

type Priority int

const (
	PriorityLow    Priority = 1
	PriorityMedium Priority = 2
	PriorityHigh   Priority = 3
)

type StateTransition struct {
	From      MessageState `json:"from"`
	To        MessageState `json:"to"`
	Timestamp int64        `json:"timestamp"`
	Instance  string       `json:"instance"`
}

type Message struct {
	ID              string           `json:"id"`
	Topic           string           `json:"topic"`
	Partition       int              `json:"partition"`
	Offset          int64            `json:"offset"`
	Key             string           `json:"key"`
	Payload         []byte           `json:"payload"`
	Priority        Priority         `json:"priority"`
	State           MessageState     `json:"state"`
	DelaySeconds    int              `json:"delay_seconds"`
	AvailableAt     int64            `json:"available_at"`
	RetryCount      int              `json:"retry_count"`
	MaxRetries      int              `json:"max_retries"`
	VisibilityUntil int64            `json:"visibility_until"`
	ConsumerGroup   string           `json:"consumer_group"`
	ConsumerInstance string          `json:"consumer_instance"`
	StateHistory    []StateTransition `json:"state_history"`
	CreatedAt       int64            `json:"created_at"`
}

type CreateMessageRequest struct {
	Key          string   `json:"key"`
	Payload      []byte   `json:"payload"`
	Priority     Priority `json:"priority"`
	DelaySeconds int      `json:"delay_seconds"`
}

func HashKey(key string, partitions int) int {
	if partitions <= 1 {
		return 0
	}
	h := sha256.Sum256([]byte(key))
	hash := binary.BigEndian.Uint64(h[:8])
	return int(hash % uint64(partitions))
}

func (m *Message) TransitionsState(to MessageState, instance string) {
	now := time.Now().UnixMilli()
	m.StateHistory = append(m.StateHistory, StateTransition{
		From:      m.State,
		To:        to,
		Timestamp: now,
		Instance:  instance,
	})
	m.State = to
}

func (m *Message) ToJSON() ([]byte, error) {
	return json.Marshal(m)
}

func MessageFromJSON(data []byte) (*Message, error) {
	var m Message
	err := json.Unmarshal(data, &m)
	return &m, err
}
