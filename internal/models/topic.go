package models

import (
	"fmt"
	"regexp"
	"time"
)

var topicNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

type TopicConfig struct {
	Name              string        `json:"name"`
	Partitions        int           `json:"partitions"`
	RetentionDays     int           `json:"retention_days"`
	MaxMessageRetries int           `json:"max_message_retries"`
	MaxMessageSize    int           `json:"max_message_size"`
}

type Topic struct {
	Config        TopicConfig `json:"config"`
	CreatedAt     int64       `json:"created_at"`
	PartitionMeta []PartitionMeta `json:"partition_meta"`
}

type PartitionMeta struct {
	Partition  int   `json:"partition"`
	NextOffset int64 `json:"next_offset"`
	DLQOffset  int64 `json:"dlq_offset"`
}

func ValidateTopicName(name string) error {
	if !topicNameRegex.MatchString(name) {
		return fmt.Errorf("topic name must contain only alphanumeric characters and underscores")
	}
	if len(name) < 1 || len(name) > 128 {
		return fmt.Errorf("topic name length must be between 1 and 128")
	}
	return nil
}

func NewTopic(config TopicConfig) (*Topic, error) {
	if err := ValidateTopicName(config.Name); err != nil {
		return nil, err
	}
	if config.Partitions < 1 || config.Partitions > 64 {
		return nil, fmt.Errorf("partitions must be between 1 and 64")
	}
	if config.RetentionDays <= 0 {
		config.RetentionDays = 7
	}
	if config.MaxMessageRetries <= 0 {
		config.MaxMessageRetries = 3
	}

	partitions := make([]PartitionMeta, config.Partitions)
	for i := 0; i < config.Partitions; i++ {
		partitions[i] = PartitionMeta{
			Partition:  i,
			NextOffset: 0,
			DLQOffset:  0,
		}
	}

	return &Topic{
		Config:        config,
		CreatedAt:     time.Now().UnixMilli(),
		PartitionMeta: partitions,
	}, nil
}

func (t *Topic) RetentionDuration() time.Duration {
	return time.Duration(t.Config.RetentionDays) * 24 * time.Hour
}
