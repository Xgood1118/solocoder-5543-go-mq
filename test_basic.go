package main

import (
	"fmt"
	"log"
	"time"

	"github.com/solomq/config"
	"github.com/solomq/internal/broker"
	"github.com/solomq/internal/models"
)

func main() {
	config.Load()
	log.Printf("=== SoloMQ Basic Test ===")

	b, err := broker.NewBroker()
	if err != nil {
		log.Fatalf("Failed to create broker: %v", err)
	}
	defer b.Close()

	log.Printf("✓ Broker created successfully")

	topicConfig := models.TopicConfig{
		Name:              "test_topic",
		Partitions:        3,
		RetentionDays:     7,
		MaxMessageRetries: 3,
		MaxMessageSize:    1048576,
	}

	topic, err := b.CreateTopic(topicConfig)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	log.Printf("✓ Topic created: %s (partitions: %d)", topic.Config.Name, topic.Config.Partitions)

	topics := b.GetAllTopics()
	log.Printf("✓ Total topics: %d", len(topics))

	publishCount := 10
	for i := 0; i < publishCount; i++ {
		key := fmt.Sprintf("user-%d", i%3)
		req := &models.CreateMessageRequest{
			Key:          key,
			Payload:      []byte(fmt.Sprintf(`{"id":%d,"type":"test","data":"hello"}`, i)),
			Priority:     models.PriorityMedium,
			DelaySeconds: 0,
		}

		msgID, err := b.Publish("test_topic", req)
		if err != nil {
			log.Fatalf("Failed to publish: %v", err)
		}
		log.Printf("✓ Published message %d: %s (key: %s)", i+1, msgID, key)
	}
	log.Printf("✓ Published %d messages total", publishCount)

	msgs, err := b.Consume("test_topic", "test_group", "instance_1", 5, 2*time.Second)
	if err != nil {
		log.Fatalf("Failed to consume: %v", err)
	}
	log.Printf("✓ Consumed %d messages in first batch", len(msgs))

	for _, msg := range msgs {
		err := b.Ack("test_topic", "test_group", "instance_1", msg.ID)
		if err != nil {
			log.Printf("Failed to ack %s: %v", msg.ID, err)
		} else {
			log.Printf("✓ Acked message: %s (offset: %d)", msg.ID, msg.Offset)
		}
	}

	msgs2, err := b.Consume("test_topic", "test_group", "instance_1", 10, 500*time.Millisecond)
	if err != nil {
		log.Fatalf("Failed to consume: %v", err)
	}
	log.Printf("✓ Consumed %d messages in second batch", len(msgs2))

	b.RefreshMetrics()
	metrics := b.GetMetrics()
	tm, exists := metrics.GetTopicMetrics("test_topic")
	if exists {
		log.Printf("✓ Metrics - Total: %d, InFlight: %d, PublishRate: %.2f/s, ConsumeRate: %.2f/s",
			tm.TotalMessages, tm.InFlightCount, tm.PublishRate, tm.ConsumeRate)
	}

	dlqMsgs := b.GetDLQMessages("test_topic")
	log.Printf("✓ DLQ messages: %d", len(dlqMsgs))

	groups := b.GetConsumerGroups("test_topic")
	log.Printf("✓ Consumer groups: %d", len(groups))

	log.Printf("")
	log.Printf("=== All tests passed! ===")
	log.Printf("")
	log.Printf("Next steps:")
	log.Printf("  1. Run: .\\solomq.exe")
	log.Printf("  2. Open admin UI: http://localhost:8320/admin?token=admin123")
	log.Printf("  3. Test API:")
	log.Printf("     - Create topic: POST /api/v1/topics (X-Admin-Token: admin123)")
	log.Printf("     - Publish: POST /api/v1/topics/test_topic/publish")
	log.Printf("     - Consume: POST /api/v1/topics/test_topic/consume?group=test_group&instance=inst1")
}
