package models

import (
	"sync"
	"time"
)

type TopicMetrics struct {
	TopicName        string  `json:"topic_name"`
	TotalMessages    int64   `json:"total_messages"`
	InFlightCount    int64   `json:"in_flight_count"`
	DLQCount         int64   `json:"dlq_count"`
	TotalLag         int64   `json:"total_lag"`
	ConsumeRate      float64 `json:"consume_rate"`
	PublishRate      float64 `json:"publish_rate"`
	LastUpdated      int64   `json:"last_updated"`
	RateHistory      []RatePoint `json:"rate_history"`
}

type RatePoint struct {
	Timestamp int64   `json:"timestamp"`
	Publish   float64 `json:"publish"`
	Consume   float64 `json:"consume"`
}

type ConsumerGroupMetrics struct {
	GroupName      string            `json:"group_name"`
	Topic          string            `json:"topic"`
	TotalLag       int64             `json:"total_lag"`
	PartitionLags  map[int]int64     `json:"partition_lags"`
	InstanceCount  int               `json:"instance_count"`
	InFlightCount  int64             `json:"in_flight_count"`
	LastUpdated    int64             `json:"last_updated"`
}

type MetricsStore struct {
	Topics        map[string]*TopicMetrics
	ConsumerGroups map[string]*ConsumerGroupMetrics
	mu            sync.RWMutex
	rateSamples   map[string][]RateSample
}

type RateSample struct {
	Timestamp   int64
	PublishDiff int64
	ConsumeDiff int64
}

func NewMetricsStore() *MetricsStore {
	return &MetricsStore{
		Topics:         make(map[string]*TopicMetrics),
		ConsumerGroups: make(map[string]*ConsumerGroupMetrics),
		rateSamples:    make(map[string][]RateSample),
	}
}

func (ms *MetricsStore) UpdateTopicMetrics(topic string, totalMsgs, inFlight, dlq int64, publishDiff, consumeDiff int64) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	now := time.Now().UnixMilli()
	sample := RateSample{
		Timestamp:   now,
		PublishDiff: publishDiff,
		ConsumeDiff: consumeDiff,
	}
	ms.rateSamples[topic] = append(ms.rateSamples[topic], sample)

	cutoff := now - 5*60*1000
	filtered := ms.rateSamples[topic][:0]
	for _, s := range ms.rateSamples[topic] {
		if s.Timestamp > cutoff {
			filtered = append(filtered, s)
		}
	}
	ms.rateSamples[topic] = filtered

	var publishRate, consumeRate float64
	if len(filtered) > 1 {
		first := filtered[0]
		last := filtered[len(filtered)-1]
		durationSec := float64(last.Timestamp-first.Timestamp) / 1000.0
		if durationSec > 0 {
			var totalPublish, totalConsume int64
			for _, s := range filtered {
				totalPublish += s.PublishDiff
				totalConsume += s.ConsumeDiff
			}
			publishRate = float64(totalPublish) / durationSec
			consumeRate = float64(totalConsume) / durationSec
		}
	}

	history := make([]RatePoint, 0, 30)
	intervalMs := int64(10000)
	for i := 29; i >= 0; i-- {
		windowStart := now - int64(i)*intervalMs
		windowEnd := windowStart + intervalMs
		var pub, con int64
		for _, s := range filtered {
			if s.Timestamp >= windowStart && s.Timestamp < windowEnd {
				pub += s.PublishDiff
				con += s.ConsumeDiff
			}
		}
		history = append(history, RatePoint{
			Timestamp: windowStart,
			Publish:   float64(pub) / 10.0,
			Consume:   float64(con) / 10.0,
		})
	}

	if _, exists := ms.Topics[topic]; !exists {
		ms.Topics[topic] = &TopicMetrics{TopicName: topic}
	}
	ms.Topics[topic].TotalMessages = totalMsgs
	ms.Topics[topic].InFlightCount = inFlight
	ms.Topics[topic].DLQCount = dlq
	ms.Topics[topic].PublishRate = publishRate
	ms.Topics[topic].ConsumeRate = consumeRate
	ms.Topics[topic].LastUpdated = now
	ms.Topics[topic].RateHistory = history
}

func (ms *MetricsStore) UpdateConsumerGroupMetrics(groupName, topic string, totalLag int64, partitionLags map[int]int64, instanceCount int, inFlight int64) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	key := topic + "/" + groupName
	if _, exists := ms.ConsumerGroups[key]; !exists {
		ms.ConsumerGroups[key] = &ConsumerGroupMetrics{
			GroupName:     groupName,
			Topic:         topic,
			PartitionLags: make(map[int]int64),
		}
	}
	ms.ConsumerGroups[key].TotalLag = totalLag
	ms.ConsumerGroups[key].PartitionLags = partitionLags
	ms.ConsumerGroups[key].InstanceCount = instanceCount
	ms.ConsumerGroups[key].InFlightCount = inFlight
	ms.ConsumerGroups[key].LastUpdated = time.Now().UnixMilli()
}

func (ms *MetricsStore) GetTopicMetrics(topic string) (*TopicMetrics, bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	m, exists := ms.Topics[topic]
	return m, exists
}

func (ms *MetricsStore) GetAllTopicMetrics() []*TopicMetrics {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	result := make([]*TopicMetrics, 0, len(ms.Topics))
	for _, v := range ms.Topics {
		result = append(result, v)
	}
	return result
}

func (ms *MetricsStore) GetConsumerGroupMetrics(topic, group string) (*ConsumerGroupMetrics, bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	key := topic + "/" + group
	m, exists := ms.ConsumerGroups[key]
	return m, exists
}

func (ms *MetricsStore) GetAllConsumerGroupMetrics() []*ConsumerGroupMetrics {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	result := make([]*ConsumerGroupMetrics, 0, len(ms.ConsumerGroups))
	for _, v := range ms.ConsumerGroups {
		result = append(result, v)
	}
	return result
}
