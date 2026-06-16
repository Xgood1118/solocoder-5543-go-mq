package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/solomq/internal/models"
)

type StorageManager struct {
	dataDir string
	topics  map[string]*TopicStorage
	mu      sync.RWMutex
}

type TopicStorage struct {
	Topic      *models.Topic
	Partitions []*AppendOnlyLog
	DLQ        *DLQStore
	MetaFile   string
}

func NewStorageManager(dataDir string) (*StorageManager, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dataDir, "topics"), 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dataDir, "meta"), 0755); err != nil {
		return nil, err
	}

	sm := &StorageManager{
		dataDir: dataDir,
		topics:  make(map[string]*TopicStorage),
	}

	if err := sm.loadExistingTopics(); err != nil {
		return nil, err
	}

	return sm, nil
}

func (sm *StorageManager) loadExistingTopics() error {
	entries, err := os.ReadDir(filepath.Join(sm.dataDir, "topics"))
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		topicName := entry.Name()
		metaPath := filepath.Join(sm.dataDir, "meta", topicName+".json")
		if _, err := os.Stat(metaPath); os.IsNotExist(err) {
			continue
		}

		data, err := os.ReadFile(metaPath)
		if err != nil {
			return err
		}

		var topic models.Topic
		if err := json.Unmarshal(data, &topic); err != nil {
			return err
		}

		ts, err := sm.openTopicStorage(&topic)
		if err != nil {
			return fmt.Errorf("open topic %s storage: %w", topicName, err)
		}
		sm.topics[topicName] = ts
	}

	return nil
}

func (sm *StorageManager) openTopicStorage(topic *models.Topic) (*TopicStorage, error) {
	partitions := make([]*AppendOnlyLog, topic.Config.Partitions)
	for i := 0; i < topic.Config.Partitions; i++ {
		log, err := NewAppendOnlyLog(sm.dataDir, topic.Config.Name, i)
		if err != nil {
			for j := 0; j < i; j++ {
				partitions[j].Close()
			}
			return nil, err
		}
		partitions[i] = log
	}

	dlq, err := NewDLQStore(sm.dataDir, topic.Config.Name)
	if err != nil {
		for _, p := range partitions {
			p.Close()
		}
		return nil, err
	}

	return &TopicStorage{
		Topic:      topic,
		Partitions: partitions,
		DLQ:        dlq,
		MetaFile:   filepath.Join(sm.dataDir, "meta", topic.Config.Name+".json"),
	}, nil
}

func (sm *StorageManager) CreateTopic(config models.TopicConfig) (*models.Topic, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.topics[config.Name]; exists {
		return nil, fmt.Errorf("topic %s already exists", config.Name)
	}

	topic, err := models.NewTopic(config)
	if err != nil {
		return nil, err
	}

	ts, err := sm.openTopicStorage(topic)
	if err != nil {
		return nil, err
	}

	metaData, err := json.MarshalIndent(topic, "", "  ")
	if err != nil {
		ts.DLQ.Close()
		for _, p := range ts.Partitions {
			p.Close()
		}
		return nil, err
	}
	if err := os.WriteFile(ts.MetaFile, metaData, 0644); err != nil {
		ts.DLQ.Close()
		for _, p := range ts.Partitions {
			p.Close()
		}
		return nil, err
	}

	for i, partition := range ts.Partitions {
		topic.PartitionMeta[i].NextOffset = partition.NextOffset()
	}

	sm.topics[config.Name] = ts
	return topic, nil
}

func (sm *StorageManager) GetTopic(name string) (*models.Topic, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	ts, exists := sm.topics[name]
	if !exists {
		return nil, false
	}
	return ts.Topic, true
}

func (sm *StorageManager) GetAllTopics() []*models.Topic {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	topics := make([]*models.Topic, 0, len(sm.topics))
	for _, ts := range sm.topics {
		topics = append(topics, ts.Topic)
	}
	return topics
}

func (sm *StorageManager) GetPartitionLog(topic string, partition int) (*AppendOnlyLog, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	ts, exists := sm.topics[topic]
	if !exists || partition < 0 || partition >= len(ts.Partitions) {
		return nil, false
	}
	return ts.Partitions[partition], true
}

func (sm *StorageManager) GetDLQ(topic string) (*DLQStore, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	ts, exists := sm.topics[topic]
	if !exists {
		return nil, false
	}
	return ts.DLQ, true
}

func (sm *StorageManager) AppendMessage(topic string, partition int, msg *models.Message) (int64, error) {
	sm.mu.RLock()
	ts, exists := sm.topics[topic]
	sm.mu.RUnlock()
	if !exists {
		return -1, fmt.Errorf("topic %s not found", topic)
	}
	if partition < 0 || partition >= len(ts.Partitions) {
		return -1, fmt.Errorf("invalid partition %d", partition)
	}

	offset, err := ts.Partitions[partition].Append(msg)
	if err != nil {
		return -1, err
	}

	sm.mu.Lock()
	ts.Topic.PartitionMeta[partition].NextOffset = ts.Partitions[partition].NextOffset()
	sm.mu.Unlock()

	return offset, nil
}

func (sm *StorageManager) ReadMessage(topic string, partition int, offset int64) (*models.Message, error) {
	sm.mu.RLock()
	ts, exists := sm.topics[topic]
	sm.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("topic %s not found", topic)
	}
	if partition < 0 || partition >= len(ts.Partitions) {
		return nil, fmt.Errorf("invalid partition %d", partition)
	}
	return ts.Partitions[partition].Read(offset)
}

func (sm *StorageManager) GetTotalMessages(topic string) int64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	ts, exists := sm.topics[topic]
	if !exists {
		return 0
	}
	var total int64
	for _, p := range ts.Partitions {
		total += p.NextOffset()
	}
	return total
}

func (sm *StorageManager) GetPartitionNextOffset(topic string, partition int) int64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	ts, exists := sm.topics[topic]
	if !exists || partition < 0 || partition >= len(ts.Partitions) {
		return 0
	}
	return ts.Partitions[partition].NextOffset()
}

func (sm *StorageManager) CleanupExpired(topic string, retention time.Duration) (int64, error) {
	sm.mu.RLock()
	ts, exists := sm.topics[topic]
	sm.mu.RUnlock()
	if !exists {
		return 0, fmt.Errorf("topic %s not found", topic)
	}

	var totalDeleted int64
	for _, p := range ts.Partitions {
		deleted, err := p.CleanupExpired(retention)
		if err != nil {
			return totalDeleted, err
		}
		totalDeleted += deleted
	}
	return totalDeleted, nil
}

func (sm *StorageManager) Close() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for _, ts := range sm.topics {
		for _, p := range ts.Partitions {
			p.Close()
		}
		ts.DLQ.Close()
	}
}
