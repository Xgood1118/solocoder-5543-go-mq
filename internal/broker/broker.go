package broker

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/solomq/config"
	"github.com/solomq/internal/models"
	"github.com/solomq/internal/storage"
)

type Broker struct {
	storage        *storage.StorageManager
	consumerGroups map[string]map[string]*models.ConsumerGroup
	inFlight       map[string]map[string]*models.Message
	delayQueue     *DelayQueue
	longPollWaiters map[string][]*LongPollRequest
	metrics        *models.MetricsStore
	roundRobinCtr  map[string]int
	lastPublishCount map[string]int64
	lastConsumeCount map[string]int64
	mu             sync.RWMutex
}

type LongPollRequest struct {
	Topic         string
	ConsumerGroup string
	InstanceID    string
	ResponseChan  chan *models.Message
	Timeout       time.Time
}

type DelayMessage struct {
	Message     *models.Message
	AvailableAt int64
}

type DelayQueue []*DelayMessage

func (dq DelayQueue) Len() int { return len(dq) }
func (dq DelayQueue) Less(i, j int) bool {
	return dq[i].AvailableAt < dq[j].AvailableAt
}
func (dq DelayQueue) Swap(i, j int) { dq[i], dq[j] = dq[j], dq[i] }

func (dq *DelayQueue) Push(x interface{}) {
	*dq = append(*dq, x.(*DelayMessage))
}

func (dq *DelayQueue) Pop() interface{} {
	old := *dq
	n := len(old)
	item := old[n-1]
	*dq = old[0 : n-1]
	return item
}

func NewBroker() (*Broker, error) {
	sm, err := storage.NewStorageManager(config.AppConfig.DataDir)
	if err != nil {
		return nil, err
	}

	dq := &DelayQueue{}
	heap.Init(dq)

	b := &Broker{
		storage:         sm,
		consumerGroups:  make(map[string]map[string]*models.ConsumerGroup),
		inFlight:        make(map[string]map[string]*models.Message),
		delayQueue:      dq,
		longPollWaiters: make(map[string][]*LongPollRequest),
		metrics:         models.NewMetricsStore(),
		roundRobinCtr:   make(map[string]int),
		lastPublishCount: make(map[string]int64),
		lastConsumeCount: make(map[string]int64),
	}

	if err := b.loadConsumerGroups(); err != nil {
		return nil, fmt.Errorf("load consumer groups: %w", err)
	}

	if err := b.loadInFlightMessages(); err != nil {
		return nil, fmt.Errorf("load in-flight messages: %w", err)
	}

	topics := sm.GetAllTopics()
	for _, topic := range topics {
		b.inFlight[topic.Config.Name] = make(map[string]*models.Message)
		b.consumerGroups[topic.Config.Name] = make(map[string]*models.ConsumerGroup)
		b.lastPublishCount[topic.Config.Name] = sm.GetTotalMessages(topic.Config.Name)
		b.lastConsumeCount[topic.Config.Name] = 0
	}

	return b, nil
}

func (b *Broker) loadConsumerGroups() error {
	groupsDir := filepath.Join(config.AppConfig.DataDir, "consumer_groups")
	if _, err := os.Stat(groupsDir); os.IsNotExist(err) {
		return nil
	}

	entries, err := os.ReadDir(groupsDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(groupsDir, entry.Name()))
		if err != nil {
			return err
		}
		var cg models.ConsumerGroup
		if err := json.Unmarshal(data, &cg); err != nil {
			continue
		}

		b.mu.Lock()
		if _, exists := b.consumerGroups[cg.Topic]; !exists {
			b.consumerGroups[cg.Topic] = make(map[string]*models.ConsumerGroup)
		}
		b.consumerGroups[cg.Topic][cg.Name] = &cg
		b.mu.Unlock()
	}

	return nil
}

func (b *Broker) loadInFlightMessages() error {
	topics := b.storage.GetAllTopics()
	for _, topic := range topics {
		topicName := topic.Config.Name
		b.mu.Lock()
		b.inFlight[topicName] = make(map[string]*models.Message)
		b.mu.Unlock()

		for p := 0; p < topic.Config.Partitions; p++ {
			nextOffset := b.storage.GetPartitionNextOffset(topicName, p)
			for offset := int64(0); offset < nextOffset; offset++ {
				msg, err := b.storage.ReadMessage(topicName, p, offset)
				if err != nil {
					continue
				}
				if msg != nil && msg.State == models.StateInFlight {
					b.mu.Lock()
					b.inFlight[topicName][msg.ID] = msg
					b.mu.Unlock()
				}
			}
		}
	}
	return nil
}

func (b *Broker) saveConsumerGroup(topic, group string) error {
	groupsDir := filepath.Join(config.AppConfig.DataDir, "consumer_groups")
	if err := os.MkdirAll(groupsDir, 0755); err != nil {
		return err
	}

	b.mu.RLock()
	cg, exists := b.consumerGroups[topic][group]
	b.mu.RUnlock()
	if !exists {
		return nil
	}

	data, err := json.MarshalIndent(cg, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(groupsDir, fmt.Sprintf("%s_%s.json", topic, group)), data, 0644)
}

func (b *Broker) CreateTopic(config models.TopicConfig) (*models.Topic, error) {
	topic, err := b.storage.CreateTopic(config)
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	b.inFlight[config.Name] = make(map[string]*models.Message)
	b.consumerGroups[config.Name] = make(map[string]*models.ConsumerGroup)
	b.lastPublishCount[config.Name] = 0
	b.lastConsumeCount[config.Name] = 0
	b.roundRobinCtr[config.Name] = 0
	b.mu.Unlock()

	return topic, nil
}

func (b *Broker) GetTopic(name string) (*models.Topic, bool) {
	return b.storage.GetTopic(name)
}

func (b *Broker) GetAllTopics() []*models.Topic {
	return b.storage.GetAllTopics()
}

func (b *Broker) routePartition(topicName string, key string, partitions int) int {
	if key != "" {
		return models.HashKey(key, partitions)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	idx := b.roundRobinCtr[topicName]
	b.roundRobinCtr[topicName] = (idx + 1) % partitions
	return idx
}

func (b *Broker) Publish(topicName string, req *models.CreateMessageRequest) (string, error) {
	topic, exists := b.storage.GetTopic(topicName)
	if !exists {
		return "", fmt.Errorf("topic %s not found", topicName)
	}

	if len(req.Payload) > topic.Config.MaxMessageSize {
		return "", fmt.Errorf("message size %d exceeds max %d", len(req.Payload), topic.Config.MaxMessageSize)
	}

	if req.DelaySeconds > 24*3600 {
		return "", fmt.Errorf("delay %d seconds exceeds max 24 hours", req.DelaySeconds)
	}

	if req.Priority == 0 {
		req.Priority = models.PriorityMedium
	}

	msgID := fmt.Sprintf("%s-%d-%d", topicName, time.Now().UnixNano(), req.Priority)

	partition := b.routePartition(topicName, req.Key, topic.Config.Partitions)

	now := time.Now()
	availableAt := now.UnixMilli()
	if req.DelaySeconds > 0 {
		availableAt = now.Add(time.Duration(req.DelaySeconds) * time.Second).UnixMilli()
	}

	msg := &models.Message{
		ID:              msgID,
		Topic:           topicName,
		Partition:       partition,
		Key:             req.Key,
		Payload:         req.Payload,
		Priority:        req.Priority,
		State:           models.StatePending,
		DelaySeconds:    req.DelaySeconds,
		AvailableAt:     availableAt,
		RetryCount:      0,
		MaxRetries:      topic.Config.MaxMessageRetries,
		VisibilityUntil: 0,
		StateHistory:    []models.StateTransition{},
		CreatedAt:       now.UnixMilli(),
	}

	msg.TransitionsState(models.StatePending, "")

	if req.DelaySeconds > 0 {
		b.mu.Lock()
		heap.Push(b.delayQueue, &DelayMessage{
			Message:     msg,
			AvailableAt: availableAt,
		})
		b.mu.Unlock()

		offset, err := b.storage.AppendMessage(topicName, partition, msg)
		if err != nil {
			return "", err
		}
		msg.Offset = offset
		return msgID, nil
	}

	offset, err := b.storage.AppendMessage(topicName, partition, msg)
	if err != nil {
		return "", err
	}
	msg.Offset = offset

	b.notifyLongPollWaiters(topicName, msg)

	return msgID, nil
}

func (b *Broker) notifyLongPollWaiters(topicName string, msg *models.Message) {
	b.mu.Lock()
	defer b.mu.Unlock()

	waiters, exists := b.longPollWaiters[topicName]
	if !exists || len(waiters) == 0 {
		return
	}

	for _, waiter := range waiters {
		select {
		case waiter.ResponseChan <- msg:
		default:
		}
	}

	b.longPollWaiters[topicName] = nil
}

func (b *Broker) GetOrCreateConsumerGroup(topicName, groupName string) (*models.ConsumerGroup, error) {
	topic, exists := b.storage.GetTopic(topicName)
	if !exists {
		return nil, fmt.Errorf("topic %s not found", topicName)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.consumerGroups[topicName]; !exists {
		b.consumerGroups[topicName] = make(map[string]*models.ConsumerGroup)
	}

	cg, exists := b.consumerGroups[topicName][groupName]
	if !exists {
		cg = models.NewConsumerGroup(groupName, topicName, topic.Config.Partitions)
		b.consumerGroups[topicName][groupName] = cg
		go b.saveConsumerGroup(topicName, groupName)
	}

	return cg, nil
}

func (b *Broker) Consume(topicName, groupName, instanceID string, maxMessages int, timeout time.Duration) ([]*models.Message, error) {
	topic, exists := b.storage.GetTopic(topicName)
	if !exists {
		return nil, fmt.Errorf("topic %s not found", topicName)
	}

	cg, err := b.GetOrCreateConsumerGroup(topicName, groupName)
	if err != nil {
		return nil, err
	}

	cg.RegisterInstance(instanceID)
	defer b.saveConsumerGroup(topicName, groupName)

	var messages []*models.Message
	now := time.Now()
	deadline := now.Add(timeout)

	for len(messages) < maxMessages && time.Now().Before(deadline) {
		msg := b.tryGetNextMessage(topic, cg, instanceID)
		if msg != nil {
			messages = append(messages, msg)
			continue
		}

		remaining := time.Until(deadline)
		if remaining < 100*time.Millisecond {
			break
		}

		waiter := &LongPollRequest{
			Topic:         topicName,
			ConsumerGroup: groupName,
			InstanceID:    instanceID,
			ResponseChan:  make(chan *models.Message, 1),
			Timeout:       deadline,
		}

		b.mu.Lock()
		b.longPollWaiters[topicName] = append(b.longPollWaiters[topicName], waiter)
		b.mu.Unlock()

		select {
		case msg := <-waiter.ResponseChan:
			if msg != nil && b.tryAssignMessage(msg, cg, instanceID) {
				messages = append(messages, msg)
			}
		case <-time.After(remaining):
		}

		b.removeLongPollWaiter(topicName, waiter)
	}

	return messages, nil
}

func (b *Broker) tryGetNextMessage(topic *models.Topic, cg *models.ConsumerGroup, instanceID string) *models.Message {
	now := time.Now().UnixMilli()

	for p := 0; p < topic.Config.Partitions; p++ {
		offset := cg.GetOffset(p)
		nextOffset := b.storage.GetPartitionNextOffset(topic.Config.Name, p)

		for searchOffset := offset; searchOffset < nextOffset; searchOffset++ {
			msg, err := b.storage.ReadMessage(topic.Config.Name, p, searchOffset)
			if err != nil {
				continue
			}
			if msg == nil {
				continue
			}

			if msg.State == models.StatePending && msg.AvailableAt <= now {
				if b.tryAssignMessage(msg, cg, instanceID) {
					return msg
				}
			}

			if msg.State == models.StateInFlight && msg.VisibilityUntil <= now {
				if msg.ConsumerGroup == cg.Name {
					if b.tryAssignMessage(msg, cg, instanceID) {
						return msg
					}
				}
			}
		}
	}

	return nil
}

func (b *Broker) tryAssignMessage(msg *models.Message, cg *models.ConsumerGroup, instanceID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now().UnixMilli()

	if msg.State == models.StateInFlight && msg.VisibilityUntil > now {
		return false
	}

	if _, exists := b.inFlight[msg.Topic][msg.ID]; exists {
		if b.inFlight[msg.Topic][msg.ID].VisibilityUntil > now {
			return false
		}
	}

	if msg.ConsumerInstance != "" && msg.ConsumerInstance != instanceID {
		if cg.Heartbeat(msg.ConsumerInstance) {
			return false
		}
	}

	oldState := msg.State
	msg.TransitionsState(models.StateInFlight, instanceID)
	msg.VisibilityUntil = now + config.AppConfig.VisibilityTimeout.Milliseconds()
	msg.ConsumerGroup = cg.Name
	msg.ConsumerInstance = instanceID

	if msg.RetryCount > 0 {
		msg.RetryCount++
	}

	if _, exists := b.inFlight[msg.Topic]; !exists {
		b.inFlight[msg.Topic] = make(map[string]*models.Message)
	}
	b.inFlight[msg.Topic][msg.ID] = msg

	cg.IncrementInFlight(instanceID, 1)

	if oldState == models.StatePending {
		cg.SetOffset(msg.Partition, msg.Offset+1)
	}

	return true
}

func (b *Broker) removeLongPollWaiter(topicName string, waiter *LongPollRequest) {
	b.mu.Lock()
	defer b.mu.Unlock()

	waiters := b.longPollWaiters[topicName]
	for i, w := range waiters {
		if w == waiter {
			b.longPollWaiters[topicName] = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}
}

func (b *Broker) Ack(topicName, groupName, instanceID, msgID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	msg, exists := b.inFlight[topicName][msgID]
	if !exists {
		return fmt.Errorf("message %s not in-flight", msgID)
	}

	if msg.ConsumerInstance != instanceID {
		return fmt.Errorf("message %s not assigned to instance %s", msgID, instanceID)
	}

	msg.TransitionsState(models.StateAcked, instanceID)
	delete(b.inFlight[topicName], msgID)

	if cg, exists := b.consumerGroups[topicName][groupName]; exists {
		cg.IncrementInFlight(instanceID, -1)
		go b.saveConsumerGroup(topicName, groupName)
	}

	offset, err := b.storage.AppendMessage(topicName, msg.Partition, msg)
	if err != nil {
		return err
	}
	msg.Offset = offset

	return nil
}

func (b *Broker) Nack(topicName, groupName, instanceID, msgID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	msg, exists := b.inFlight[topicName][msgID]
	if !exists {
		return fmt.Errorf("message %s not in-flight", msgID)
	}

	if msg.ConsumerInstance != instanceID {
		return fmt.Errorf("message %s not assigned to instance %s", msgID, instanceID)
	}

	msg.RetryCount++
	if msg.RetryCount >= msg.MaxRetries {
		msg.TransitionsState(models.StateDLQ, instanceID)
		delete(b.inFlight[topicName], msgID)

		if dlq, ok := b.storage.GetDLQ(topicName); ok {
			if err := dlq.Add(msg); err != nil {
				log.Printf("ERROR: Failed to add message to DLQ: %v", err)
			}
		}

		offset, err := b.storage.AppendMessage(topicName, msg.Partition, msg)
		if err != nil {
			return err
		}
		msg.Offset = offset
	} else {
		msg.TransitionsState(models.StatePending, instanceID)
		msg.VisibilityUntil = 0
		delete(b.inFlight[topicName], msgID)

		offset, err := b.storage.AppendMessage(topicName, msg.Partition, msg)
		if err != nil {
			return err
		}
		msg.Offset = offset
	}

	if cg, exists := b.consumerGroups[topicName][groupName]; exists {
		cg.IncrementInFlight(instanceID, -1)
		go b.saveConsumerGroup(topicName, groupName)
	}

	return nil
}

func (b *Broker) Heartbeat(topicName, groupName, instanceID string) error {
	cg, exists := b.consumerGroups[topicName][groupName]
	if !exists {
		return fmt.Errorf("consumer group %s not found", groupName)
	}

	if !cg.Heartbeat(instanceID) {
		return fmt.Errorf("instance %s not registered", instanceID)
	}

	go b.saveConsumerGroup(topicName, groupName)
	return nil
}

func (b *Broker) Replay(topicName, groupName string, partitionOffsets map[int]int64) error {
	cg, exists := b.consumerGroups[topicName][groupName]
	if !exists {
		return fmt.Errorf("consumer group %s not found", groupName)
	}

	cg.SetAllOffsets(partitionOffsets)
	go b.saveConsumerGroup(topicName, groupName)
	return nil
}

func (b *Broker) GetConsumerGroup(topicName, groupName string) (*models.ConsumerGroup, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	topicGroups, exists := b.consumerGroups[topicName]
	if !exists {
		return nil, false
	}
	cg, exists := topicGroups[groupName]
	return cg, exists
}

func (b *Broker) GetConsumerGroups(topicName string) []*models.ConsumerGroup {
	b.mu.RLock()
	defer b.mu.RUnlock()

	topicGroups, exists := b.consumerGroups[topicName]
	if !exists {
		return nil
	}

	groups := make([]*models.ConsumerGroup, 0, len(topicGroups))
	for _, cg := range topicGroups {
		groups = append(groups, cg)
	}
	return groups
}

func (b *Broker) GetInFlightCount(topicName string) int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return int64(len(b.inFlight[topicName]))
}

func (b *Broker) GetDLQMessages(topicName string) []*models.Message {
	dlq, exists := b.storage.GetDLQ(topicName)
	if !exists {
		return nil
	}
	return dlq.List()
}

func (b *Broker) ResendDLQMessage(topicName, msgID string) error {
	dlq, exists := b.storage.GetDLQ(topicName)
	if !exists {
		return fmt.Errorf("DLQ not found for topic %s", topicName)
	}

	msg, err := dlq.Remove(msgID)
	if err != nil {
		return err
	}

	msg.RetryCount = 0
	msg.TransitionsState(models.StatePending, "admin")
	msg.VisibilityUntil = 0
	msg.ConsumerGroup = ""
	msg.ConsumerInstance = ""

	offset, err := b.storage.AppendMessage(topicName, msg.Partition, msg)
	if err != nil {
		return err
	}
	msg.Offset = offset

	b.notifyLongPollWaiters(topicName, msg)

	return nil
}

func (b *Broker) GetMetrics() *models.MetricsStore {
	return b.metrics
}

func (b *Broker) RefreshMetrics() {
	topics := b.storage.GetAllTopics()

	for _, topic := range topics {
		topicName := topic.Config.Name
		totalMsgs := b.storage.GetTotalMessages(topicName)
		inFlight := b.GetInFlightCount(topicName)
		dlq, _ := b.storage.GetDLQ(topicName)
		dlqCount := int64(0)
		if dlq != nil {
			dlqCount = int64(dlq.Count())
		}

		publishDiff := totalMsgs - b.lastPublishCount[topicName]
		b.lastPublishCount[topicName] = totalMsgs

		groups := b.GetConsumerGroups(topicName)
		var totalLag int64
		var consumeDiff int64
		for _, cg := range groups {
			offsets := cg.GetAllOffsets()
			partitionLags := make(map[int]int64)
			var groupLag int64
			var inFlightCount int64
			for p, offset := range offsets {
				nextOffset := b.storage.GetPartitionNextOffset(topicName, p)
				lag := nextOffset - offset
				if lag < 0 {
					lag = 0
				}
				partitionLags[p] = lag
				groupLag += lag
			}

			inFlightCount = cg.GetTotalInFlight()
			instanceCount := cg.GetInstanceCount()

			totalLag += groupLag

			b.metrics.UpdateConsumerGroupMetrics(cg.Name, topicName, groupLag, partitionLags, instanceCount, inFlightCount)
		}

		consumeDiff = publishDiff - inFlight
		if consumeDiff < 0 {
			consumeDiff = 0
		}

		b.metrics.UpdateTopicMetrics(topicName, totalMsgs, inFlight, dlqCount, publishDiff, consumeDiff)

		if tm, exists := b.metrics.GetTopicMetrics(topicName); exists {
			tm.TotalLag = totalLag
		}
	}
}

func (b *Broker) ProcessDeadInstances() {
	b.mu.RLock()
	topics := make([]string, 0, len(b.consumerGroups))
	for t := range b.consumerGroups {
		topics = append(topics, t)
	}
	b.mu.RUnlock()

	for _, topicName := range topics {
		b.mu.RLock()
		groups := make([]string, 0, len(b.consumerGroups[topicName]))
		for g := range b.consumerGroups[topicName] {
			groups = append(groups, g)
		}
		b.mu.RUnlock()

		for _, groupName := range groups {
			cg, exists := b.GetConsumerGroup(topicName, groupName)
			if !exists {
				continue
			}

			dead := cg.CheckDeadInstances(config.AppConfig.HeartbeatTimeout)
			for _, instanceID := range dead {
				log.Printf("INFO: Instance %s in group %s/%s timed out, releasing in-flight messages", instanceID, topicName, groupName)

				b.mu.Lock()
				for msgID, msg := range b.inFlight[topicName] {
					if msg.ConsumerInstance == instanceID {
						msg.TransitionsState(models.StatePending, "system")
						msg.VisibilityUntil = 0
						delete(b.inFlight[topicName], msgID)
					}
				}
				b.mu.Unlock()

				cg.RemoveInstance(instanceID)
				go b.saveConsumerGroup(topicName, groupName)
			}
		}
	}
}

func (b *Broker) CheckVisibilityTimeouts() {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now().UnixMilli()
	for _, msgs := range b.inFlight {
		for msgID, msg := range msgs {
			if msg.VisibilityUntil <= now {
				log.Printf("INFO: Message %s visibility timeout, releasing", msgID)
				msg.TransitionsState(models.StatePending, "system")
				msg.VisibilityUntil = 0
				delete(msgs, msgID)
			}
		}
	}
}

func (b *Broker) ProcessDelayQueue() {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now().UnixMilli()
	for b.delayQueue.Len() > 0 {
		dm := (*b.delayQueue)[0]
		if dm.AvailableAt > now {
			break
		}

		dm = heap.Pop(b.delayQueue).(*DelayMessage)
		msg := dm.Message
		msg.AvailableAt = now

		offset, err := b.storage.AppendMessage(msg.Topic, msg.Partition, msg)
		if err != nil {
			log.Printf("ERROR: Failed to append delayed message: %v", err)
			continue
		}
		msg.Offset = offset

		b.notifyLongPollWaiters(msg.Topic, msg)
		log.Printf("INFO: Delayed message %s now available", msg.ID)
	}
}

func (b *Broker) CleanupExpired() {
	topics := b.storage.GetAllTopics()
	for _, topic := range topics {
		deleted, err := b.storage.CleanupExpired(topic.Config.Name, topic.RetentionDuration())
		if err != nil {
			log.Printf("ERROR: Cleanup expired for %s: %v", topic.Config.Name, err)
		} else if deleted > 0 {
			log.Printf("INFO: Cleaned up %d expired messages from %s", deleted, topic.Config.Name)
		}

		dlq, exists := b.storage.GetDLQ(topic.Config.Name)
		if exists {
			count, warnings, err := dlq.CleanupExpired(30)
			if err != nil {
				log.Printf("ERROR: Cleanup DLQ for %s: %v", topic.Config.Name, err)
			} else if count > 0 {
				for _, w := range warnings {
					log.Printf("ALERT: DLQ message cleaned up after 30 days - %s", w)
				}
			}
		}
	}
}

func (b *Broker) Close() {
	b.storage.Close()
}
