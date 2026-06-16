package models

import (
	"sync"
	"time"
)

type ConsumerGroup struct {
	Name      string              `json:"name"`
	Topic     string              `json:"topic"`
	Instances map[string]*ConsumerInstance `json:"instances"`
	Offsets   map[int]int64       `json:"offsets"`
	mu        sync.RWMutex
}

type ConsumerInstance struct {
	ID            string `json:"id"`
	LastHeartbeat int64  `json:"last_heartbeat"`
	InFlightCount int    `json:"in_flight_count"`
}

func NewConsumerGroup(name, topic string, partitions int) *ConsumerGroup {
	offsets := make(map[int]int64)
	for i := 0; i < partitions; i++ {
		offsets[i] = 0
	}
	return &ConsumerGroup{
		Name:      name,
		Topic:     topic,
		Instances: make(map[string]*ConsumerInstance),
		Offsets:   offsets,
	}
}

func (cg *ConsumerGroup) RegisterInstance(instanceID string) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	cg.Instances[instanceID] = &ConsumerInstance{
		ID:            instanceID,
		LastHeartbeat: time.Now().UnixMilli(),
		InFlightCount: 0,
	}
}

func (cg *ConsumerGroup) Heartbeat(instanceID string) bool {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	inst, exists := cg.Instances[instanceID]
	if !exists {
		return false
	}
	inst.LastHeartbeat = time.Now().UnixMilli()
	return true
}

func (cg *ConsumerGroup) CheckDeadInstances(timeout time.Duration) []string {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	var dead []string
	now := time.Now().UnixMilli()
	for id, inst := range cg.Instances {
		if now-inst.LastHeartbeat > timeout.Milliseconds() {
			dead = append(dead, id)
		}
	}
	return dead
}

func (cg *ConsumerGroup) RemoveInstance(instanceID string) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	delete(cg.Instances, instanceID)
}

func (cg *ConsumerGroup) GetOffset(partition int) int64 {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	return cg.Offsets[partition]
}

func (cg *ConsumerGroup) SetOffset(partition int, offset int64) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	cg.Offsets[partition] = offset
}

func (cg *ConsumerGroup) GetAllOffsets() map[int]int64 {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	offsets := make(map[int]int64)
	for k, v := range cg.Offsets {
		offsets[k] = v
	}
	return offsets
}

func (cg *ConsumerGroup) SetAllOffsets(offsets map[int]int64) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	for k, v := range offsets {
		cg.Offsets[k] = v
	}
}

func (cg *ConsumerGroup) IncrementInFlight(instanceID string, delta int) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	if inst, exists := cg.Instances[instanceID]; exists {
		inst.InFlightCount += delta
	}
}

func (cg *ConsumerGroup) GetInstanceCount() int {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	return len(cg.Instances)
}

func (cg *ConsumerGroup) GetTotalInFlight() int64 {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	var total int64
	for _, inst := range cg.Instances {
		total += int64(inst.InFlightCount)
	}
	return total
}
