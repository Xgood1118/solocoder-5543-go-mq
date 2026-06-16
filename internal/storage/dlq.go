package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/solomq/internal/models"
)

type DLQStore struct {
	dataDir   string
	topic     string
	file      *os.File
	indexFile *os.File
	mu        sync.RWMutex
	messages  map[string]*models.Message
	nextIndex int64
}

func NewDLQStore(dataDir, topic string) (*DLQStore, error) {
	dlqDir := filepath.Join(dataDir, "topics", topic, "dlq")
	if err := os.MkdirAll(dlqDir, 0755); err != nil {
		return nil, err
	}

	logPath := filepath.Join(dlqDir, "log.dat")
	indexPath := filepath.Join(dlqDir, "index.dat")

	logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		logFile.Close()
		return nil, err
	}

	store := &DLQStore{
		dataDir:  dataDir,
		topic:    topic,
		file:     logFile,
		indexFile: indexFile,
		messages: make(map[string]*models.Message),
	}

	if err := store.load(); err != nil {
		return nil, err
	}

	return store, nil
}

func (s *DLQStore) load() error {
	s.indexFile.Seek(0, 0)
	stat, err := s.indexFile.Stat()
	if err != nil {
		return err
	}
	size := stat.Size()
	if size == 0 {
		return nil
	}

	buf := make([]byte, size)
	if _, err := s.indexFile.Read(buf); err != nil {
		return err
	}

	for i := int64(0); i < size; i += 24 {
		if i+24 > size {
			break
		}
		position := binary.BigEndian.Uint64(buf[i : i+8])
		length := binary.BigEndian.Uint32(buf[i+8 : i+12])
		msgIDLen := binary.BigEndian.Uint32(buf[i+12 : i+16])

		msgIDBuf := make([]byte, msgIDLen)
		if _, err := s.file.ReadAt(msgIDBuf, int64(position)); err != nil {
			continue
		}
		msgID := string(msgIDBuf)

		dataBuf := make([]byte, length)
		if _, err := s.file.ReadAt(dataBuf, int64(position)+int64(msgIDLen)); err != nil {
			continue
		}

		msg, err := models.MessageFromJSON(dataBuf)
		if err != nil {
			continue
		}
		s.messages[msgID] = msg
		s.nextIndex++
	}

	return nil
}

func (s *DLQStore) Add(msg *models.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	msgID := []byte(msg.ID)
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	position, err := s.file.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}

	msgIDLen := uint32(len(msgID))
	length := uint32(len(data))
	createdAt := uint64(time.Now().UnixMilli())

	if _, err := s.file.Write(msgID); err != nil {
		return err
	}
	if _, err := s.file.Write(data); err != nil {
		return err
	}
	if err := s.file.Sync(); err != nil {
		return err
	}

	idxEntry := make([]byte, 24)
	binary.BigEndian.PutUint64(idxEntry[0:8], uint64(position))
	binary.BigEndian.PutUint32(idxEntry[8:12], length)
	binary.BigEndian.PutUint32(idxEntry[12:16], msgIDLen)
	binary.BigEndian.PutUint64(idxEntry[16:24], createdAt)
	if _, err := s.indexFile.Write(idxEntry); err != nil {
		return err
	}
	s.indexFile.Sync()

	s.messages[msg.ID] = msg
	s.nextIndex++
	return nil
}

func (s *DLQStore) Remove(msgID string) (*models.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	msg, exists := s.messages[msgID]
	if !exists {
		return nil, fmt.Errorf("message %s not found in DLQ", msgID)
	}
	delete(s.messages, msgID)
	return msg, nil
}

func (s *DLQStore) Get(msgID string) (*models.Message, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	msg, exists := s.messages[msgID]
	return msg, exists
}

func (s *DLQStore) List() []*models.Message {
	s.mu.RLock()
	defer s.mu.RUnlock()
	msgs := make([]*models.Message, 0, len(s.messages))
	for _, msg := range s.messages {
		msgs = append(msgs, msg)
	}
	return msgs
}

func (s *DLQStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.messages)
}

func (s *DLQStore) CleanupExpired(retentionDays int) (int, []string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().AddDate(0, 0, -retentionDays).UnixMilli()
	var deletedIDs []string
	var deletedMessages []string

	for id, msg := range s.messages {
		lastState := msg.StateHistory[len(msg.StateHistory)-1]
		if lastState.Timestamp < cutoff {
			deletedIDs = append(deletedIDs, id)
			deletedMessages = append(deletedMessages,
				fmt.Sprintf("Topic: %s, MsgID: %s, LastState: %s, Age: %d hours",
					s.topic, id, lastState.To, (time.Now().UnixMilli()-lastState.Timestamp)/3600000))
		}
	}

	for _, id := range deletedIDs {
		delete(s.messages, id)
	}

	return len(deletedIDs), deletedMessages, nil
}

func (s *DLQStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		s.file.Close()
	}
	if s.indexFile != nil {
		s.indexFile.Close()
	}
	return nil
}
