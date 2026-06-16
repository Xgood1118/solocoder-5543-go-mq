package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/solomq/internal/models"
)

type AppendOnlyLog struct {
	topic     string
	partition int
	dataDir   string
	file      *os.File
	indexFile *os.File
	mu        sync.RWMutex
	nextOffset int64
	index     map[int64]int64
	dirty     bool
}

type IndexEntry struct {
	Offset   int64
	Position int64
	Length   int32
}

func NewAppendOnlyLog(dataDir, topic string, partition int) (*AppendOnlyLog, error) {
	topicDir := filepath.Join(dataDir, "topics", topic, fmt.Sprintf("partition-%d", partition))
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		return nil, fmt.Errorf("create topic dir: %w", err)
	}

	logPath := filepath.Join(topicDir, "log.dat")
	indexPath := filepath.Join(topicDir, "index.dat")

	logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}

	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("open index file: %w", err)
	}

	alog := &AppendOnlyLog{
		topic:     topic,
		partition: partition,
		dataDir:   dataDir,
		file:      logFile,
		indexFile: indexFile,
		index:     make(map[int64]int64),
	}

	if err := alog.loadIndex(); err != nil {
		return nil, fmt.Errorf("load index: %w", err)
	}

	return alog, nil
}

func (l *AppendOnlyLog) loadIndex() error {
	l.indexFile.Seek(0, 0)
	stat, err := l.indexFile.Stat()
	if err != nil {
		return err
	}
	size := stat.Size()
	if size == 0 {
		return nil
	}

	buf := make([]byte, size)
	if _, err := l.indexFile.Read(buf); err != nil {
		return err
	}

	for i := int64(0); i < size; i += 20 {
		if i+20 > size {
			break
		}
		offset := binary.BigEndian.Uint64(buf[i : i+8])
		position := binary.BigEndian.Uint64(buf[i+8 : i+16])
		length := binary.BigEndian.Uint32(buf[i+16 : i+20])
		l.index[int64(offset)] = int64(position)<<32 | int64(length)
		if int64(offset)+1 > l.nextOffset {
			l.nextOffset = int64(offset) + 1
		}
	}

	return nil
}

func (l *AppendOnlyLog) Append(msg *models.Message) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	data, err := msg.ToJSON()
	if err != nil {
		return -1, err
	}

	length := int32(len(data))
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(length))

	position, err := l.file.Seek(0, os.SEEK_END)
	if err != nil {
		return -1, err
	}

	if _, err := l.file.Write(header); err != nil {
		return -1, err
	}
	if _, err := l.file.Write(data); err != nil {
		return -1, err
	}
	if err := l.file.Sync(); err != nil {
		return -1, err
	}

	offset := l.nextOffset
	l.nextOffset++

	l.index[offset] = position<<32 | int64(length)
	l.dirty = true

	idxEntry := make([]byte, 20)
	binary.BigEndian.PutUint64(idxEntry[0:8], uint64(offset))
	binary.BigEndian.PutUint64(idxEntry[8:16], uint64(position))
	binary.BigEndian.PutUint32(idxEntry[16:20], uint32(length))
	if _, err := l.indexFile.Write(idxEntry); err != nil {
		return -1, err
	}
	l.indexFile.Sync()

	return offset, nil
}

func (l *AppendOnlyLog) Read(offset int64) (*models.Message, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	posLen, exists := l.index[offset]
	if !exists {
		return nil, fmt.Errorf("offset %d not found", offset)
	}
	position := posLen >> 32
	length := int32(posLen & 0xFFFFFFFF)

	data := make([]byte, length)
	if _, err := l.file.ReadAt(data, position+4); err != nil {
		return nil, err
	}

	return models.MessageFromJSON(data)
}

func (l *AppendOnlyLog) Scan(startOffset int64, maxCount int, filter func(*models.Message) bool) ([]*models.Message, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var messages []*models.Message
	count := 0

	for offset := startOffset; offset < l.nextOffset && count < maxCount; offset++ {
		msg, err := l.Read(offset)
		if err != nil {
			continue
		}
		if filter == nil || filter(msg) {
			messages = append(messages, msg)
			count++
		}
	}

	return messages, nil
}

func (l *AppendOnlyLog) NextOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.nextOffset
}

func (l *AppendOnlyLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file != nil {
		l.file.Close()
	}
	if l.indexFile != nil {
		l.indexFile.Close()
	}
	return nil
}

func (l *AppendOnlyLog) CleanupExpired(retention time.Duration) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	cutoff := time.Now().Add(-retention).UnixMilli()
	var deleted int64

	newIndex := make(map[int64]int64)
	for offset, posLen := range l.index {
		msg, err := l.Read(offset)
		if err != nil {
			continue
		}
		if msg.CreatedAt >= cutoff {
			newIndex[offset] = posLen
		} else {
			deleted++
		}
	}

	if deleted > 0 {
		l.index = newIndex
		l.dirty = true
	}

	return deleted, nil
}

func (l *AppendOnlyLog) RewriteIndex() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, err := l.indexFile.Seek(0, 0); err != nil {
		return err
	}
	if err := l.indexFile.Truncate(0); err != nil {
		return err
	}

	for offset, posLen := range l.index {
		position := posLen >> 32
		length := int32(posLen & 0xFFFFFFFF)
		idxEntry := make([]byte, 20)
		binary.BigEndian.PutUint64(idxEntry[0:8], uint64(offset))
		binary.BigEndian.PutUint64(idxEntry[8:16], uint64(position))
		binary.BigEndian.PutUint32(idxEntry[16:20], uint32(length))
		if _, err := l.indexFile.Write(idxEntry); err != nil {
			return err
		}
	}

	return l.indexFile.Sync()
}
