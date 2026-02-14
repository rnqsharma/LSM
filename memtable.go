package LSM

import (
	"time"

	"github.com/huandu/skiplist"
)

type Memtable struct {
	data skiplist.SkipList
	size int64
}

func NewMemtable() *Memtable {
	return &Memtable{
		data: *skiplist.New(skiplist.String),
		size: 0,
	}
}

func (m *Memtable) Put(key string, value []byte) {
	sizeChange := int64(len(value))
	existingEntry := m.data.Get(key)

	if existingEntry != nil {
		m.size -= int64(len(existingEntry.Value.(*LSMEntry).Value))
	} else {
		sizeChange += int64(len(key))
	}

	entry := getLSMEntry(key, &value, Command_PUT)
	m.data.Set(key, entry)
	m.size += sizeChange
}

func getLSMEntry(key string, value *[]byte, command Command) *LSMEntry {
	entry := &LSMEntry{
		Key:       key,
		Command:   command,
		Timestamp: time.Now().UnixNano(),
	}
	if value != nil {
		entry.Value = *value
	}

	return entry
}
