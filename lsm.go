package LSM

import (
	"context"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	gw "github.com/JyotinderSingh/go-wal"
)

const (
	SSTableFilePrefix  = "sstable_"
	WALDirectorySuffix = "_wal"
	maxLevels          = 6 // Maximum number of levels in the LSMTree.
)

// Maximum number of SSTables in each level before compaction is triggered.
var maxLevelSSTables = map[int]int{
	0: 4,
	1: 8,
	2: 16,
	3: 32,
	4: 64,
	5: 128,
	6: 256,
}

type level struct {
	sstables []*SSTable
	mu       sync.RWMutex
}

type LSMTree struct {
	memtable             *Memtable
	mu                   sync.RWMutex
	maxMemtableSize      int64
	directory            string
	wal                  *gw.WAL
	inRecovery           bool
	levels               []*level
	current_sst_sequence uint64
	compactionChan       chan int
	flushingQueue        []*Memtable
	flushingQueueMu      sync.RWMutex
	flushingChan         chan *Memtable
	ctx                  context.Context
	cancel               context.CancelFunc
	wg                   sync.WaitGroup
}

func Open(directory string, maxMemtableSize int64, recoverFromWAL bool) (*LSMTree, error) {
	ctx, cancel := context.WithCancel(context.Background())

	wal, err := gw.OpenWAL(directory+WALDirectorySuffix, true, 128000, 1000)
	if err != nil {
		cancel()
		return nil, err
	}

	levels := make([]*level, maxLevels)

	for i := 0; i < maxLevels; i++ {
		levels[i] = &level{
			sstables: make([]*SSTable, 0),
		}
	}

	lsm := &LSMTree{
		memtable:             NewMemtable(),
		maxMemtableSize:      maxMemtableSize,
		directory:            directory,
		wal:                  wal,
		inRecovery:           recoverFromWAL,
		levels:               levels,
		current_sst_sequence: 0,
		compactionChan:       make(chan int, 100),
		flushingQueue:        make([]*Memtable, 0),
		flushingChan:         make(chan *Memtable, 100),
		ctx:                  ctx,
		cancel:               cancel,
	}

	if err := lsm.loadSSTables(); err != nil {
		return nil, err
	}

	lsm.wg.Add(2)
	go lsm.backgroundCompaction()
	go lsm.backgroundMemtableFlushing()

	if recoverFromWAL {
		if err := lsm.recoverFromWAL(); err != nil {
			return nil, err
		}
	}

	return lsm, nil
}

func (l *LSMTree) loadSSTables() error {
	if err := os.MkdirAll(l.directory, 0755); err != nil {
		return err
	}

	if err := l.loadSSTablesFromDisk(); err != nil {
		return err
	}

	l.sortSSTablesBySequenceNumber()
	l.initializeCurrentSequenceNumber()

	return nil
}

func (l *LSMTree) loadSSTablesFromDisk() error {
	files, err := os.ReadDir(l.directory)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() || !isSSTableFile(file.Name()) {
			continue
		}

		sstable, err := OpenSSTable(l.directory + "/" + file.Name())
		if err != nil {
			return err
		}

		// Did not get it
		level := l.getLevelFromSSTableFileName(sstable.file.Name())
		l.levels[level].sstables = append(l.levels[level].sstables, sstable)
	}

	return nil
}

func (l *LSMTree) getLevelFromSSTableFileName(fileName string) int {
	levelStr := fileName[len(l.directory)+1+len(SSTableFilePrefix) : len(l.directory)+2+len(SSTableFilePrefix)]
	level, err := strconv.Atoi(levelStr)
	if err != nil {
		panic(err)
	}

	return level
}

func (l *LSMTree) sortSSTablesBySequenceNumber() {
	for _, level := range l.levels {
		sort.Slice(level.sstables, func(i, j int) bool {
			iSequence := l.getSequenceNumber(level.sstables[i].file.Name())
			jSequence := l.getSequenceNumber(level.sstables[j].file.Name())
			return iSequence < jSequence
		})
	}
}

func (l *LSMTree) Put(key string, value []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Update WAL
	if !l.inRecovery {
		l.wal.WriteEntry(mustMarshal(&WALEntry{
			Key:       key,
			Command:   Command_PUT,
			Value:     value,
			Timestamp: time.Now().UnixNano(),
		}))
	}

	l.memtable.Put(key, value)

	if l.memtable.size > l.maxMemtableSize {
		l.flushingQueueMu.Lock()
		l.flushingQueue = append(l.flushingQueue, l.memtable)
		l.flushingQueueMu.Unlock()

		l.flushingChan <- l.memtable
		l.memtable = NewMemtable()
	}

	return nil
}

func (l *LSMTree) Delete(key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.inRecovery {
		l.wal.WriteEntry(mustMarshal(&WALEntry{
			Key:       key,
			Command:   Command_DELETE,
			Timestamp: time.Now().UnixNano(),
		}))
	}

	l.memtable.Delete(key)

	if l.memtable.size > l.maxMemtableSize {
		l.flushingQueueMu.Lock()
		l.flushingQueue = append(l.flushingQueue, l.memtable)
		l.flushingQueueMu.Unlock()

		l.flushingChan <- l.memtable
		l.memtable = NewMemtable()
	}

	return nil
}

func (l *LSMTree) Get(key string) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	value := l.memtable.Get(key)

	if value != nil {
		l.mu.RUnlock()
		return handleValue(value)
	}
	l.mu.RUnlock()

	l.flushingQueueMu.RLock()
	for i := len(l.flushingQueue) - 1; i >= 0; i-- {
		value := l.flushingQueue[i].Get(key)
		if value != nil {
			l.flushingQueueMu.RUnlock()
			return handleValue(value)
		}
	}
	l.flushingQueueMu.RUnlock()

	for level := range l.levels {
		l.levels[level].mu.RLock()

		for i := len(l.levels[level].sstables) - 1; i >= 0; i-- {
			value, err := l.levels[level].sstables[i].Get(key)

			if err != nil {
				l.levels[level].mu.RUnlock()
				return nil, err
			}

			if value != nil {
				l.levels[level].mu.RUnlock()
				return handleValue(value)
			}
		}

		l.levels[level].mu.RUnlock()
	}

	return nil, nil
}

func (l *LSMTree) backgroundCompaction() error {
	defer l.wg.Done()
	for {
		select {
		case <-l.ctx.Done():
			if readyToExit := l.checkAndTriggerCompaction(); readyToExit {
				return nil
			}
		case compactionCandidate := <-l.compactionChan:
			l.compactLevel(compactionCandidate)
		}
	}
}

func (l *LSMTree) checkAndTriggerCompaction() bool {
	readyToExit := true
	for idx, level := range l.levels {
		level.mu.RLock()
		if len(level.sstables) > maxLevelSSTables[idx] {
			l.compactionChan <- idx
			readyToExit = false
		}
		level.mu.RUnlock()
	}

	return readyToExit
}

func (l *LSMTree) compactLevel(compactionCandidate int) error {
	if compactionCandidate == maxLevels-1 {
		// Didnt understand this part
		return nil
	}

	l.levels[compactionCandidate].mu.RLock()

	if len(l.levels[compactionCandidate].sstables) < maxLevelSSTables[idx] {
		l.levels[compactionCandidate].mu.RUnlock()
		return nil
	}

	_, iterators := l.getSSTableHandlesAtLevel(compactionCandidate)

	l.levels[compactionCandidate].mu.RUnlock()

	mergedSSTable, err := l.mergeSSTables(iterators, compactionCandidate+1)
	if err != nil {
		return err
	}

	l.levels[compactionCandidate].mu.Lock()
	l.levels[compactionCandidate+1].mu.Lock()

	l.deleteSSTablesAtLevel(compactionCandidate, iterators)

	l.addSSTableToLevel(mergedSSTable, compactionCandidate+1)

	l.levels[compactionCandidate].mu.Unlock()
	l.levels[compactionCandidate+1].mu.Unlock()

	return nil
}

func (l *LSMTree) getSSTableHandlesAtLevel(level int) ([]*SSTable, []*SSTableIterator) {
	sstables := l.levels[level].sstables
	iterators := make([]*SSTableIterator, len(sstables))
	for i, sstable := range sstables {
		// TODO
		iterators[i] = &SSTableIterator{
			s:     sstable,
			file:  sstable.file,
			Value: nil,
		}
	}
	return sstables, iterators
}

// ----------------- utilities -----------------

func handleValue(value *LSMEntry) ([]byte, error) {
	if value.Command == Command_DELETE {
		return nil, nil
	}

	return value.Value, nil
}

func isSSTableFile(fileName string) bool {
	return fileName[:len(SSTableFilePrefix)] == SSTableFilePrefix
}
