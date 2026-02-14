package LSM

import (
	"context"
	"os"
	"sync"

	gw "github.com/JyotinderSingh/go-wal"
)

const (
	WALDirectorySuffix = "_wal"
	maxLevels          = 6
)

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

		level := l.getLevelFromSSTableFileName(sstable.file.Name())
		l.levels[level].sstables = append(l.levels[level].sstables, sstable)
	}

	return nil
}
