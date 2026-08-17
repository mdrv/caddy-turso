package caddyturso

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// RequestRecord is one row of the _requests table.
type RequestRecord struct {
	TS        string
	IP        string
	Method    string
	Host      string
	Path      string
	Query     string
	Status    int
	LatencyMs int64
	BytesSent int64
	UserAgent string
}

// BatchWriter buffers request records and flushes them in batches.
// buffer_size bounds TOTAL pending records (channel + batch buffer):
// with overflow=drop excess records are discarded and counted; with
// overflow=block producers wait for space (backpressure).
type BatchWriter struct {
	// db is swapped atomically when the database is restored in place.
	db        atomic.Pointer[sql.DB]
	ch        chan RequestRecord
	batchSize int
	capacity  int    // total pending bound: len(ch) + len(buf)
	overflow  string // drop|block
	dropped   atomic.Int64
	flushTick *time.Ticker
	mu        sync.Mutex
	buf       []RequestRecord
	wg        sync.WaitGroup
	stopCh    chan struct{}
	logger    *zap.Logger
}

func NewBatchWriter(db *sql.DB, batchSize int, flushInterval time.Duration, bufferSize int, overflow string, logger *zap.Logger) *BatchWriter {
	w := &BatchWriter{
		ch:        make(chan RequestRecord, bufferSize),
		batchSize: batchSize,
		capacity:  bufferSize,
		overflow:  overflow,
		flushTick: time.NewTicker(flushInterval),
		buf:       make([]RequestRecord, 0, batchSize),
		stopCh:    make(chan struct{}),
		logger:    logger,
	}
	w.db.Store(db)
	w.wg.Add(1)
	go w.run()
	return w
}

// SetDB points the writer at a new database handle (used after an
// in-place restore).
func (w *BatchWriter) SetDB(db *sql.DB) {
	w.db.Store(db)
}

// Flush writes any pending records to the database immediately.
func (w *BatchWriter) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.flush()
}

// Write enqueues a record. With overflow=drop (default) it never blocks:
// when the buffer is full the record is discarded and counted. With
// overflow=block it waits for space (backpressure), escaping only on
// shutdown so Stop() can always make progress.
func (w *BatchWriter) Write(r RequestRecord) {
	if w.overflow == "block" {
		select {
		case w.ch <- r:
		case <-w.stopCh:
			w.dropped.Add(1)
		}
		return
	}
	select {
	case w.ch <- r:
	default:
		w.dropped.Add(1)
	}
}

// Buffered returns the number of pending records (channel + batch).
func (w *BatchWriter) Buffered() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.ch) + len(w.buf)
}

// Dropped returns how many records were discarded due to a full buffer
// (or shutdown during a blocking write).
func (w *BatchWriter) Dropped() int64 {
	return w.dropped.Load()
}

func (w *BatchWriter) Stop() {
	close(w.stopCh)
	w.flushTick.Stop()
	w.wg.Wait()
}

func (w *BatchWriter) run() {
	defer w.wg.Done()
	for {
		// Only drain the channel while the total pending count is under
		// capacity; otherwise let the channel back up so Write() sees
		// pressure and applies the overflow policy (drop or block).
		var recv chan RequestRecord
		w.mu.Lock()
		if len(w.buf) < w.capacity {
			recv = w.ch
		}
		w.mu.Unlock()
		select {
		case r := <-recv:
			w.mu.Lock()
			w.buf = append(w.buf, r)
			if len(w.buf) >= w.batchSize {
				w.flush()
			}
			w.mu.Unlock()
		case <-w.flushTick.C:
			w.mu.Lock()
			if len(w.buf) > 0 {
				w.flush()
			}
			w.mu.Unlock()
		case <-w.stopCh:
			w.drain()
			return
		}
	}
}

// drain flushes whatever is pending and empties the channel.
func (w *BatchWriter) drain() {
	for {
		select {
		case r := <-w.ch:
			w.buf = append(w.buf, r)
		default:
			w.mu.Lock()
			w.flush()
			w.mu.Unlock()
			return
		}
	}
}

func (w *BatchWriter) flush() {
	if len(w.buf) == 0 {
		return
	}

	var sb strings.Builder
	sb.WriteString(`INSERT INTO _requests (ts, ip, method, host, path, query, status, latency_ms, bytes_sent, user_agent) VALUES `)

	args := make([]any, 0, len(w.buf)*10)
	for i, r := range w.buf {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(?,?,?,?,?,?,?,?,?,?)")
		args = append(args,
			r.TS, r.IP, r.Method, r.Host, r.Path,
			r.Query, r.Status, r.LatencyMs, r.BytesSent, r.UserAgent,
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	_, err := w.db.Load().ExecContext(ctx, sb.String(), args...)
	cancel()
	if err != nil {
		w.logger.Error("turso: request log batch write failed",
			zap.Int("batch_size", len(w.buf)), zap.Error(err))
	}

	w.buf = w.buf[:0]
}
