package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// ─────────────────────────────────────────────
// Job types
// ─────────────────────────────────────────────

type JobStatus string

const (
	StatusPending   JobStatus = "pending"
	StatusRunning   JobStatus = "running"
	StatusDone      JobStatus = "done"
	StatusFailed    JobStatus = "failed"
	StatusCancelled JobStatus = "cancelled"
)

type Job struct {
	ID        string        `json:"id"`
	Payload   string        `json:"payload"`
	Status    JobStatus     `json:"status"`
	Result    string        `json:"result,omitempty"`
	Error     string        `json:"error,omitempty"`
	CreatedAt time.Time     `json:"created_at"`
	StartedAt *time.Time    `json:"started_at,omitempty"`
	FinishedAt *time.Time   `json:"finished_at,omitempty"`
	Duration  string        `json:"duration,omitempty"`
}

// ─────────────────────────────────────────────
// Metrics
// ─────────────────────────────────────────────

type Metrics struct {
	TotalSubmitted  int64 `json:"total_submitted"`
	TotalCompleted  int64 `json:"total_completed"`
	TotalFailed     int64 `json:"total_failed"`
	TotalCancelled  int64 `json:"total_cancelled"`
	ActiveWorkers   int64 `json:"active_workers"`
	QueueDepth      int   `json:"queue_depth"`
	Goroutines      int   `json:"goroutines"`
}

// ─────────────────────────────────────────────
// Dispatcher — the core of the system
// ─────────────────────────────────────────────

type Dispatcher struct {
	workers    int
	jobQueue   chan *Job
	jobs       sync.Map          // map[string]*Job
	cancels    sync.Map          // map[string]context.CancelFunc
	wg         sync.WaitGroup

	submitted  atomic.Int64
	completed  atomic.Int64
	failed     atomic.Int64
	cancelled  atomic.Int64
	active     atomic.Int64

	ctx        context.Context
	shutdown   context.CancelFunc
}

func NewDispatcher(workers, queueSize int) *Dispatcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &Dispatcher{
		workers:  workers,
		jobQueue: make(chan *Job, queueSize),
		ctx:      ctx,
		shutdown: cancel,
	}
}

func (d *Dispatcher) Start() {
	for i := 0; i < d.workers; i++ {
		d.wg.Add(1)
		go d.runWorker(i)
	}
	log.Printf("dispatcher started with %d workers", d.workers)
}

func (d *Dispatcher) Stop() {
	d.shutdown()
	d.wg.Wait()
	log.Println("dispatcher stopped cleanly")
}

func (d *Dispatcher) Submit(payload string) (*Job, error) {
	job := &Job{
		ID:        fmt.Sprintf("job-%d", time.Now().UnixNano()),
		Payload:   payload,
		Status:    StatusPending,
		CreatedAt: time.Now(),
	}

	// register before queuing so Get() always finds it
	d.jobs.Store(job.ID, job)
	d.submitted.Add(1)

	select {
	case d.jobQueue <- job:
		return job, nil
	default:
		job.Status = StatusFailed
		job.Error = "queue full"
		d.failed.Add(1)
		return job, fmt.Errorf("queue full")
	}
}

func (d *Dispatcher) Cancel(jobID string) bool {
	if fn, ok := d.cancels.Load(jobID); ok {
		fn.(context.CancelFunc)()
		return true
	}
	// job may still be pending in queue
	if v, ok := d.jobs.Load(jobID); ok {
		job := v.(*Job)
		if job.Status == StatusPending {
			job.Status = StatusCancelled
			d.cancelled.Add(1)
			return true
		}
	}
	return false
}

func (d *Dispatcher) Get(jobID string) (*Job, bool) {
	v, ok := d.jobs.Load(jobID)
	if !ok {
		return nil, false
	}
	return v.(*Job), true
}

func (d *Dispatcher) Metrics() Metrics {
	depth := len(d.jobQueue)
	return Metrics{
		TotalSubmitted: d.submitted.Load(),
		TotalCompleted: d.completed.Load(),
		TotalFailed:    d.failed.Load(),
		TotalCancelled: d.cancelled.Load(),
		ActiveWorkers:  d.active.Load(),
		QueueDepth:     depth,
		Goroutines:     runtime.NumGoroutine(),
	}
}

// ─────────────────────────────────────────────
// Worker loop — THIS is the goroutine-leak fix.
// Every job gets its own child context. If the
// job is cancelled mid-flight, ctx.Done() fires
// and the goroutine exits immediately.
// ─────────────────────────────────────────────

func (d *Dispatcher) runWorker(id int) {
	defer d.wg.Done()
	log.Printf("worker %d ready", id)

	for {
		select {
		case <-d.ctx.Done():
			log.Printf("worker %d shutting down", id)
			return

		case job, ok := <-d.jobQueue:
			if !ok {
				return
			}
			if job.Status == StatusCancelled {
				continue // was cancelled while pending
			}

			// per-job context so we can cancel individually
			jobCtx, cancel := context.WithTimeout(d.ctx, 30*time.Second)
			d.cancels.Store(job.ID, cancel)

			d.active.Add(1)
			now := time.Now()
			job.StartedAt = &now
			job.Status = StatusRunning

			result, err := processJob(jobCtx, job)

			finished := time.Now()
			job.FinishedAt = &finished
			job.Duration = finished.Sub(now).Round(time.Millisecond).String()

			if err != nil {
				if jobCtx.Err() == context.Canceled {
					job.Status = StatusCancelled
					d.cancelled.Add(1)
				} else {
					job.Status = StatusFailed
					job.Error = err.Error()
					d.failed.Add(1)
				}
			} else {
				job.Status = StatusDone
				job.Result = result
				d.completed.Add(1)
			}

			d.active.Add(-1)
			cancel()
			d.cancels.Delete(job.ID)
		}
	}
}

// ─────────────────────────────────────────────
// The actual "work" — simulates CPU/IO with
// proper cancellation support via ctx.Done()
// ─────────────────────────────────────────────

func processJob(ctx context.Context, job *Job) (string, error) {
	// simulate variable duration work (1-5 seconds)
	duration := time.Duration(1+rand.Intn(4)) * time.Second
	ticker := time.NewTicker(200 * time.Millisecond)
	deadline := time.NewTimer(duration)
	defer ticker.Stop()
	defer deadline.Stop()

	steps := 0
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline.C:
			return fmt.Sprintf("processed '%s' in %d steps", job.Payload, steps), nil
		case <-ticker.C:
			steps++
			// simulate occasional failure
			if rand.Intn(20) == 0 {
				return "", fmt.Errorf("transient error at step %d", steps)
			}
		}
	}
}

// ─────────────────────────────────────────────
// HTTP API
// ─────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func main() {
	d := NewDispatcher(8, 256)
	d.Start()

	mux := http.NewServeMux()

	// POST /jobs  {"payload": "some work"}
	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Payload string `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Payload == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "payload required"})
			return
		}
		job, err := d.Submit(body.Payload)
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusAccepted, job)
	})

	// GET /jobs/{id}
	mux.HandleFunc("GET /jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		job, ok := d.Get(id)
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
			return
		}
		writeJSON(w, http.StatusOK, job)
	})

	// DELETE /jobs/{id}  — cancel
	mux.HandleFunc("DELETE /jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if d.Cancel(id) {
			writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
		} else {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found or already finished"})
		}
	})

	// GET /metrics
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, d.Metrics())
	})

	// GET /health
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	srv := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 35 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Println("API listening on :8080")
	log.Fatal(srv.ListenAndServe())
}