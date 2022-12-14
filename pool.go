// Copyright (c) 2022 Ilya Toropchenko <eliastor@users.noreply.github.com>
//
// Use if this source code is covered by an MIT-style
// license that can be found in the LICENSE file

// Package proletarian providers worker pool with graceful shutdowns and retires of tasks
package proletarian

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
)

// Task is the interface that every task in pool must satisfy
type Task interface {
	// SetError sets error and increase error counter if err != nil
	SetError(error)
	// ErrorCount returns number of errors for the task
	ErrorCount() int

	// Err returns last error set by SetError
	Err() error

	// Unwrap is same as Err and returns last error set by SetError
	Unwrap() error
}

var (
	_ Task = &TaskHeader{}
)

// TaskHeader is structure that satisfy Task interface and intended to be embedded in user-defined tasks
type TaskHeader struct {
	retries int
	err     error
}

// SetError sets error in TaskHeader and increase error counter if err != nil
func (t *TaskHeader) SetError(err error) {
	t.err = err
	if err != nil {
		t.retries++
	}

}

// ErrorCount returns number of errors for the task
func (t *TaskHeader) ErrorCount() int {
	return t.retries
}

// Err returns last error occurred in task
func (t *TaskHeader) Err() error {
	return t.err
}

func (t *TaskHeader) Unwrap() error {
	return t.err
}

// PoolConfig includes configuration for the pool. All values are normalized to limits.
type PoolConfig struct {
	// LobbbySize sets size of input queue, default value is 0
	LobbySize int

	// Size sets size of workers, default value is 1, it is normalized to range [1 .. runtime.GOMAXPROCS(0) * 32]
	Size int

	// Retries limits number of retries for every task. Set this value to something bigger than 0
	Retries int

	// Func is function that will be executed in every worker
	Func func(t Task) error
}

// pool represents pool with input, worker and error queues. Each task landed in input queue is transported to worker queue.
// Worker queue is handled by workers and if worker function returned error it will be placed to worker queue again until hits retries limit.
// After that errored task will be sent to error queue where it must be read by user code.

type Pooler interface {
	// Run triggers start of the pool. Must be called only once, can be called without creating new goroutine.
	Run()

	// Queue puts new task in input queue.
	Queue(t Task)

	// Shutdown gracefully stops pool, waiting for all task will be finished (successfully or errored after retries). The pool must not be used after Shutdown
	Shutdown()

	// Cancel stops pool ungracefully. The pool must not be used after Cancel
	Cancel()

	// ErroredTask returns task with error. It waits while such task appears and returns the task or nil if pool was shut down and no more errored task available.
	ErroredTask() Task

	// Wait holds execution and waits until all tasks pool execution queue will be empty
	Wait()
}

var _ Pooler = &pool{}

type pool struct {
	inputQ     chan Task
	workersQ   chan Task
	errCh      chan Task
	workersWG  *sync.WaitGroup
	inflightWG *sync.WaitGroup
	inputLock  *sync.Mutex

	shutdownOnce sync.Once

	f        func(t Task) error
	ctx      context.Context
	cancel   context.CancelFunc
	inQ      atomic.Int32
	inWork   atomic.Int32
	inFlight atomic.Int32
	cfg      PoolConfig
}

// NewPool creates new Pool
func NewPool(ctx context.Context, cfg PoolConfig) Pooler {
	if cfg.Size < 1 {
		cfg.Size = 1
	}
	if cfg.Size > runtime.GOMAXPROCS(0)*32 {
		cfg.Size = runtime.GOMAXPROCS(0) * 32
	}
	if cfg.Retries < -1 {
		cfg.Retries = -1
	}

	p := &pool{
		f:          cfg.Func,
		workersWG:  new(sync.WaitGroup),
		inflightWG: new(sync.WaitGroup),
		inputQ:     make(chan Task, cfg.LobbySize),
		workersQ:   make(chan Task),
		errCh:      make(chan Task),
		inputLock:  new(sync.Mutex),
		cfg:        cfg,
	}
	p.ctx, p.cancel = context.WithCancel(ctx)

	return p
}

func (p *pool) lobby() {
	go func() {
		for task := range p.inputQ {
			p.inQ.Add(-1)
			p.workersQ <- task
		}
		p.inputQ = nil
		p.inputLock.Unlock()
	}()

}

// Run triggers start of the pool. Must be called only once, can be called without creating new goroutine.
func (p *pool) Run() {
	p.inputLock.Lock()
	go p.lobby()
	for i := 0; i < p.cfg.Size; i++ {
		p.workersWG.Add(1)
		go p.worker(i)
	}
}

// Queue puts new task in input queue.
func (p *pool) Queue(t Task) {
	// ok := p.inputLock.TryLock()
	if p.inputQ != nil {
		p.inQ.Add(1)
		p.inFlight.Add(1)
		p.inflightWG.Add(1)
		p.inputQ <- t
	}
}

// Shutdown gracefully stops pool, waiting for all task will be finished (successfully or errored after retries). The pool must not be used after Shutdown
func (p *pool) Shutdown() {
	p.shutdownOnce.Do(func() {
		close(p.inputQ)
		p.inputQ = nil

		p.inflightWG.Wait()
		close(p.workersQ)
		p.workersQ = nil
		close(p.errCh)
	})
}

// Cancel stops pool ungracefully. The pool must not be used after Cancel
func (p *pool) Cancel() {
	p.cancel()
}

// ErroredTask returns task with error. It waits while such task appears and returns the task or nil if pool was shut down and no more errored task available.
func (p *pool) ErroredTask() Task {
	return <-p.errCh
}

// Wait holds execution and waits until all tasks pool execution queue will be empty
func (p *pool) Wait() {
	p.inputLock.Lock()
	defer p.inputLock.Unlock()

	p.inflightWG.Wait()
}

func (p *pool) worker(i int) {
	defer p.workersWG.Done()

	for task := range p.workersQ {
		p.inWork.Add(1)
		p.inQ.Add(-1)
		err := p.f(task)
		task.SetError(err)
		if err != nil {
			select {
			case <-p.ctx.Done():
			default:
				if task.ErrorCount() >= p.cfg.Retries {
					go func() {
						select {
						case <-p.ctx.Done():
						case p.errCh <- task:
							p.inflightWG.Done()
							p.inFlight.Add(-1)
						}
					}()

				} else {
					go func() {
						select {
						case <-p.ctx.Done():
						case p.workersQ <- task:
							p.inQ.Add(1)
						}
					}()
				}
			}
		} else {
			p.inflightWG.Done()
			p.inFlight.Add(-1)
		}
		p.inWork.Add(-1)
	}
}
