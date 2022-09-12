package proletarian_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/eliastor/proletarian"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type task struct {
	proletarian.TaskHeader
	name     string
	mustfail bool
	failed   bool

	maxretries int
	retries    int
}

func newPoolTestDunc(t *testing.T) func(tt proletarian.Task) error {
	return func(tt proletarian.Task) error {
		current := tt.(*task)
		if current.mustfail {
			current.retries++
			assert.LessOrEqual(t, current.retries, current.maxretries, "task has already failed")
			current.failed = true
			return fmt.Errorf("%s failed", current.name)
		} else if current.maxretries > 0 {
			assert.LessOrEqual(t, current.retries, current.maxretries, "too much retries")
			current.retries++
			return fmt.Errorf("%s retry %d", current.name, current.retries)
		}

		return nil
	}
}

func newTask(name string, mustfail bool, maxretries int) *task {
	return &task{
		name:       name,
		mustfail:   mustfail,
		maxretries: maxretries,
	}
}

func TestPoolAllOk(t *testing.T) {

	tasks := []*task{
		newTask("ok1", false, 0),
		newTask("retry1", false, 1),
		newTask("retry2", false, 2),
	}

	pool := proletarian.NewPool(context.TODO(), proletarian.PoolConfig{
		LobbySize: 0,
		Size:      2,
		Retries:   2,
		Func:      newPoolTestDunc(t),
	})

	pool.Run()

	go func() {
		for i := range tasks {
			pool.Queue(tasks[i])
		}
		t.Log("Shutting down pool")
		pool.Shutdown()
	}()
	erroredTasks := []proletarian.Task{}
	go func() {
		for {
			errTask := pool.ErroredTask()
			if errTask == nil {
				break
			}
			t.Log(errTask)
			if assert.NotNil(t, errTask) {
				erroredTasks = append(erroredTasks, errTask)
			}
		}
	}()

	pool.Wait()

}

func TestPollAllFailed(t *testing.T) {
	tasks := []*task{
		newTask("retried", false, 2),
		newTask("failed", true, 2),
	}

	pool := proletarian.NewPool(context.TODO(), proletarian.PoolConfig{
		LobbySize: 0,
		Size:      2,
		Retries:   2,
		Func:      newPoolTestDunc(t),
	})

	go pool.Run()

	go func() {
		for i := range tasks {
			pool.Queue(tasks[i])
		}
		pool.Shutdown()
	}()

	var caughtFailed, caughtRetried bool

	require.Eventually(t, func() bool {
		errTask := pool.ErroredTask()
		t.Log(errTask)
		if errTask == nil {
			return caughtFailed && caughtRetried
		}
		switch errTask.(*task).name {
		case "retried":
			caughtRetried = true
		case "failed":
			caughtFailed = true
		}
		return false
	}, 2*time.Second, 100*time.Millisecond)
	pool.Wait()
}
