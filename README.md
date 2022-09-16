
# proletarian [![GoDoc](https://godoc.org/github.com/ngithub.com/eliastor/proletarian?status.svg)](https://godoc.org/github.com/eliastor/proletarian) [![GitHub go.mod Go version of a Go module](https://img.shields.io/github/go-mod/go-version/gomods/athens.svg)](https://godoc.org/github.com/eliastor/proletarian) [![Go Report Card](https://goreportcard.com/badge/github.com/eliastor/proletarian)](https://goreportcard.com/report/github.com/eliastor/proletarian)

Worker pool with retries and gracefull shutdown for Go

## Installation

Using `go get`

```
go get github.com/eliastor/proletarian
```

## Usage

```go

type Task struct {
	proletarian.TaskHeader // embed this to make your task supported by proletarian
    ... // all other fields for your task
}

pool := proletarian.NewPool(context.TODO(), proletarian.PoolConfig{
    LobbySize: 0,
    Size:      2,
    Retries:   2,
    Func:      func(poolTask proletarian.Task) error {
		task := poolTask.(*Task)
        ... // working with task
		return nil // return error, if nil, if returned error is not nil, then retry mechanism will be applied
	},
})

pool.Run()

go func() {
	for i := range tasks {
		pool.Queue(tasks[i])
	}
	pool.Shutdown()
}()

erroredTasks := []proletarian.Task{}

go func() {
	for {
		errTask := pool.ErroredTask()
		if errTask == nil {
			break
		}

		if assert.NotNil(t, errTask) {
			erroredTasks = append(erroredTasks, errTask)
		}
	}
}()

pool.Wait()


```