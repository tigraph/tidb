package executor

import (
	"context"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util/chunk"
	"sync"
)

var _ Executor = &TraverseExecutor{}

const workerConcurrency = 5

type tempResult struct {
	vertexIds  []int64
	chainLevel int64
}

type DirType uint8

const (
	IN DirType = iota
	OUT
	BOTH
)

type condition struct {
	edgeName  string
	direction DirType
}

type TraverseExecutor struct {
	baseExecutor

	workerWg *sync.WaitGroup
	doneErr  error

	conditionChain []condition

	workerChan chan *tempResult
	tablePlan  plannercore.PhysicalPlan
}

// Open initializes necessary variables for using this executor.
func (e *TraverseExecutor) Open(ctx context.Context) error {
	err := e.children[0].Open(ctx)
	if err != nil {
		return err
	}

	e.startWorkers(ctx)
	return nil
}

func (e *TraverseExecutor) runNewWorker(ctx context.Context) {
	defer func() {
		e.workerWg.Done()
	}()

	var task *tempResult
	for ok := true; ok; {
		select {
		case task, ok = <-e.workerChan:
			if !ok {
				return
			}
		case <-ctx.Done():
			return
		}

		err := e.handleTraverseTask(ctx, task)
		e.doneErr = err
		return
	}
}

func (e *TraverseExecutor) startWorkers(ctx context.Context) {
	e.workerChan = make(chan *tempResult, workerConcurrency)

	for i := 0; i < workerConcurrency; i++ {
		e.workerWg.Add(1)
		go e.runNewWorker(ctx)
	}
}

func (e *TraverseExecutor) handleTraverseTask(ctx context.Context, task *tempResult) error {
	return nil
}

func (e *TraverseExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()

	return nil
}

func (e *TraverseExecutor) Close() error {
	return nil
}
