package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

var workDelay = 200 * time.Millisecond

var ErrWorkers = errors.New("некорректное число обработчиков")

type Result struct {
	Input  int
	Square int
}

func processTask(ctx context.Context, input int) (Result, error) {
	timer := time.NewTimer(workDelay)
	defer stopTimer(timer)

	select {
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case <-timer.C:
	}

	return Result{Input: input, Square: input * input}, nil
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func runSequential(ctx context.Context, inputs []int) ([]Result, error) {
	collected := make([]Result, 0, len(inputs))

	for _, input := range inputs {
		if err := ctx.Err(); err != nil {
			return collected, fmt.Errorf("последовательный проход прерван: %w", err)
		}

		result, err := processTask(ctx, input)
		if err != nil {
			return collected, err
		}

		collected = append(collected, result)
	}

	return collected, nil
}

type pool struct {
	workers int
	jobs    chan int
	results chan Result
	wg      sync.WaitGroup
}

func newPool(workers int) (*pool, error) {
	if workers < 1 {
		return nil, fmt.Errorf("%w: получено %d, нужно минимум 1", ErrWorkers, workers)
	}

	return &pool{
		workers: workers,
		jobs:    make(chan int),
		results: make(chan Result),
	}, nil
}

func (p *pool) start(ctx context.Context, inputs []int) {
	go p.feed(ctx, inputs)

	p.wg.Add(p.workers)
	for i := 0; i < p.workers; i++ {
		go p.work(ctx)
	}

	go p.finish()
}

func (p *pool) feed(ctx context.Context, inputs []int) {
	defer close(p.jobs)

	for _, input := range inputs {
		select {
		case p.jobs <- input:
		case <-ctx.Done():
			return
		}
	}
}

func (p *pool) work(ctx context.Context) {
	defer p.wg.Done()

	for input := range p.jobs {
		result, err := processTask(ctx, input)
		if err != nil {
			return
		}

		select {
		case p.results <- result:
		case <-ctx.Done():
			return
		}
	}
}

func (p *pool) finish() {
	p.wg.Wait()
	close(p.results)
}

func (p *pool) drain() []Result {
	collected := make([]Result, 0, p.workers)

	for result := range p.results {
		collected = append(collected, result)
	}

	return collected
}

func runPool(ctx context.Context, inputs []int, workers int) ([]Result, error) {
	p, err := newPool(workers)
	if err != nil {
		return nil, err
	}

	p.start(ctx, inputs)
	collected := p.drain()

	sortByInput(collected)

	if err := ctx.Err(); err != nil {
		return collected, fmt.Errorf("пул остановлен: %w", err)
	}

	return collected, nil
}

func sortByInput(results []Result) {
	slices.SortFunc(results, func(a, b Result) int {
		return cmp.Compare(a.Input, b.Input)
	})
}

func sumSquares(results []Result) int {
	total := 0
	for _, result := range results {
		total += result.Square
	}
	return total
}
