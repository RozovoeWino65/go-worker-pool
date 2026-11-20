package main

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"
)

func scenarioTable() map[int]int {
	return map[int]int{1: 1, 2: 4, 3: 9, 4: 16, 5: 25, 6: 36, 7: 49, 8: 64}
}

func assertPairs(t *testing.T, results []Result) {
	t.Helper()

	table := scenarioTable()
	if len(results) != len(table) {
		t.Fatalf("ждали %d результатов, получили %d", len(table), len(results))
	}

	used := make(map[int]struct{}, len(results))

	for _, result := range results {
		want, known := table[result.Input]
		if !known {
			t.Fatalf("неизвестный вход %d", result.Input)
		}
		if result.Square != want {
			t.Errorf("%d в квадрате = %d, ждали %d", result.Input, result.Square, want)
		}
		if _, duplicate := used[result.Input]; duplicate {
			t.Errorf("вход %d посчитан дважды", result.Input)
		}
		used[result.Input] = struct{}{}
	}

	if got := sumSquares(results); got != expectedSum {
		t.Errorf("сумма квадратов %d, ждали %d", got, expectedSum)
	}
}

func shortDelay(t *testing.T) {
	t.Helper()

	previous := workDelay
	workDelay = 20 * time.Millisecond

	t.Cleanup(func() {
		workDelay = previous
	})
}

func TestSequentialReturnsAllPairs(t *testing.T) {
	shortDelay(t)

	started := time.Now()
	results, err := runSequential(context.Background(), scenarioInputs())
	elapsed := time.Since(started)

	if err != nil {
		t.Fatalf("неждали ошибку: %v", err)
	}

	assertPairs(t, results)

	if elapsed < 8*workDelay {
		t.Errorf("проход занял %v, это быстрее восьми заданий", elapsed)
	}
}

func TestSequentialStopsWhenCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	results, err := runSequential(ctx, scenarioInputs())

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ждали context.Canceled, получили %v", err)
	}

	if len(results) != 0 {
		t.Errorf("ждали пустой набор, получили %d", len(results))
	}
}

func TestPoolWorkerCounts(t *testing.T) {
	shortDelay(t)

	for _, workers := range []int{1, 2, 4, 8, 16} {
		workers := workers

		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			t.Parallel()

			results, err := runPool(context.Background(), scenarioInputs(), workers)
			if err != nil {
				t.Fatalf("неждали ошибку: %v", err)
			}

			assertPairs(t, results)

			for i := 1; i < len(results); i++ {
				if results[i-1].Input > results[i].Input {
					t.Fatalf("порядок нарушен: %v", results)
				}
			}
		})
	}
}

func TestPoolSpeedupWithFourWorkers(t *testing.T) {
	shortDelay(t)

	started := time.Now()
	if _, err := runSequential(context.Background(), scenarioInputs()); err != nil {
		t.Fatalf("последовательный проход: %v", err)
	}
	sequentialTime := time.Since(started)

	started = time.Now()
	if _, err := runPool(context.Background(), scenarioInputs(), 4); err != nil {
		t.Fatalf("пул: %v", err)
	}
	poolTime := time.Since(started)

	if poolTime >= sequentialTime/2 {
		t.Errorf("пул из 4 обработчиков %v, последовательно %v, ускорения нет", poolTime, sequentialTime)
	}
}

func TestPoolRejectsBadWorkerCount(t *testing.T) {
	t.Parallel()

	for _, workers := range []int{0, -1, -25} {
		results, err := runPool(context.Background(), scenarioInputs(), workers)

		if !errors.Is(err, ErrWorkers) {
			t.Errorf("workers=%d: ждали ErrWorkers, получили %v", workers, err)
		}
		if results != nil {
			t.Errorf("workers=%d: при ошибке набор должен быть nil", workers)
		}
	}
}

func TestPoolHandlesEmptyInput(t *testing.T) {
	t.Parallel()

	results, err := runPool(context.Background(), nil, 3)
	if err != nil {
		t.Fatalf("неждали ошибку: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("ждали пустой набор, получили %d", len(results))
	}
}

func TestPoolReturnsPartialOnTimeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 330*time.Millisecond)
	defer cancel()

	started := time.Now()
	results, err := runPool(ctx, scenarioInputs(), 4)
	elapsed := time.Since(started)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ждали context.DeadlineExceeded, получили %v", err)
	}

	if len(results) == 0 || len(results) >= 8 {
		t.Fatalf("ждали частичный набор, получили %d из 8", len(results))
	}

	if elapsed > time.Second {
		t.Errorf("выход занял %v, дольше секунды после дедлайна", elapsed)
	}

	table := scenarioTable()
	for _, result := range results {
		if table[result.Input] != result.Square {
			t.Errorf("испорченная пара %+v", result)
		}
	}
}

func TestPoolCanceledBeforeStartReturnsFast(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)

	go func() {
		_, err := runPool(ctx, scenarioInputs(), 4)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ждали context.Canceled, получили %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runPool не вернулся на отменённом контексте")
	}
}

func TestPoolStopsAllWorkers(t *testing.T) {
	baseline := runtime.NumGoroutine()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	if _, err := runPool(ctx, scenarioInputs(), 6); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ждали context.DeadlineExceeded, получили %v", err)
	}

	limit := time.Now().Add(2 * time.Second)

	for time.Now().Before(limit) {
		if runtime.NumGoroutine() <= baseline {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("после возврата живых горутин %d, было %d", runtime.NumGoroutine(), baseline)
}

func TestNewPoolValidatesWorkers(t *testing.T) {
	t.Parallel()

	if _, err := newPool(0); !errors.Is(err, ErrWorkers) {
		t.Errorf("newPool(0): ждали ErrWorkers, получили %v", err)
	}

	p, err := newPool(2)
	if err != nil {
		t.Fatalf("newPool(2): неждали ошибку: %v", err)
	}

	if p.workers != 2 {
		t.Errorf("workers = %d, ждали 2", p.workers)
	}
}

func TestSortByInputKeepsOrder(t *testing.T) {
	t.Parallel()

	results := []Result{{Input: 5}, {Input: 1}, {Input: 3}}
	sortByInput(results)

	want := []int{1, 3, 5}
	for i, result := range results {
		if result.Input != want[i] {
			t.Fatalf("позиция %d: %d, ждали %d", i, result.Input, want[i])
		}
	}
}
