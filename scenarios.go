package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"
)

const expectedSum = 204

func scenarioInputs() []int {
	return []int{1, 2, 3, 4, 5, 6, 7, 8}
}

func report(label string, results []Result, elapsed time.Duration, err error) {
	fmt.Printf("[%s]\n", label)

	for _, result := range results {
		fmt.Printf("  %d^2 = %d\n", result.Input, result.Square)
	}

	fmt.Printf("  всего: %d, сумма квадратов: %d, время: %v\n",
		len(results), sumSquares(results), elapsed.Round(time.Millisecond))

	if err != nil {
		fmt.Printf("  ошибка: %v\n", err)
	}

	fmt.Println()
}

func checkFull(results []Result) error {
	want := len(scenarioInputs())

	if len(results) != want {
		return fmt.Errorf("ждали %d результатов, получили %d", want, len(results))
	}

	if got := sumSquares(results); got != expectedSum {
		return fmt.Errorf("сумма квадратов %d, ждали %d", got, expectedSum)
	}

	return nil
}

func runSequentialScenario() error {
	started := time.Now()
	results, err := runSequential(context.Background(), scenarioInputs())
	elapsed := time.Since(started)

	report("последовательно", results, elapsed, err)

	if err != nil {
		return err
	}

	return checkFull(results)
}

func runPoolScenario(workers int) error {
	started := time.Now()
	results, err := runPool(context.Background(), scenarioInputs(), workers)
	elapsed := time.Since(started)

	report(fmt.Sprintf("пул, обработчиков: %d", workers), results, elapsed, err)

	if err != nil {
		return err
	}

	return checkFull(results)
}

func runTimeoutScenario(budget time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	started := time.Now()
	results, err := runPool(ctx, scenarioInputs(), 4)
	elapsed := time.Since(started)

	report(fmt.Sprintf("пул, таймаут %v", budget), results, elapsed, err)

	if !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("ждали context.DeadlineExceeded, получили %v", err)
	}

	if len(results) == 0 || len(results) >= len(scenarioInputs()) {
		return fmt.Errorf("ждали частичный набор, получили %d из %d", len(results), len(scenarioInputs()))
	}

	return nil
}

func runCanceledScenario() error {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	started := time.Now()
	results, err := runPool(ctx, scenarioInputs(), 4)
	elapsed := time.Since(started)

	report("отмена до старта пула", results, elapsed, err)

	if !errors.Is(err, context.Canceled) {
		return fmt.Errorf("ждали context.Canceled, получили %v", err)
	}

	if len(results) != 0 {
		return fmt.Errorf("ждали пустой набор, получили %d", len(results))
	}

	return nil
}

type runStats struct {
	label   string
	samples []time.Duration
}

func (s runStats) median() time.Duration {
	return s.samples[len(s.samples)/2]
}

func (s runStats) String() string {
	parts := make([]string, 0, len(s.samples))

	for _, sample := range s.samples {
		parts = append(parts, sample.Round(time.Millisecond).String())
	}

	return strings.Join(parts, " ")
}

func collect(label string, repeats int, run func() time.Duration) runStats {
	stats := runStats{label: label, samples: make([]time.Duration, 0, repeats)}

	for i := 0; i < repeats; i++ {
		stats.samples = append(stats.samples, run())
	}

	sortDurations(stats.samples)

	return stats
}

func sortDurations(values []time.Duration) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

func speedup(baseline, measured time.Duration) float64 {
	if measured <= 0 {
		return 0
	}

	return float64(baseline) / float64(measured)
}

func runBenchScenario(repeats int) error {
	if repeats < 1 {
		return fmt.Errorf("число прогонов должно быть минимум 1")
	}

	baseline := collect("последовательно", repeats, func() time.Duration {
		started := time.Now()
		_, _ = runSequential(context.Background(), scenarioInputs())
		return time.Since(started)
	})

	single := collect("пул, 1 обработчик", repeats, func() time.Duration {
		started := time.Now()
		_, _ = runPool(context.Background(), scenarioInputs(), 1)
		return time.Since(started)
	})

	four := collect("пул, 4 обработчика", repeats, func() time.Duration {
		started := time.Now()
		_, _ = runPool(context.Background(), scenarioInputs(), 4)
		return time.Since(started)
	})

	fmt.Printf("замеры, прогонов на режим: %d (в таблице медиана)\n\n", repeats)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "режим\tпрогоны\tмедиана\tускорение")

	for _, stats := range []runStats{baseline, single, four} {
		fmt.Fprintf(w, "%s\t%s\t%v\t%.2fx\n",
			stats.label,
			stats,
			stats.median().Round(time.Millisecond),
			speedup(baseline.median(), stats.median()),
		)
	}

	return w.Flush()
}
