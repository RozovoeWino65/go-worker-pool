package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

const usage = `лабораторная работа: пул обработчиков на горутинах

запуск:
  go run .                 все сценарии и замеры
  go run . seq             последовательная обработка
  go run . pool 4          пул из 4 обработчиков
  go run . timeout 300ms   пул с бюджетом времени
  go run . cancel          отмена до старта пула
  go run . bench 3         медиана по 3 прогонам на режим

флаги (после подкоманды):
  -delay 200ms   имитация ожидания внешнего ресурса
  -w 4           обработчиков по умолчанию для подкоманды pool
`

func main() {
	if len(os.Args) < 2 {
		runAll()
		return
	}

	command := os.Args[1]
	set := flag.NewFlagSet(command, flag.ExitOnError)

	delay := set.Duration("delay", 200*time.Millisecond, "имитация ожидания")
	workers := set.Int("w", 4, "число обработчиков для pool")

	if command == "-h" || command == "--help" || command == "help" {
		fmt.Print(usage)
		return
	}

	arg := ""
	if len(os.Args) > 2 && !isFlag(os.Args[2]) {
		arg = os.Args[2]
		rest := append([]string{}, os.Args[3:]...)
		if err := set.Parse(rest); err != nil {
			exitWith(err)
		}
	} else if err := set.Parse(os.Args[2:]); err != nil {
		exitWith(err)
	}

	workDelay = *delay

	if err := dispatch(command, arg, *workers); err != nil {
		exitWith(err)
	}
}

func isFlag(value string) bool {
	return len(value) > 0 && value[0] == '-'
}

func exitWith(err error) {
	fmt.Fprintln(os.Stderr, "ошибка:", err)
	os.Exit(1)
}

func runAll() {
	steps := []func() error{
		runSequentialScenario,
		func() error { return runPoolScenario(1) },
		func() error { return runPoolScenario(4) },
		func() error { return runTimeoutScenario(300 * time.Millisecond) },
		runCanceledScenario,
		func() error { return runBenchScenario(3) },
	}

	for _, step := range steps {
		if err := step(); err != nil {
			exitWith(err)
		}
	}
}

func dispatch(command, arg string, workers int) error {
	switch command {
	case "seq":
		return runSequentialScenario()
	case "pool":
		count, err := poolWorkers(arg, workers)
		if err != nil {
			return err
		}
		return runPoolScenario(count)
	case "timeout":
		budget := 300 * time.Millisecond
		if arg != "" {
			parsed, err := time.ParseDuration(arg)
			if err != nil {
				return fmt.Errorf("бюджет времени %q: %w", arg, err)
			}
			budget = parsed
		}
		return runTimeoutScenario(budget)
	case "cancel":
		return runCanceledScenario()
	case "bench":
		repeats, err := repeatsCount(arg)
		if err != nil {
			return err
		}
		return runBenchScenario(repeats)
	case "all":
		runAll()
		return nil
	default:
		return fmt.Errorf("неизвестная подкоманда %q, см. go run . help", command)
	}
}

func poolWorkers(arg string, fallback int) (int, error) {
	if arg == "" {
		return fallback, nil
	}

	var count int
	if _, err := fmt.Sscanf(arg, "%d", &count); err != nil {
		return 0, fmt.Errorf("число обработчиков %q: %w", arg, err)
	}

	return count, nil
}

func repeatsCount(arg string) (int, error) {
	if arg == "" {
		return 3, nil
	}

	var repeats int
	if _, err := fmt.Sscanf(arg, "%d", &repeats); err != nil {
		return 0, fmt.Errorf("число прогонов %q: %w", arg, err)
	}

	return repeats, nil
}
