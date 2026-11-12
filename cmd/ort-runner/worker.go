package main

import (
	"context"
	"fmt"
	"io"

	"github.com/developer-overheid-nl/ort-runner/internal/worker"
)

func executeWorker(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if !noArguments("worker", args, stderr) {
		return 2
	}
	cfg, err := loadWorkerConfig()
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if _, err := worker.Scan(ctx, cfg); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
