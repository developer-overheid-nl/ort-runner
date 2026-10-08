package main

import (
	"context"
	"fmt"
	"io"

	"github.com/developer-overheid-nl/ort-runner/internal/register"
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

func executeDeliver(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if !noArguments("deliver", args, stderr) {
		return 2
	}
	cfg, err := loadDeliverConfig()
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if cfg.Worker.ResultsURL != "" {
		httpClient, err := newHTTPClient(ctx, cfg.HTTP)
		if err != nil {
			fmt.Fprintf(stderr, "Configure client credentials: %v\n", err)
			return 2
		}
		cfg.Worker.Client = register.Client{HTTP: httpClient}
	}
	if err := worker.Deliver(ctx, cfg.Worker); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
