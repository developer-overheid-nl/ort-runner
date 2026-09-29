package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/developer-overheid-nl/ort-runner/internal/register"
	"github.com/developer-overheid-nl/ort-runner/internal/runner"
	"github.com/developer-overheid-nl/ort-runner/internal/worker"
)

const workerUsage = `Usage: ort-runner worker

Scans the repository at the Indexed Job completion index. Runs without
credentials. Configured through the environment:

  ORT_BATCH_ID, ORT_MANIFEST_DIR, JOB_COMPLETION_INDEX
  ORT_CONFIG_DIR (default /config), ORT_OUTPUT_DIR (default /output)
  ORT_STAGE_TIMEOUT (default 30m)
`

const deliverUsage = `Usage: ort-runner deliver

Posts the submission written by "ort-runner worker". Configured through the
environment: ORT_BATCH_ID, ORT_OUTPUT_DIR (default /output), ORT_RESULTS_URL
and the AUTH_* client credentials. An empty ORT_RESULTS_URL only logs the result.
`

func executeWorker(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if code, done := internalCommandArgs(args, workerUsage, stdout, stderr); done {
		return code
	}
	stageTimeout, err := envDuration("ORT_STAGE_TIMEOUT", 30*time.Minute)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	index, err := strconv.Atoi(os.Getenv("JOB_COMPLETION_INDEX"))
	if err != nil || index < 0 {
		fmt.Fprintln(stderr, "JOB_COMPLETION_INDEX must be a non-negative integer")
		return 2
	}
	cfg := worker.ScanConfig{
		BatchID:         os.Getenv("ORT_BATCH_ID"),
		CompletionIndex: index,
		ManifestDir:     os.Getenv("ORT_MANIFEST_DIR"),
		Runner: runner.Config{
			ConfigDir:     envDefault("ORT_CONFIG_DIR", "/config"),
			OutputDir:     envDefault("ORT_OUTPUT_DIR", "/output"),
			ORTImage:      os.Getenv("ORT_RUNNER_ORT_IMAGE"),
			RunnerVersion: version,
			StageTimeout:  stageTimeout,
		},
	}
	if cfg.BatchID == "" || cfg.ManifestDir == "" {
		fmt.Fprintln(stderr, "ORT_BATCH_ID and ORT_MANIFEST_DIR are required")
		return 2
	}
	if _, err := worker.Scan(ctx, cfg); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func executeDeliver(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if code, done := internalCommandArgs(args, deliverUsage, stdout, stderr); done {
		return code
	}
	cfg := worker.DeliverConfig{
		BatchID:    os.Getenv("ORT_BATCH_ID"),
		OutputDir:  envDefault("ORT_OUTPUT_DIR", "/output"),
		ResultsURL: os.Getenv(register.ResultsURLVariable),
	}
	if cfg.BatchID == "" {
		fmt.Fprintln(stderr, "ORT_BATCH_ID is required")
		return 2
	}
	if cfg.ResultsURL != "" {
		resultsHTTP, err := newResultsHTTPClient(ctx, 30*time.Second)
		if err != nil {
			fmt.Fprintf(stderr, "Configure result credentials: %v\n", err)
			return 2
		}
		cfg.Client = register.Client{HTTP: resultsHTTP}
	}
	if err := worker.Deliver(ctx, cfg); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
