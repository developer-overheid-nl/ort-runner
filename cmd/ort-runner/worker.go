package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/developer-overheid-nl/ort-runner/internal/register"
	"github.com/developer-overheid-nl/ort-runner/internal/runner"
	workerpkg "github.com/developer-overheid-nl/ort-runner/internal/worker"
)

func init() {
	workerCommand = executeWorker
}

func executeWorker(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	stageTimeout, err := envDuration("ORT_STAGE_TIMEOUT", 30*time.Minute)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	flags := flag.NewFlagSet("ort-runner worker", flag.ContinueOnError)
	flags.SetOutput(stderr)
	batchID := os.Getenv("ORT_BATCH_ID")
	manifestDir := os.Getenv("ORT_MANIFEST_DIR")
	indexText := os.Getenv("JOB_COMPLETION_INDEX")
	resultsURL := os.Getenv("ORT_RESULTS_URL")
	configDir := envDefault("ORT_CONFIG_DIR", "/config")
	outputDir := envDefault("ORT_OUTPUT_DIR", "/output")
	flags.StringVar(&batchID, "batch-id", batchID, "Stable batch identifier")
	flags.StringVar(&manifestDir, "manifest-dir", manifestDir, "Directory containing repository manifest chunks")
	flags.StringVar(&indexText, "completion-index", indexText, "Indexed Job completion index")
	flags.StringVar(&resultsURL, "results-url", resultsURL, "Result POST endpoint")
	flags.StringVar(&configDir, "config-dir", configDir, "Directory containing the ORT configuration")
	flags.StringVar(&outputDir, "output-dir", outputDir, "Worker output directory")
	flags.DurationVar(&stageTimeout, "stage-timeout", stageTimeout, "Timeout per checkout or ORT stage")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	index, indexErr := strconv.Atoi(indexText)
	if flags.NArg() != 0 || batchID == "" || manifestDir == "" || indexErr != nil || index < 0 || configDir == "" || outputDir == "" || stageTimeout <= 0 {
		fmt.Fprintln(stderr, "Worker requires batch ID, manifest directory, non-negative completion index, config directory and output directory.")
		return 2
	}
	resultsHTTP, err := newResultsHTTPClient(ctx, 30*time.Second)
	if err != nil {
		fmt.Fprintf(stderr, "Configure result credentials: %v\n", err)
		return 2
	}
	cfg := workerpkg.Config{
		BatchID: batchID, CompletionIndex: index, ManifestDir: manifestDir, ResultsURL: resultsURL,
		ResultsClient: register.Client{HTTP: resultsHTTP},
		Runner:        runner.Config{ConfigDir: configDir, OutputDir: outputDir, ORTBinary: "ort", ORTImage: os.Getenv("ORT_RUNNER_ORT_IMAGE"), RunnerVersion: version, StageTimeout: stageTimeout},
	}
	result, err := workerpkg.Run(ctx, cfg)
	if encodeErr := json.NewEncoder(stdout).Encode(result); encodeErr != nil && err == nil {
		err = encodeErr
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
