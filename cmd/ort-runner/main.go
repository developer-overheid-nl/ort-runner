package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/developer-overheid-nl/ort-runner/internal/runner"
)

var version = "dev"

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	os.Exit(execute(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func execute(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("ort-runner", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cfg := runner.Config{RunnerVersion: version, ORTImage: os.Getenv("ORT_RUNNER_ORT_IMAGE")}
	batch := runner.BatchConfig{RegisterAPIKey: os.Getenv("ORT_REGISTER_API_KEY")}
	flags.StringVar(&batch.RepositoriesURL, "repositories-url", os.Getenv("ORT_REPOSITORIES_URL"), "OSS-register GET endpoint; processes all repositories")
	flags.StringVar(&batch.ResultsURL, "results-url", os.Getenv("ORT_RESULTS_URL"), "Result POST endpoint; omitted means save messages locally")
	flags.DurationVar(&batch.HTTPTimeout, "http-timeout", 30*time.Second, "Timeout per register GET or result POST")
	flags.StringVar(&cfg.Repository, "repository", "", "Repository URL or local Git path for a single scan")
	flags.StringVar(&cfg.Revision, "revision", "", "Full Git commit SHA; omitted uses the default branch HEAD")
	flags.StringVar(&cfg.ConfigDir, "config-dir", "/config", "Directory containing the ORT configuration")
	flags.StringVar(&cfg.OutputDir, "output-dir", "/output", "Parent directory for individual run results")
	flags.StringVar(&cfg.ORTBinary, "ort-binary", "ort", "Path to the ORT executable")
	flags.DurationVar(&cfg.StageTimeout, "stage-timeout", 30*time.Minute, "Timeout per checkout or ORT stage")
	showVersion := flags.Bool("version", false, "Print runner version")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *showVersion {
		fmt.Fprintln(stdout, version)
		return 0
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "Unexpected positional arguments; use --help for all options.")
		return 2
	}
	if batch.RepositoriesURL != "" {
		if cfg.Repository != "" || cfg.Revision != "" {
			fmt.Fprintln(stderr, "Use either --repositories-url or --repository.")
			return 2
		}
		batch.Runner = cfg
		if err := configureBatchHTTPClients(ctx, &batch); err != nil {
			fmt.Fprintf(stderr, "Configure client credentials: %v\n", err)
			return 2
		}
		report, err := runner.RunBatch(ctx, batch)
		if report.OutputDir != "" {
			fmt.Fprintf(stderr, "Batch report: %s/batch.json\n", report.OutputDir)
		}
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	}
	if cfg.Repository == "" || batch.ResultsURL != "" {
		fmt.Fprintln(stderr, "Provide --repositories-url or --repository. --results-url requires --repositories-url.")
		return 2
	}
	report, err := runner.Run(ctx, cfg)
	if report.OutputDir != "" {
		fmt.Fprintf(stderr, "Run report: %s/run.json\n", report.OutputDir)
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
