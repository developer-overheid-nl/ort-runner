// Package worker runs one repository of an Indexed Job. Scanning and delivery
// are separate steps in separate containers, so code that is executed while
// scanning a repository never shares a process or environment with the result
// credentials.
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"github.com/developer-overheid-nl/ort-runner/internal/orchestration"
	"github.com/developer-overheid-nl/ort-runner/internal/register"
	"github.com/developer-overheid-nl/ort-runner/internal/runner"
)

const submissionFile = "submission.json"

var chunkNamePattern = regexp.MustCompile(`^chunk-([0-9]{4})-([0-9a-f]{64})\.json$`)

// Delivery is retried in-process first; a failing Pod would rescan the repository.
var deliveryDelays = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}

type ScanConfig struct {
	BatchID         string
	CompletionIndex int
	ManifestDir     string
	// Timeout bounds the whole repository scan. A repository that exceeds it is
	// recorded as failed instead of being retried by Kubernetes.
	Timeout time.Duration
	Runner  runner.Config

	scan func(context.Context, runner.Config) (runner.Report, error)
}

type DeliverConfig struct {
	BatchID    string
	OutputDir  string
	ResultsURL string
	Client     register.Client

	sleep func(context.Context, time.Duration) error
}

func ResultIdempotencyKey(batchID, repositoryID string) string {
	sum := sha256.Sum256([]byte(batchID + "\x00" + repositoryID))
	return "ort:" + hex.EncodeToString(sum[:])
}

// Scan scans the repository at the completion index and writes its submission.
// A failed scan is a result, not an error; errors mean the worker itself failed.
func Scan(ctx context.Context, cfg ScanConfig) (runner.Submission, error) {
	if cfg.BatchID == "" || cfg.ManifestDir == "" || cfg.Runner.OutputDir == "" || cfg.Timeout <= 0 {
		return runner.Submission{}, fmt.Errorf("batch ID, manifest directory, output directory and positive timeout are required")
	}
	manifest, err := readManifest(cfg.ManifestDir)
	if err != nil {
		return runner.Submission{}, err
	}
	if manifest.BatchID != cfg.BatchID {
		return runner.Submission{}, fmt.Errorf("manifest batch ID %q does not match %q", manifest.BatchID, cfg.BatchID)
	}
	repository, err := manifest.Repository(cfg.CompletionIndex)
	if err != nil {
		return runner.Submission{}, err
	}
	if err := os.MkdirAll(cfg.Runner.OutputDir, 0750); err != nil {
		return runner.Submission{}, err
	}

	scan := cfg.scan
	if scan == nil {
		scan = runner.Run
	}
	scanConfig := cfg.Runner
	scanConfig.Repository = repository.URL
	scanCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	report, scanErr := scan(scanCtx, scanConfig)
	cancel()
	if ctx.Err() != nil {
		return runner.Submission{}, fmt.Errorf("scan interrupted: %w", ctx.Err())
	}
	if errors.Is(scanCtx.Err(), context.DeadlineExceeded) {
		report.Status = "failed"
		report.Error = fmt.Sprintf("repository scan exceeded the %s time budget", cfg.Timeout)
	}
	if report.Status == "" {
		report.Status = "failed"
	}
	if report.Repository == "" {
		report.Repository = repository.URL
	}
	if scanErr != nil && report.Error == "" {
		report.Error = scanErr.Error()
	}

	submission := runner.Submission{SchemaVersion: 1, RepositoryID: repository.ID, Scan: report}
	data, err := json.MarshalIndent(submission, "", "  ")
	if err != nil {
		return runner.Submission{}, err
	}
	target := filepath.Join(cfg.Runner.OutputDir, submissionFile)
	if err := os.WriteFile(target+".tmp", append(data, '\n'), 0600); err != nil {
		return runner.Submission{}, err
	}
	if err := os.Rename(target+".tmp", target); err != nil {
		return runner.Submission{}, err
	}
	slog.Info("Repository scanned", "batch_id", cfg.BatchID, "repository_id", repository.ID, "scan_status", report.Status)
	return submission, nil
}

// Deliver posts the submission written by Scan. Without a result endpoint it
// only logs the outcome.
func Deliver(ctx context.Context, cfg DeliverConfig) error {
	if cfg.BatchID == "" || cfg.OutputDir == "" {
		return fmt.Errorf("batch ID and output directory are required")
	}
	data, err := os.ReadFile(filepath.Join(cfg.OutputDir, submissionFile))
	if err != nil {
		return fmt.Errorf("read submission: %w", err)
	}
	var submission runner.Submission
	if err := json.Unmarshal(data, &submission); err != nil {
		return fmt.Errorf("decode submission: %w", err)
	}
	if submission.RepositoryID == "" {
		return fmt.Errorf("submission has no repository ID")
	}
	if cfg.ResultsURL == "" {
		slog.Info("Result delivery not configured", "batch_id", cfg.BatchID, "repository_id", submission.RepositoryID, "scan_status", submission.Scan.Status)
		return nil
	}

	sleep := cfg.sleep
	if sleep == nil {
		sleep = sleepContext
	}
	key := ResultIdempotencyKey(cfg.BatchID, submission.RepositoryID)
	for attempt := 0; ; attempt++ {
		err = cfg.Client.PostResult(ctx, cfg.ResultsURL, data, key)
		if err == nil {
			slog.Info("Result delivered", "batch_id", cfg.BatchID, "repository_id", submission.RepositoryID, "scan_status", submission.Scan.Status)
			return nil
		}
		if attempt == len(deliveryDelays) {
			return fmt.Errorf("deliver result after %d attempts: %w", attempt+1, err)
		}
		if err := sleep(ctx, deliveryDelays[attempt]); err != nil {
			return err
		}
	}
}

func readManifest(directory string) (orchestration.Manifest, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return orchestration.Manifest{}, fmt.Errorf("read manifest directory: %w", err)
	}
	var names []string
	for _, entry := range entries {
		if !entry.IsDir() && chunkNamePattern.MatchString(entry.Name()) {
			names = append(names, entry.Name())
		}
	}
	chunks := make([]orchestration.Chunk, 0, len(names))
	for _, name := range names {
		matches := chunkNamePattern.FindStringSubmatch(name)
		index, _ := strconv.Atoi(matches[1])
		data, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			return orchestration.Manifest{}, err
		}
		chunks = append(chunks, orchestration.Chunk{Index: index, Count: len(names), Digest: matches[2], Data: data})
	}
	manifest, err := orchestration.DecodeManifest(chunks)
	if err != nil {
		return orchestration.Manifest{}, fmt.Errorf("decode manifest: %w", err)
	}
	return manifest, nil
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
