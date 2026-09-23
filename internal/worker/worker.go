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
	"sort"
	"strconv"
	"time"

	"github.com/developer-overheid-nl/ort-runner/internal/orchestration"
	"github.com/developer-overheid-nl/ort-runner/internal/register"
	"github.com/developer-overheid-nl/ort-runner/internal/runner"
)

var chunkNamePattern = regexp.MustCompile(`^chunk-([0-9]{4})-([0-9a-f]{64})\.gz$`)

type Config struct {
	BatchID         string
	CompletionIndex int
	ManifestDir     string
	ResultsURL      string
	ResultsClient   register.Client
	Runner          runner.Config

	scan  func(context.Context, runner.Config) (runner.Report, error)
	sleep func(context.Context, time.Duration) error
}

type Result struct {
	RepositoryID   string `json:"repositoryId"`
	ScanStatus     string `json:"scanStatus"`
	Delivery       string `json:"delivery"`
	SubmissionPath string `json:"submissionPath"`
}

func ResultIdempotencyKey(batchID, repositoryID string) string {
	sum := sha256.Sum256([]byte(batchID + "\x00" + repositoryID))
	return "ort:" + hex.EncodeToString(sum[:])
}

func Run(ctx context.Context, cfg Config) (Result, error) {
	if cfg.BatchID == "" || cfg.ManifestDir == "" || cfg.Runner.OutputDir == "" {
		return Result{}, fmt.Errorf("batch ID, manifest directory and output directory are required")
	}
	manifest, err := readManifest(cfg.ManifestDir)
	if err != nil {
		return Result{}, err
	}
	if manifest.BatchID != cfg.BatchID {
		return Result{}, fmt.Errorf("manifest batch ID %q does not match %q", manifest.BatchID, cfg.BatchID)
	}
	repository, err := manifest.Repository(cfg.CompletionIndex)
	if err != nil {
		return Result{}, err
	}
	if err := os.MkdirAll(cfg.Runner.OutputDir, 0750); err != nil {
		return Result{}, err
	}

	scan := cfg.scan
	if scan == nil {
		scan = runner.Run
	}
	scanConfig := cfg.Runner
	scanConfig.Repository = repository.URL
	report, scanErr := scan(ctx, scanConfig)
	if report.Status == "" {
		report.Status = "failed"
	}
	if report.Repository == "" {
		report.Repository = repository.URL
	}
	if scanErr != nil {
		if report.Error == "" {
			report.Error = scanErr.Error()
		} else if report.Error != scanErr.Error() {
			report.Error = errors.Join(errors.New(report.Error), scanErr).Error()
		}
	}
	submission := runner.Submission{SchemaVersion: 1, RepositoryID: repository.ID, Scan: report}
	data, err := json.MarshalIndent(submission, "", "  ")
	if err != nil {
		return Result{}, err
	}
	data = append(data, '\n')
	target := filepath.Join(cfg.Runner.OutputDir, "submission.json")
	if err := writeAtomic(target, data); err != nil {
		return Result{}, err
	}

	result := Result{RepositoryID: repository.ID, ScanStatus: report.Status, Delivery: "not_configured", SubmissionPath: target}
	if cfg.ResultsURL == "" {
		slog.Info("Repository processed", "batch_id", cfg.BatchID, "repository_id", repository.ID, "scan_status", report.Status, "delivery", result.Delivery)
		return result, nil
	}
	if err := deliver(ctx, cfg, data, ResultIdempotencyKey(cfg.BatchID, repository.ID)); err != nil {
		result.Delivery = "failed"
		return result, err
	}
	result.Delivery = "posted"
	slog.Info("Repository processed", "batch_id", cfg.BatchID, "repository_id", repository.ID, "scan_status", report.Status, "delivery", result.Delivery)
	return result, nil
}

func readManifest(directory string) (orchestration.Manifest, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return orchestration.Manifest{}, fmt.Errorf("read manifest directory: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if chunkNamePattern.MatchString(entry.Name()) {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	chunks := make([]orchestration.Chunk, 0, len(names))
	for _, name := range names {
		matches := chunkNamePattern.FindStringSubmatch(name)
		index, err := strconv.Atoi(matches[1])
		if err != nil {
			return orchestration.Manifest{}, err
		}
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

func deliver(ctx context.Context, cfg Config, data []byte, idempotencyKey string) error {
	sleep := cfg.sleep
	if sleep == nil {
		sleep = sleepContext
	}
	delays := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		err = cfg.ResultsClient.PostResult(ctx, cfg.ResultsURL, data, idempotencyKey)
		if err == nil {
			return nil
		}
		if attempt < len(delays) {
			if sleepErr := sleep(ctx, delays[attempt]); sleepErr != nil {
				return sleepErr
			}
		}
	}
	return fmt.Errorf("deliver result after 5 attempts: %w", err)
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

func writeAtomic(target string, data []byte) error {
	if err := os.WriteFile(target+".tmp", data, 0600); err != nil {
		return err
	}
	return os.Rename(target+".tmp", target)
}
