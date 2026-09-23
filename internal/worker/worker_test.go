package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/developer-overheid-nl/ort-runner/internal/orchestration"
	"github.com/developer-overheid-nl/ort-runner/internal/register"
	"github.com/developer-overheid-nl/ort-runner/internal/runner"
)

func TestResultIdempotencyKeyIsStable(t *testing.T) {
	first := ResultIdempotencyKey("batch-1", "repo-1")
	if first != ResultIdempotencyKey("batch-1", "repo-1") || first == ResultIdempotencyKey("batch-1", "repo-2") || !regexp.MustCompile(`^ort:[0-9a-f]{64}$`).MatchString(first) {
		t.Fatalf("invalid idempotency key %q", first)
	}
}

func TestRunPostsSuccessfulScan(t *testing.T) {
	var submission runner.Submission
	var key string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key = r.Header.Get("Idempotency-Key")
		if err := json.NewDecoder(r.Body).Decode(&submission); err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	cfg := workerConfig(t, server.URL)
	calls := 0
	cfg.scan = func(_ context.Context, scan runner.Config) (runner.Report, error) {
		calls++
		return runner.Report{Status: "completed", Repository: scan.Repository}, nil
	}

	result, err := Run(context.Background(), cfg)
	if err != nil || calls != 1 || result.RepositoryID != "repo-a" || result.Delivery != "posted" || submission.RepositoryID != "repo-a" || key != ResultIdempotencyKey("batch-1", "repo-a") {
		t.Fatalf("result=%+v submission=%+v key=%q calls=%d err=%v", result, submission, key, calls, err)
	}
	assertSubmissionFile(t, cfg.Runner.OutputDir, "completed", true)
}

func TestRunTreatsScanFailureAsDeliveredRepositoryResult(t *testing.T) {
	var submission runner.Submission
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&submission)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()
	cfg := workerConfig(t, server.URL)
	cfg.scan = func(_ context.Context, _ runner.Config) (runner.Report, error) {
		return runner.Report{Error: "analyze failed"}, errors.New("analyze failed")
	}

	result, err := Run(context.Background(), cfg)
	if err != nil || result.ScanStatus != "failed" || submission.Scan.Status != "failed" || submission.Scan.Error != "analyze failed" {
		t.Fatalf("result=%+v submission=%+v err=%v", result, submission, err)
	}
}

func TestRunWithoutResultsEndpointKeepsSubmission(t *testing.T) {
	cfg := workerConfig(t, "")
	cfg.scan = func(_ context.Context, scan runner.Config) (runner.Report, error) {
		return runner.Report{Status: "completed", Repository: scan.Repository}, nil
	}
	result, err := Run(context.Background(), cfg)
	if err != nil || result.Delivery != "not_configured" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	assertSubmissionFile(t, cfg.Runner.OutputDir, "completed", true)
}

func TestRunRetriesDeliveryWithoutRescanning(t *testing.T) {
	attempts := 0
	var keys []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		if attempts < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	cfg := workerConfig(t, server.URL)
	scans := 0
	cfg.scan = func(_ context.Context, scan runner.Config) (runner.Report, error) {
		scans++
		return runner.Report{Status: "completed", Repository: scan.Repository}, nil
	}
	var delays []time.Duration
	cfg.sleep = func(_ context.Context, delay time.Duration) error { delays = append(delays, delay); return nil }
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if scans != 1 || attempts != 3 || !slices.Equal(delays, []time.Duration{time.Second, 2 * time.Second}) || keys[0] != keys[1] || keys[1] != keys[2] {
		t.Fatalf("scans=%d attempts=%d delays=%v keys=%v", scans, attempts, delays, keys)
	}
}

func TestRunReturnsInfrastructureErrorAfterDeliveryRetries(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	cfg := workerConfig(t, server.URL)
	cfg.scan = func(_ context.Context, scan runner.Config) (runner.Report, error) {
		return runner.Report{Status: "completed", Repository: scan.Repository}, nil
	}
	cfg.sleep = func(context.Context, time.Duration) error { return nil }
	if _, err := Run(context.Background(), cfg); err == nil || attempts != 5 {
		t.Fatalf("attempts=%d err=%v", attempts, err)
	}
}

func TestRunRejectsInvalidManifestSelectionBeforeScan(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "negative index", mutate: func(cfg *Config) { cfg.CompletionIndex = -1 }},
		{name: "out of range", mutate: func(cfg *Config) { cfg.CompletionIndex = 1 }},
		{name: "batch mismatch", mutate: func(cfg *Config) { cfg.BatchID = "other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := workerConfig(t, "")
			called := false
			cfg.scan = func(context.Context, runner.Config) (runner.Report, error) {
				called = true
				return runner.Report{}, nil
			}
			tc.mutate(&cfg)
			if _, err := Run(context.Background(), cfg); err == nil || called {
				t.Fatalf("invalid selection reached scan: called=%t err=%v", called, err)
			}
		})
	}
}

func workerConfig(t *testing.T, resultsURL string) Config {
	t.Helper()
	manifestDir := t.TempDir()
	chunks, err := orchestration.EncodeManifest(orchestration.Manifest{SchemaVersion: 1, BatchID: "batch-1", Repositories: []register.Repository{{ID: "repo-a", URL: "https://example.test/a.git"}}}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	for _, chunk := range chunks {
		name := fmt.Sprintf("chunk-%04d-%s.gz", chunk.Index, chunk.Digest)
		if err := os.WriteFile(filepath.Join(manifestDir, name), chunk.Data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return Config{
		BatchID: "batch-1", CompletionIndex: 0, ManifestDir: manifestDir, ResultsURL: resultsURL,
		ResultsClient: register.Client{HTTP: http.DefaultClient},
		Runner:        runner.Config{OutputDir: t.TempDir(), ConfigDir: t.TempDir(), StageTimeout: time.Minute},
		sleep:         func(context.Context, time.Duration) error { t.Fatal("unexpected retry"); return nil },
	}
}

func assertSubmissionFile(t *testing.T, outputDir, status string, equal bool) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(outputDir, "submission.json"))
	if err != nil {
		t.Fatal(err)
	}
	var submission runner.Submission
	if err := json.Unmarshal(data, &submission); err != nil {
		t.Fatal(err)
	}
	if (submission.Scan.Status == status) != equal {
		t.Fatalf("submission status=%q want comparison %t with %q", submission.Scan.Status, equal, status)
	}
}
