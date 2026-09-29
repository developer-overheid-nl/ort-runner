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

func TestScanWritesSubmissionForSelectedRepository(t *testing.T) {
	cfg := scanConfig(t)
	var scanned string
	cfg.scan = func(_ context.Context, scan runner.Config) (runner.Report, error) {
		scanned = scan.Repository
		return runner.Report{Status: "completed", Repository: scan.Repository}, nil
	}
	if _, err := Scan(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	submission := readSubmission(t, cfg.Runner.OutputDir)
	if scanned != "https://example.test/a.git" || submission.RepositoryID != "repo-a" || submission.Scan.Status != "completed" {
		t.Fatalf("scanned=%q submission=%+v", scanned, submission)
	}
}

func TestScanTreatsRepositoryFailureAsResult(t *testing.T) {
	cfg := scanConfig(t)
	cfg.scan = func(context.Context, runner.Config) (runner.Report, error) {
		return runner.Report{}, errors.New("analyze failed")
	}
	if _, err := Scan(context.Background(), cfg); err != nil {
		t.Fatalf("repository failure failed the worker: %v", err)
	}
	submission := readSubmission(t, cfg.Runner.OutputDir)
	if submission.Scan.Status != "failed" || submission.Scan.Error != "analyze failed" || submission.Scan.Repository == "" {
		t.Fatalf("submission=%+v", submission)
	}
}

func TestScanRejectsInvalidManifestSelection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*ScanConfig)
	}{
		{name: "negative index", mutate: func(cfg *ScanConfig) { cfg.CompletionIndex = -1 }},
		{name: "out of range", mutate: func(cfg *ScanConfig) { cfg.CompletionIndex = 1 }},
		{name: "batch mismatch", mutate: func(cfg *ScanConfig) { cfg.BatchID = "other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := scanConfig(t)
			called := false
			cfg.scan = func(context.Context, runner.Config) (runner.Report, error) {
				called = true
				return runner.Report{}, nil
			}
			tc.mutate(&cfg)
			if _, err := Scan(context.Background(), cfg); err == nil || called {
				t.Fatalf("invalid selection reached scan: called=%t err=%v", called, err)
			}
		})
	}
}

func TestDeliverPostsWithIdempotencyKey(t *testing.T) {
	var key string
	var posted runner.Submission
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key = r.Header.Get("Idempotency-Key")
		if err := json.NewDecoder(r.Body).Decode(&posted); err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	cfg := deliverConfig(t, server.URL)
	if err := Deliver(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if posted.RepositoryID != "repo-a" || key != ResultIdempotencyKey("batch-1", "repo-a") {
		t.Fatalf("posted=%+v key=%q", posted, key)
	}
}

func TestDeliverWithoutEndpointOnlyLogs(t *testing.T) {
	cfg := deliverConfig(t, "")
	if err := Deliver(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
}

func TestDeliverRetriesWithSameKey(t *testing.T) {
	var keys []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		if len(keys) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	cfg := deliverConfig(t, server.URL)
	var delays []time.Duration
	cfg.sleep = func(_ context.Context, delay time.Duration) error { delays = append(delays, delay); return nil }
	if err := Deliver(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 3 || keys[0] != keys[2] || !slices.Equal(delays, []time.Duration{time.Second, 2 * time.Second}) {
		t.Fatalf("keys=%v delays=%v", keys, delays)
	}
}

func TestDeliverFailsAfterRetries(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	cfg := deliverConfig(t, server.URL)
	cfg.sleep = func(context.Context, time.Duration) error { return nil }
	if err := Deliver(context.Background(), cfg); err == nil || attempts != len(deliveryDelays)+1 {
		t.Fatalf("attempts=%d err=%v", attempts, err)
	}
}

func TestDeliverRejectsMissingSubmission(t *testing.T) {
	cfg := DeliverConfig{BatchID: "batch-1", OutputDir: t.TempDir()}
	if err := Deliver(context.Background(), cfg); err == nil {
		t.Fatal("missing submission accepted")
	}
}

func scanConfig(t *testing.T) ScanConfig {
	t.Helper()
	manifestDir := t.TempDir()
	manifest, err := orchestration.NewManifest("batch-1", []register.Repository{{ID: "repo-a", URL: "https://example.test/a.git"}})
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := orchestration.EncodeManifest(manifest, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	for _, chunk := range chunks {
		name := fmt.Sprintf("chunk-%04d-%s.json", chunk.Index, chunk.Digest)
		if err := os.WriteFile(filepath.Join(manifestDir, name), chunk.Data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return ScanConfig{
		BatchID: "batch-1", ManifestDir: manifestDir,
		Runner: runner.Config{OutputDir: t.TempDir(), ConfigDir: t.TempDir(), StageTimeout: time.Minute},
	}
}

func deliverConfig(t *testing.T, resultsURL string) DeliverConfig {
	t.Helper()
	outputDir := t.TempDir()
	data, err := json.Marshal(runner.Submission{SchemaVersion: 1, RepositoryID: "repo-a", Scan: runner.Report{Status: "completed"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outputDir, submissionFile), data, 0600); err != nil {
		t.Fatal(err)
	}
	return DeliverConfig{
		BatchID: "batch-1", OutputDir: outputDir, ResultsURL: resultsURL,
		Client: register.Client{HTTP: http.DefaultClient},
		sleep:  func(context.Context, time.Duration) error { t.Fatal("unexpected retry"); return nil },
	}
}

func readSubmission(t *testing.T, outputDir string) runner.Submission {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(outputDir, submissionFile))
	if err != nil {
		t.Fatal(err)
	}
	var submission runner.Submission
	if err := json.Unmarshal(data, &submission); err != nil {
		t.Fatal(err)
	}
	return submission
}
