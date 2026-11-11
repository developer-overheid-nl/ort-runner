package worker

import (
	"bytes"
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

func TestScanRecordsTimeoutAsFailedResult(t *testing.T) {
	cfg := scanConfig(t)
	cfg.Timeout = 10 * time.Millisecond
	cfg.scan = func(ctx context.Context, scan runner.Config) (runner.Report, error) {
		<-ctx.Done()
		return runner.Report{Status: "failed", Repository: scan.Repository}, ctx.Err()
	}
	if _, err := Scan(context.Background(), cfg); err != nil {
		t.Fatalf("timeout failed the worker, Kubernetes would retry it: %v", err)
	}
	submission := readSubmission(t, cfg.Runner.OutputDir)
	if submission.Scan.Status != "failed" || submission.Scan.Error != "repository scan exceeded the 10ms time budget" {
		t.Fatalf("submission=%+v", submission.Scan)
	}
}

func TestScanFailsWorkerWhenInterrupted(t *testing.T) {
	cfg := scanConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	cfg.scan = func(context.Context, runner.Config) (runner.Report, error) {
		cancel()
		return runner.Report{Status: "failed"}, context.Canceled
	}
	if _, err := Scan(ctx, cfg); err == nil {
		t.Fatal("interrupted scan reported as repository result")
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
	cfg := deliverConfig(t, "")
	if err := os.Remove(filepath.Join(cfg.OutputDir, submissionFile)); err != nil {
		t.Fatal(err)
	}
	if err := Deliver(context.Background(), cfg); err == nil {
		t.Fatal("missing submission accepted")
	}
}

// The scan container runs code from the scanned repository and can write the
// shared output directory, so the submission must not decide which repository
// a result belongs to.
func TestDeliverRejectsSubmissionForOtherRepository(t *testing.T) {
	posted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posted = true
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	cfg := deliverConfig(t, server.URL)
	writeSubmission(t, cfg.OutputDir, runner.Submission{SchemaVersion: 1, RepositoryID: "repo-b", Scan: runner.Report{Status: "completed"}})
	if err := Deliver(context.Background(), cfg); err == nil || posted {
		t.Fatalf("submission for another repository delivered: posted=%t err=%v", posted, err)
	}
}

func TestDeliverRejectsInvalidManifestSelection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*DeliverConfig)
	}{
		{name: "negative index", mutate: func(cfg *DeliverConfig) { cfg.CompletionIndex = -1 }},
		{name: "out of range", mutate: func(cfg *DeliverConfig) { cfg.CompletionIndex = 1 }},
		{name: "batch mismatch", mutate: func(cfg *DeliverConfig) { cfg.BatchID = "other" }},
		{name: "missing manifest", mutate: func(cfg *DeliverConfig) { cfg.ManifestDir = t.TempDir() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := deliverConfig(t, "")
			tc.mutate(&cfg)
			if err := Deliver(context.Background(), cfg); err == nil {
				t.Fatal("invalid manifest selection accepted")
			}
		})
	}
}

func TestDeliverRejectsOversizedSubmission(t *testing.T) {
	cfg := deliverConfig(t, "")
	// Valid JSON padded with whitespace, so only the size can reject it.
	data, err := json.Marshal(runner.Submission{SchemaVersion: 1, RepositoryID: "repo-a", Scan: runner.Report{Status: "completed"}})
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, bytes.Repeat([]byte(" "), maxSubmissionBytes+1-len(data))...)
	if err := os.WriteFile(filepath.Join(cfg.OutputDir, submissionFile), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := Deliver(context.Background(), cfg); err == nil {
		t.Fatal("oversized submission accepted")
	}
}

func scanConfig(t *testing.T) ScanConfig {
	t.Helper()
	return ScanConfig{
		BatchID: "batch-1", ManifestDir: writeManifest(t), Timeout: time.Minute,
		Runner: runner.Config{OutputDir: t.TempDir(), ConfigDir: t.TempDir(), StageTimeout: time.Minute},
	}
}

func writeManifest(t *testing.T) string {
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
	return manifestDir
}

func deliverConfig(t *testing.T, resultsURL string) DeliverConfig {
	t.Helper()
	outputDir := t.TempDir()
	writeSubmission(t, outputDir, runner.Submission{SchemaVersion: 1, RepositoryID: "repo-a", Scan: runner.Report{Status: "completed"}})
	return DeliverConfig{
		BatchID: "batch-1", CompletionIndex: 0, ManifestDir: writeManifest(t), OutputDir: outputDir, ResultsURL: resultsURL,
		Client: register.Client{HTTP: http.DefaultClient},
		sleep:  func(context.Context, time.Duration) error { t.Fatal("unexpected retry"); return nil },
	}
}

func writeSubmission(t *testing.T, outputDir string, submission runner.Submission) {
	t.Helper()
	data, err := json.Marshal(submission)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outputDir, submissionFile), data, 0600); err != nil {
		t.Fatal(err)
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
