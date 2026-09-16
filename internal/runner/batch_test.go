package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBatchCombinesDefaultAndMissingPublicCodeRepositories(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodGet || r.URL.Path != "/repositories" || r.URL.Query().Get("archived") != "false" ||
			r.URL.Query().Get("page") != "1" || r.URL.Query().Get("perPage") != "100" {
			t.Errorf("unexpected list request: %s %s", r.Method, r.URL)
		}
		w.Header().Set("Total-Pages", "1")
		switch values, present := r.URL.Query()["publiccode"]; {
		case !present:
			fmt.Fprint(w, `[{"id":"shared","url":"https://example.invalid/shared"},{"id":"default","url":"https://example.invalid/default"}]`)
		case len(values) == 1 && values[0] == "false":
			fmt.Fprint(w, `[{"id":"shared","url":"https://example.invalid/shared"},{"id":"missing","url":"https://example.invalid/missing"}]`)
		default:
			t.Errorf("unexpected publiccode filter: %v", values)
		}
	}))
	defer server.Close()

	scanned := make(map[string]int)
	got, err := runBatch(context.Background(), BatchConfig{
		RepositoriesURL: server.URL + "/repositories?archived=false&publiccode=true",
		HTTPTimeout:     time.Second,
		Runner:          Config{OutputDir: t.TempDir(), ConfigDir: "config", StageTimeout: time.Minute},
	}, func(_ context.Context, config Config) (Report, error) {
		scanned[config.Repository]++
		return Report{Status: "completed", Repository: config.Repository}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 || got.Total != 3 || len(got.Items) != 3 {
		t.Fatalf("repository sources not combined: requests=%d, report=%+v", requests, got)
	}
	for _, repositoryURL := range []string{
		"https://example.invalid/shared",
		"https://example.invalid/default",
		"https://example.invalid/missing",
	} {
		if scanned[repositoryURL] != 1 {
			t.Fatalf("repository %s scanned %d times", repositoryURL, scanned[repositoryURL])
		}
	}
}

func TestBatchPostsEachRepositoryAndContinuesAfterScanFailure(t *testing.T) {
	var posted []Submission
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			if r.Header.Get("X-Api-Key") != "read-key" || r.Header.Get("Authorization") != "" {
				t.Errorf("incorrect GET authentication")
			}
			w.Header().Set("Total-Pages", "1")
			fmt.Fprint(w, `[{"id":"repo-1","url":"https://example.invalid/one"},{"id":"repo-2","url":"https://example.invalid/two"}]`)
			return
		}
		if r.URL.Path != "/results" || r.Header.Get("X-Api-Key") != "" {
			t.Errorf("incorrect result destination or authentication")
		}
		var submission Submission
		if err := json.NewDecoder(r.Body).Decode(&submission); err != nil {
			t.Errorf("decode result: %v", err)
		}
		posted = append(posted, submission)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()
	cfg := BatchConfig{RepositoriesURL: server.URL + "/repositories", ResultsURL: server.URL + "/results",
		RegisterAPIKey: "read-key", HTTPTimeout: time.Second,
		Runner: Config{OutputDir: t.TempDir(), ConfigDir: "external-config", StageTimeout: time.Minute}}
	called := 0
	// Only the slow external scan is replaced; HTTP, iteration and persisted messages are real.
	scan := func(ctx context.Context, config Config) (Report, error) {
		called++
		if config.Revision != "" || config.ConfigDir != "external-config" || config.StageTimeout != time.Minute {
			t.Errorf("incorrect scan configuration: %+v", config)
		}
		if called == 1 {
			return Report{Status: "failed", Repository: config.Repository, Error: "checkout failed"}, errors.New("checkout failed")
		}
		return Report{Status: "completed", Repository: config.Repository, Revision: "0123456789012345678901234567890123456789",
			Findings: []Finding{{Rule: "MISSING_SECURITY_FILE", Severity: "ERROR", Message: "missing"}}}, nil
	}
	got, err := runBatch(context.Background(), cfg, scan)
	if err == nil || got.Status != "incomplete" || called != 2 || len(posted) != 2 {
		t.Fatalf("batch did not retain failures and continue: %+v, %v, scans=%d posts=%d", got, err, called, len(posted))
	}
	if posted[0].RepositoryID != "repo-1" || posted[0].Scan.Status != "failed" || posted[1].RepositoryID != "repo-2" || len(posted[1].Scan.Findings) != 1 {
		t.Fatalf("incorrect mapping of repository results: %+v", posted)
	}
	assertBatchFiles(t, got)
	for _, item := range got.Items {
		if item.Delivery != "posted" {
			t.Fatalf("delivery not recorded: %+v", item)
		}
	}
}

func TestBatchKeepsUnsentMessages(t *testing.T) {
	for _, postConfigured := range []bool{false, true} {
		t.Run(fmt.Sprint(postConfigured), func(t *testing.T) {
			posts := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "POST" {
					posts++
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				w.Header().Set("Total-Pages", "1")
				fmt.Fprint(w, `[{"id":"repo-1","url":"https://example.invalid/one"},{"id":"repo-2","url":"https://example.invalid/two"}]`)
			}))
			defer server.Close()
			cfg := BatchConfig{RepositoriesURL: server.URL, HTTPTimeout: time.Second, Runner: Config{OutputDir: t.TempDir(), ConfigDir: "config", StageTimeout: time.Minute}}
			if postConfigured {
				cfg.ResultsURL = server.URL
			}
			got, err := runBatch(context.Background(), cfg, func(ctx context.Context, config Config) (Report, error) {
				return Report{Status: "completed", Repository: config.Repository, Findings: []Finding{}}, nil
			})
			if (err != nil) != postConfigured || len(got.Items) != 2 {
				t.Fatalf("unexpected batch result: %+v, %v", got, err)
			}
			wantDelivery := "not_configured"
			if postConfigured {
				wantDelivery = "failed"
				if posts != 2 {
					t.Fatalf("POST errors stopped processing or were blindly retried: posts=%d", posts)
				}
			} else if posts != 0 {
				t.Fatal("POST attempted without a configured endpoint")
			}
			for _, item := range got.Items {
				if item.Delivery != wantDelivery {
					t.Fatalf("unsent result not recorded: %+v", item)
				}
			}
			assertBatchFiles(t, got)
		})
	}
}

func TestBatchStopsOnListFailureOrCancellation(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprint(canceled), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) }))
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if canceled {
				cancel()
			}
			got, err := runBatch(ctx, BatchConfig{RepositoriesURL: server.URL, HTTPTimeout: time.Second, Runner: Config{OutputDir: t.TempDir(), ConfigDir: "config", StageTimeout: time.Minute}}, func(context.Context, Config) (Report, error) {
				t.Fatal("scan started without a repository list")
				return Report{}, nil
			})
			if err == nil || got.Status != "failed" || len(got.Items) != 0 {
				t.Fatalf("list failure/cancellation ignored: %+v, %v", got, err)
			}
		})
	}
}

func TestBatchCancellationKeepsResultAndStopsBeforeNextRepository(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("POST attempted after cancellation")
		}
		w.Header().Set("Total-Pages", "1")
		fmt.Fprint(w, `[{"id":"repo-1","url":"https://example.invalid/one"},{"id":"repo-2","url":"https://example.invalid/two"}]`)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	got, err := runBatch(ctx, BatchConfig{RepositoriesURL: server.URL, ResultsURL: server.URL, HTTPTimeout: time.Second,
		Runner: Config{OutputDir: t.TempDir(), ConfigDir: "config", StageTimeout: time.Minute}}, func(context.Context, Config) (Report, error) {
		calls++
		cancel()
		return Report{Status: "failed", Error: "context canceled"}, context.Canceled
	})
	if !errors.Is(err, context.Canceled) || calls != 1 || got.Status != "failed" || len(got.Items) != 1 || got.Items[0].Delivery != "pending" {
		t.Fatalf("batch cancellation ignored: %+v, %v, calls=%d", got, err, calls)
	}
	assertBatchFiles(t, got)
}

func TestBatchRunsPipelineAndPublishesRecordedCommit(t *testing.T) {
	config := setup(t, "completed")
	write(t, filepath.Join(config.Repository, "README.md"), "requested commit\n")
	git(t, config.Repository, "commit", "-qam", "default branch scan fixture")
	commit := git(t, config.Repository, "rev-parse", "HEAD")
	// Route the test repository URL to a real local Git repository without changing host Git config.
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("GIT_CONFIG_KEY_0", "url."+config.Repository+".insteadOf")
	t.Setenv("GIT_CONFIG_VALUE_0", "https://example.invalid/fixture")
	var posted Submission
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			w.Header().Set("Total-Pages", "1")
			fmt.Fprint(w, `[{"id":"register-fixture","url":"https://example.invalid/fixture"}]`)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&posted); err != nil {
			t.Errorf("decode POST: %v", err)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()
	config.Repository, config.Revision = "", ""
	got, err := RunBatch(context.Background(), BatchConfig{RepositoriesURL: server.URL, ResultsURL: server.URL,
		HTTPTimeout: time.Second, Runner: config})
	if err != nil || got.Status != "completed" {
		t.Fatalf("actual batch pipeline failed: %+v, %v", got, err)
	}
	if posted.RepositoryID != "register-fixture" || posted.Scan.Revision != commit || len(posted.Scan.Findings) != 1 || posted.Scan.Findings[0].Rule != "MISSING_SECURITY_FILE" {
		t.Fatalf("POST does not contain the actual scan: %+v", posted)
	}
	if posted.Scan.Stages[3].ExitCode == nil || *posted.Scan.Stages[3].ExitCode != 2 {
		t.Fatal("evaluator findings not retained")
	}
	assertBatchFiles(t, got)
	for _, artifact := range []string{"run.json", "analyzer-result.yml", "advisor-result.yml", "evaluation-result.yml"} {
		if _, err := os.Stat(filepath.Join(posted.Scan.OutputDir, artifact)); err != nil {
			t.Fatal(err)
		}
	}
}

func assertBatchFiles(t *testing.T, report BatchReport) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(report.OutputDir, "batch.json"))
	if err != nil {
		t.Fatal(err)
	}
	var saved BatchReport
	if err := json.Unmarshal(data, &saved); err != nil || saved.Status != report.Status || saved.FinishedAt == nil {
		t.Fatalf("batch report not finalized: %s, %v", data, err)
	}
	for _, item := range report.Items {
		data, err := os.ReadFile(filepath.Join(report.OutputDir, item.Directory, "submission.json"))
		if err != nil {
			t.Fatal(err)
		}
		var submission Submission
		if err := json.Unmarshal(data, &submission); err != nil || submission.RepositoryID != item.RepositoryID || submission.SchemaVersion != 1 {
			t.Fatalf("submission not preserved with register identifier: %s, %v", data, err)
		}
	}
}
