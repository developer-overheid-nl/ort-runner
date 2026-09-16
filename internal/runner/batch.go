package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/developer-overheid-nl/ort-runner/internal/register"
)

func RunBatch(ctx context.Context, cfg BatchConfig) (BatchReport, error) {
	return runBatch(ctx, cfg, Run)
}

func loadRepositorySets(ctx context.Context, source register.Client, endpoint string) ([]register.Repository, error) {
	baseURL, err := register.ParseURL(endpoint)
	if err != nil {
		return nil, err
	}
	baseQuery := baseURL.Query()
	baseQuery.Del("publiccode")
	baseURL.RawQuery = baseQuery.Encode()
	missingPublicCodeURL := *baseURL
	query := missingPublicCodeURL.Query()
	query.Set("publiccode", "false")
	missingPublicCodeURL.RawQuery = query.Encode()

	endpoints := []string{baseURL.String()}
	if filtered := missingPublicCodeURL.String(); filtered != endpoints[0] {
		endpoints = append(endpoints, filtered)
	}
	repositories := make([]register.Repository, 0)
	seen := make(map[string]string)
	for _, repositoryEndpoint := range endpoints {
		items, err := source.Repositories(ctx, repositoryEndpoint)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			if existingURL, ok := seen[item.ID]; ok {
				if existingURL != item.URL {
					return nil, fmt.Errorf("repository %s has conflicting URLs across register filters", item.ID)
				}
				continue
			}
			seen[item.ID] = item.URL
			repositories = append(repositories, item)
		}
	}
	return repositories, nil
}

func runBatch(ctx context.Context, cfg BatchConfig, scan func(context.Context, Config) (Report, error)) (report BatchReport, batchErr error) {
	if cfg.Runner.Repository != "" || cfg.Runner.Revision != "" {
		return report, fmt.Errorf("batch mode cannot be combined with a single repository or revision")
	}
	if cfg.HTTPTimeout <= 0 || cfg.Runner.StageTimeout <= 0 || cfg.Runner.OutputDir == "" || cfg.Runner.ConfigDir == "" {
		return report, fmt.Errorf("output/config directories and positive HTTP/stage timeouts are required")
	}
	if _, err := register.ParseURL(cfg.RepositoriesURL); err != nil {
		return report, err
	}
	if cfg.ResultsURL != "" {
		if _, err := register.ParseURL(cfg.ResultsURL); err != nil {
			return report, err
		}
	}
	root, err := filepath.Abs(cfg.Runner.OutputDir)
	if err != nil {
		return report, err
	}
	if err := os.MkdirAll(root, 0750); err != nil {
		return report, err
	}
	dir, err := os.MkdirTemp(root, "batch-")
	if err != nil {
		return report, err
	}
	report = BatchReport{Status: "running", OutputDir: dir, StartedAt: time.Now().UTC(), Items: []BatchItem{}}
	save := func() error {
		_, err := saveJSON(filepath.Join(dir, "batch.json"), report)
		return err
	}
	defer func() {
		if report.Status == "running" {
			report.Status = "failed"
		}
		if batchErr != nil {
			report.Error = batchErr.Error()
		}
		finished := time.Now().UTC()
		report.FinishedAt = &finished
		batchErr = errors.Join(batchErr, save())
	}()
	if err := save(); err != nil {
		return report, err
	}
	client := &http.Client{Timeout: cfg.HTTPTimeout}
	registerHTTPClient := cfg.RegisterHTTPClient
	if registerHTTPClient == nil {
		registerHTTPClient = client
	}
	resultsHTTPClient := cfg.ResultsHTTPClient
	if resultsHTTPClient == nil {
		resultsHTTPClient = client
	}
	source := register.Client{HTTP: registerHTTPClient, APIKey: cfg.RegisterAPIKey}
	destination := register.Client{HTTP: resultsHTTPClient}
	// Read all pages before lengthy scans, keeping list retrieval close together in time.
	repositories, err := loadRepositorySets(ctx, source, cfg.RepositoriesURL)
	if err != nil {
		return report, fmt.Errorf("read register: %w", err)
	}
	report.Total = len(repositories)
	slog.Info("Repository list loaded", "repositories", report.Total, "posting_enabled", cfg.ResultsURL != "")
	hasFailures := false
	for _, repository := range repositories {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		itemDir, err := os.MkdirTemp(dir, "repository-")
		if err != nil {
			return report, err
		}
		item := BatchItem{RepositoryID: repository.ID, RepositoryURL: repository.URL,
			Directory: filepath.Base(itemDir), ScanStatus: "running", Delivery: "pending"}
		if cfg.ResultsURL == "" {
			item.Delivery = "not_configured"
		}
		report.Items = append(report.Items, item)
		if err := save(); err != nil {
			return report, err
		}
		slog.Info("Repository scan started", "repository_id", repository.ID)
		scanCfg := cfg.Runner
		scanCfg.Repository, scanCfg.OutputDir = repository.URL, itemDir
		result, scanErr := scan(ctx, scanCfg)
		if result.Status == "" {
			result.Status, result.Repository = "failed", repository.URL
		}
		if scanErr != nil {
			result.Error = scanErr.Error()
			item.Error = scanErr.Error()
		}
		item.ScanStatus = result.Status
		report.Items[len(report.Items)-1] = item
		// Save exactly the JSON that is sent, even for a failed scan or an unavailable POST endpoint.
		data, err := saveJSON(filepath.Join(itemDir, "submission.json"), Submission{SchemaVersion: 1, RepositoryID: repository.ID, Scan: result})
		if err != nil {
			return report, err
		}
		if err := ctx.Err(); err != nil {
			return report, err
		}
		if cfg.ResultsURL != "" {
			if err := destination.Post(ctx, cfg.ResultsURL, data); err != nil {
				item.Delivery = "failed"
				item.Error = errors.Join(scanErr, err).Error()
			} else {
				item.Delivery = "posted"
			}
		}
		if scanErr != nil || item.ScanStatus != "completed" || item.Delivery == "failed" {
			hasFailures = true
		}
		report.Items[len(report.Items)-1] = item
		if err := save(); err != nil {
			return report, err
		}
		slog.Info("Repository processed", "repository_id", repository.ID, "scan_status", item.ScanStatus, "delivery", item.Delivery)
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}
	report.Status = "completed"
	if hasFailures {
		report.Status = "incomplete"
		batchErr = fmt.Errorf("one or more scans or result deliveries failed; see batch.json")
	}
	return report, batchErr
}
