package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// Run scans the requested commit, or fetches the default branch when Revision is empty.
// In both cases the actual checked-out commit is recorded in the report.
func Run(ctx context.Context, cfg Config) (report Report, runErr error) {
	if cfg.Revision != "" && !regexp.MustCompile(`^[a-fA-F0-9]{40}$`).MatchString(cfg.Revision) {
		return report, fmt.Errorf("revision must be a full 40-character Git commit SHA")
	}
	if cfg.Repository == "" || strings.HasPrefix(cfg.Repository, "-") {
		return report, fmt.Errorf("repository URL or local path is required")
	}
	if cfg.ConfigDir == "" || cfg.OutputDir == "" || cfg.StageTimeout <= 0 {
		return report, fmt.Errorf("config directory, output directory and positive stage timeout are required")
	}
	if cfg.ORTBinary == "" {
		cfg.ORTBinary = "ort"
	}
	if info, err := os.Stat(cfg.Repository); err == nil && info.IsDir() {
		cfg.Repository, err = filepath.Abs(cfg.Repository)
		if err != nil {
			return report, err
		}
	}
	if strings.ContainsRune(cfg.ORTBinary, filepath.Separator) {
		var err error
		cfg.ORTBinary, err = filepath.Abs(cfg.ORTBinary)
		if err != nil {
			return report, err
		}
	}
	cfg.Revision = strings.ToLower(cfg.Revision)
	output, err := filepath.Abs(cfg.OutputDir)
	if err != nil {
		return report, err
	}
	if err := os.MkdirAll(output, 0750); err != nil {
		return report, err
	}
	dir, err := os.MkdirTemp(output, "run-")
	if err != nil {
		return report, err
	}
	report = Report{
		SchemaVersion: 1, Status: "running", Repository: cfg.Repository, Revision: cfg.Revision,
		ORTImage: cfg.ORTImage, RunnerVersion: cfg.RunnerVersion, OutputDir: dir,
		StartedAt: time.Now().UTC(), Findings: []Finding{}, Vulnerabilities: []Vulnerability{},
		Stages: []Stage{{Name: "checkout", Status: "pending"}, {Name: "analyze", Status: "pending"},
			{Name: "advise", Status: "pending"}, {Name: "evaluate", Status: "pending"}},
	}
	defer func() {
		if runErr != nil {
			report.Error = runErr.Error()
		}
		if report.Status == "running" {
			report.Status = "failed"
		}
		finished := time.Now().UTC()
		report.FinishedAt = &finished
		for i := range report.Stages {
			if report.Stages[i].Status == "pending" {
				report.Stages[i].Status = "skipped"
			}
		}
		runErr = errors.Join(runErr, saveReport(report))
	}()
	if err := saveReport(report); err != nil {
		return report, err
	}
	work, err := os.MkdirTemp("", "ort-runner-")
	if err != nil {
		return report, err
	}
	defer func() {
		if err := os.RemoveAll(work); err != nil {
			slog.Warn("Temporary checkout cleanup failed", "error", err)
		}
	}()
	config := filepath.Join(work, "config")
	report.ConfigDigest, err = snapshotConfig(cfg.ConfigDir, config)
	if err != nil {
		return report, fmt.Errorf("prepare configuration: %w", err)
	}
	project := filepath.Join(work, "project")
	if err := os.Mkdir(project, 0700); err != nil {
		return report, err
	}

	stage := &report.Stages[0]
	startStage(stage)
	stage.Log = "checkout.log"
	checkoutCtx, cancel := context.WithTimeout(ctx, cfg.StageTimeout)
	revision, code, err := checkout(checkoutCtx, cfg, project, filepath.Join(dir, stage.Log))
	cancel()
	if revision != "" {
		report.Revision = revision
	}
	finishStage(stage, code, err)
	if err != nil {
		return report, fmt.Errorf("checkout failed: %w", err)
	}
	if err := saveReport(report); err != nil {
		return report, err
	}

	artifacts := []string{"analyzer-result.yml", "advisor-result.yml", "evaluation-result.yml"}
	for i, artifact := range artifacts {
		stage = &report.Stages[i+1]
		startStage(stage)
		stage.Log, stage.Result = stage.Name+".log", artifact
		args := []string{"-P", "ort.forceOverwrite=true", "--stacktrace", stage.Name}
		switch stage.Name {
		case "analyze":
			args = append(args, "-i", project, "-o", dir, "-f", "YAML")
		case "advise":
			args = append(args, "-i", filepath.Join(dir, artifacts[0]), "-o", dir, "-a", "OSV", "-f", "YAML")
		case "evaluate":
			args = append(args, "-i", filepath.Join(dir, artifacts[1]), "-o", dir,
				"--rules-file", filepath.Join(config, "evaluator.rules.kts"),
				"-f", "YAML")
		}
		stageCtx, cancel := context.WithTimeout(ctx, cfg.StageTimeout)
		code, commandErr := runCommand(stageCtx, cfg.ORTBinary, args, work,
			[]string{"ORT_CONFIG_DIR=" + config, "GIT_TERMINAL_PROMPT=0"}, filepath.Join(dir, stage.Log))
		cancel()
		var info resultInfo
		if commandErr == nil || code == 2 {
			info, err = readResult(filepath.Join(dir, artifact), stage.Name, code)
		} else {
			err = commandErr
		}
		stage.Status, stage.ORTVersion = info.Status, info.Version
		finishStage(stage, code, err)
		if err != nil {
			return report, fmt.Errorf("%s failed: %w", stage.Name, err)
		}
		if stage.Name == "analyze" {
			report.PackageCount = &info.Packages
		}
		if stage.Name == "advise" {
			report.Vulnerabilities = append(report.Vulnerabilities, info.Vulnerabilities...)
		}
		if stage.Name == "evaluate" {
			report.Findings = append(report.Findings, info.Findings...)
		}
		if err := saveReport(report); err != nil {
			return report, err
		}
	}
	report.Status = "completed"
	for _, stage := range report.Stages {
		if stage.Status == "incomplete" {
			report.Status = "incomplete"
			runErr = fmt.Errorf("ORT reported incomplete analysis or advice; see stage logs and result files")
		}
	}
	return report, runErr
}

func startStage(stage *Stage) {
	now := time.Now().UTC()
	stage.StartedAt, stage.Status = &now, "running"
	slog.Info("Stage started", "stage", stage.Name)
}

func finishStage(stage *Stage, code int, err error) {
	now := time.Now().UTC()
	stage.FinishedAt, stage.ExitCode = &now, &code
	if stage.Status != "incomplete" {
		stage.Status = "completed"
	}
	if err != nil {
		stage.Status, stage.Error = "failed", err.Error()
	}
	slog.Info("Stage command finished", "stage", stage.Name, "exit_code", code, "status", stage.Status)
}

func saveReport(report Report) error {
	_, err := saveJSON(filepath.Join(report.OutputDir, "run.json"), report)
	return err
}

func saveJSON(target string, value any) ([]byte, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	data = append(data, '\n')
	if err := os.WriteFile(target+".tmp", data, 0600); err != nil {
		return nil, err
	}
	return data, os.Rename(target+".tmp", target)
}
