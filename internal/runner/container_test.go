//go:build integration

package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// This test exercises the actual ORT image and the separately versioned baseline rules.
// The fixture has no dependencies: networked vulnerability coverage needs its own test repositories.
func TestContainerBaseline(t *testing.T) {
	config := os.Getenv("ORT_RUNNER_TEST_CONFIG_DIR")
	if config == "" {
		t.Fatal("set ORT_RUNNER_TEST_CONFIG_DIR to an ort-config checkout")
	}
	config, err := filepath.Abs(config)
	if err != nil {
		t.Fatal(err)
	}
	image := os.Getenv("ORT_RUNNER_TEST_IMAGE")
	if image == "" {
		image = "ort-runner:dev"
	}
	for _, securityPresent := range []bool{false, true} {
		t.Run(fmt.Sprintf("security_present_%t", securityPresent), func(t *testing.T) {
			root := t.TempDir()
			repo, output := filepath.Join(root, "repo"), filepath.Join(root, "output")
			for _, dir := range []string{filepath.Join(repo, ".github", "workflows"), output} {
				if err := os.MkdirAll(dir, 0755); err != nil {
					t.Fatal(err)
				}
			}
			files := map[string]string{
				"README.md":                "# ORT runner integration fixture\n",
				"LICENSE":                  "SPDX-License-Identifier: MIT\n",
				"publiccode.yml":           "publiccodeYmlVersion: '0.5'\nname: ort-runner-fixture\n",
				"CONTRIBUTING.md":          "# Contributing\n",
				"CODE_OF_CONDUCT.md":       "# Code of conduct\n",
				"CHANGELOG":                "Initial fixture.\n",
				".github/workflows/ci.yml": "name: Fixture\non: workflow_dispatch\njobs: {}\n",
			}
			if securityPresent {
				files["SECURITY.md"] = "# Reporting a vulnerability\n"
			}
			for name, data := range files {
				write(t, filepath.Join(repo, name), data)
			}
			git(t, repo, "init", "-q")
			git(t, repo, "config", "user.name", "Runner Test")
			git(t, repo, "config", "user.email", "runner@example.invalid")
			git(t, repo, "config", "commit.gpgsign", "false")
			git(t, repo, "add", ".")
			git(t, repo, "commit", "-qm", "baseline fixture")
			revision := git(t, repo, "rev-parse", "HEAD")
			name := fmt.Sprintf("ort-runner-test-%d", time.Now().UnixNano())
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				// Only remove this test's container, including when the test command times out.
				_ = exec.CommandContext(ctx, "docker", "rm", "-f", name).Run()
			})
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, "docker", "run", "--rm", "--name", name,
				"--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()), "-e", "HOME=/tmp",
				"-v", repo+":/fixture/repo:ro", "-v", config+":/config:ro", "-v", output+":/output",
				image, "--repository", "file:///fixture/repo",
				"--stage-timeout", "2m")
			console, commandErr := cmd.CombinedOutput()
			runs, err := filepath.Glob(filepath.Join(output, "run-*"))
			if err != nil || len(runs) != 1 {
				t.Fatalf("expected one output directory: %v, %v; container: %v\n%s", runs, err, commandErr, console)
			}
			t.Cleanup(func() {
				if t.Failed() {
					for _, file := range []string{"run.json", "checkout.log", "analyze.log", "advise.log", "evaluate.log"} {
						data, _ := os.ReadFile(filepath.Join(runs[0], file))
						t.Logf("%s:\n%s", file, data)
					}
				}
			})
			if commandErr != nil {
				t.Fatalf("container failed: %v\n%s", commandErr, console)
			}
			data, err := os.ReadFile(filepath.Join(runs[0], "run.json"))
			if err != nil {
				t.Fatal(err)
			}
			var report Report
			if err := json.Unmarshal(data, &report); err != nil {
				t.Fatal(err)
			}
			if report.Status != "completed" || report.Revision != revision || report.ConfigDigest == "" || len(report.Stages) != 4 {
				t.Fatalf("unexpected report: %s", data)
			}
			for _, stage := range report.Stages {
				if stage.Status != "completed" || stage.ExitCode == nil {
					t.Fatalf("stage did not complete: %+v", stage)
				}
				if stage.Result != "" {
					if _, err := os.Stat(filepath.Join(runs[0], stage.Result)); err != nil {
						t.Fatal(err)
					}
				}
			}
			if securityPresent {
				if len(report.Findings) != 0 || *report.Stages[3].ExitCode != 0 {
					t.Fatalf("unexpected baseline findings: %+v", report.Findings)
				}
			} else if len(report.Findings) != 1 || report.Findings[0].Rule != "MISSING_SECURITY_FILE" || *report.Stages[3].ExitCode != 2 {
				t.Fatalf("missing security file not reported correctly: %s", data)
			}
		})
	}
}
