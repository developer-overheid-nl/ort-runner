package runner

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func git(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func write(t *testing.T, path, data string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}

func setup(t *testing.T, scenario string) Config {
	t.Helper()
	dir := t.TempDir()
	source := filepath.Join(dir, "source repo")
	config := filepath.Join(dir, "config")
	for _, p := range []string{source, config} {
		if err := os.MkdirAll(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	write(t, filepath.Join(config, "evaluator.rules.kts"), "# test config\n")
	git(t, source, "init", "-q")
	git(t, source, "config", "user.name", "Runner Test")
	git(t, source, "config", "user.email", "runner@example.invalid")
	git(t, source, "config", "commit.gpgsign", "false")
	write(t, filepath.Join(source, "README.md"), "requested commit\n")
	git(t, source, "add", ".")
	git(t, source, "commit", "-qm", "fixture")
	revision := git(t, source, "rev-parse", "HEAD")
	write(t, filepath.Join(source, "README.md"), "newer commit\n")
	git(t, source, "commit", "-qam", "newer commit must not be scanned")
	binary := filepath.Join(dir, "fake-ort")
	script := `#!/bin/sh
set -eu
while [ "$#" -gt 0 ]; do
  case "$1" in
    analyze|advise|evaluate) stage="$1"; shift; break ;;
    *) shift ;;
  esac
done
rules=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -i) input="$2"; shift 2 ;;
    -o) output="$2"; shift 2 ;;
    --rules-file) rules="$2"; shift 2 ;;
    --license-classifications-file|--resolutions-file|--package-curations-dir|--package-configurations-dir)
      echo "unexpected obsolete config option: $1" >&2; exit 31 ;;
    *) shift ;;
  esac
done
printf 'running %s\n' "$stage"
case "$stage" in
  analyze)
    test "$(cat "$input/README.md")" = 'requested commit'
    SCENARIO_ACTION
    cat > "$output/analyzer-result.yml" <<'YAML'
analyzer:
  environment: {ort_version: 92.4.0}
  result:
    packages: []
    issues: ANALYZER_ISSUES
YAML
    exit ANALYZER_EXIT ;;
  advise)
    test -s "$input"
    cat > "$output/advisor-result.yml" <<'YAML'
advisor:
  environment: {ort_version: 92.4.0}
  provider_issues: []
  results: ADVISOR_RESULTS
YAML
    ;;
  evaluate)
    test -s "$input"
    test -s "$rules"
    cat > "$output/evaluation-result.yml" <<'YAML'
evaluator:
  environment: {ort_version: 92.4.0}
  violations:
  - {rule: MISSING_SECURITY_FILE, severity: ERROR, message: missing}
YAML
    exit 2 ;;
esac
`
	action, issues, code, advisorResults := ":", "{}", "0", "{}"
	switch scenario {
	case "incomplete":
		issues, code = "{'GoMod::example:': [{severity: ERROR, message: download failed}]}", "2"
	case "vulnerable":
		advisorResults = `
    "Go::example.org/module:1.0.0":
    - summary: {issues: []}
      vulnerabilities:
      - {id: GO-2026-1234, summary: Example vulnerability, first_fixed_versions: [1.1.0]}`
	case "missing":
		action = "exit 0"
	case "timeout":
		action = "sleep 60"
	}
	script = strings.NewReplacer(
		"SCENARIO_ACTION", action,
		"ANALYZER_ISSUES", issues,
		"ANALYZER_EXIT", code,
		"ADVISOR_RESULTS", advisorResults,
	).Replace(script)
	if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	return Config{Repository: source, Revision: revision, ConfigDir: config, OutputDir: filepath.Join(dir, "output"), ORTBinary: binary, StageTimeout: 10 * time.Second, RunnerVersion: "test"}
}

func TestRunKeepsAdvisorVulnerabilities(t *testing.T) {
	got, err := Run(context.Background(), setup(t, "vulnerable"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Vulnerabilities) != 1 || got.Vulnerabilities[0].ID != "GO-2026-1234" {
		t.Fatalf("advisor vulnerabilities missing from report: %#v", got.Vulnerabilities)
	}

	data, err := os.ReadFile(filepath.Join(got.OutputDir, "run.json"))
	if err != nil {
		t.Fatal(err)
	}
	var saved Report
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	if len(saved.Vulnerabilities) != 1 || saved.Vulnerabilities[0].PackageID != "Go::example.org/module:1.0.0" {
		t.Fatalf("advisor vulnerabilities missing from saved report: %#v", saved.Vulnerabilities)
	}
}

func TestRunChecksOutRequestedCommitAndKeepsFindings(t *testing.T) {
	cfg := setup(t, "completed")
	got, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "completed" || got.Revision != cfg.Revision || len(got.Findings) != 1 {
		t.Fatalf("unexpected report: %+v", got)
	}
	if len(got.Stages) != 4 || got.Stages[3].ExitCode == nil || *got.Stages[3].ExitCode != 2 {
		t.Fatalf("evaluation exit code not retained: %+v", got.Stages)
	}
	for _, file := range []string{"checkout.log", "analyze.log", "advise.log", "evaluate.log", "analyzer-result.yml", "advisor-result.yml", "evaluation-result.yml"} {
		if _, err := os.Stat(filepath.Join(got.OutputDir, file)); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(filepath.Join(got.OutputDir, "run.json"))
	if err != nil {
		t.Fatal(err)
	}
	var saved Report
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.Status != "completed" || saved.FinishedAt == nil {
		t.Fatalf("incomplete saved report: %+v", saved)
	}
	if saved.ConfigDigest == "" {
		t.Fatal("configuration fingerprint missing")
	}
	again, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got.OutputDir == again.OutputDir {
		t.Fatal("second run overwrote first run")
	}
	if got.ConfigDigest != again.ConfigDigest {
		t.Fatal("unchanged config has a different fingerprint")
	}
	write(t, filepath.Join(cfg.ConfigDir, "README.md"), "not used by ORT\n")
	withUnrelatedFile, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got.ConfigDigest != withUnrelatedFile.ConfigDigest {
		t.Fatal("unrelated files changed the evaluator configuration fingerprint")
	}
	write(t, filepath.Join(cfg.ConfigDir, "evaluator.rules.kts"), "# changed rules\n")
	changed, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got.ConfigDigest == changed.ConfigDigest {
		t.Fatal("changed rules were not recorded")
	}
}

func TestIncompleteAnalysisStillAllowsRepositoryEvaluation(t *testing.T) {
	got, err := Run(context.Background(), setup(t, "incomplete"))
	if err == nil || got.Status != "incomplete" {
		t.Fatalf("incomplete analysis hidden: %+v, %v", got, err)
	}
	if len(got.Findings) != 1 || got.Stages[3].Status != "completed" {
		t.Fatalf("evaluation did not finish: %+v", got)
	}
}

func TestMissingAnalyzerArtifactStopsDependentStages(t *testing.T) {
	got, err := Run(context.Background(), setup(t, "missing"))
	if err == nil || got.Status != "failed" {
		t.Fatalf("missing output accepted: %+v, %v", got, err)
	}
	if got.Stages[2].Status != "skipped" || got.Stages[3].Status != "skipped" {
		t.Fatalf("dependent stages ran: %+v", got.Stages)
	}
	if _, err := os.Stat(filepath.Join(got.OutputDir, "run.json")); err != nil {
		t.Fatal(err)
	}
}

func TestTimeoutStopsProcessAndKeepsReport(t *testing.T) {
	cfg := setup(t, "timeout")
	cfg.StageTimeout = time.Second
	start := time.Now()
	got, err := Run(context.Background(), cfg)
	if err == nil || got.Status != "failed" {
		t.Fatalf("timeout accepted: %+v, %v", got, err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("timed-out subprocess kept runner alive")
	}
	if len(got.Stages) < 2 || !strings.Contains(got.Stages[1].Error, "deadline") {
		t.Fatalf("timeout not recorded: %+v", got)
	}
}

func TestRejectsBranchInsteadOfExactRevision(t *testing.T) {
	cfg := setup(t, "completed")
	cfg.Revision = "main"
	if _, err := Run(context.Background(), cfg); err == nil {
		t.Fatal("mutable branch accepted as revision")
	}
}

func TestRelativeLocalPaths(t *testing.T) {
	cfg := setup(t, "completed")
	t.Chdir(filepath.Dir(cfg.Repository))
	cfg.Repository = filepath.Base(cfg.Repository)
	cfg.ORTBinary = "./" + filepath.Base(cfg.ORTBinary)
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
}

func TestPreparationFailureIsRecorded(t *testing.T) {
	cfg := setup(t, "completed")
	if err := os.Remove(filepath.Join(cfg.ConfigDir, "evaluator.rules.kts")); err != nil {
		t.Fatal(err)
	}
	got, err := Run(context.Background(), cfg)
	if err == nil || got.Status != "failed" {
		t.Fatalf("invalid configuration accepted: %+v, %v", got, err)
	}
	data, err := os.ReadFile(filepath.Join(got.OutputDir, "run.json"))
	if err != nil {
		t.Fatal(err)
	}
	var saved map[string]any
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	if message, ok := saved["error"].(string); !ok || !strings.Contains(message, "prepare configuration") {
		t.Fatalf("preparation error missing from run report: %s", data)
	}
}

func TestCheckoutIncludesSubmodules(t *testing.T) {
	cfg := setup(t, "completed")
	submodule := t.TempDir()
	git(t, submodule, "init", "-q")
	git(t, submodule, "config", "user.name", "Runner Test")
	git(t, submodule, "config", "user.email", "runner@example.invalid")
	git(t, submodule, "config", "commit.gpgsign", "false")
	write(t, filepath.Join(submodule, "go.mod"), "module example.invalid/submodule\n")
	git(t, submodule, "add", ".")
	git(t, submodule, "commit", "-qm", "submodule fixture")
	// Permit the local fixture transport only for this test.
	t.Setenv("GIT_ALLOW_PROTOCOL", "file")
	git(t, cfg.Repository, "submodule", "add", submodule, "vendor/lib")
	write(t, filepath.Join(cfg.Repository, "README.md"), "requested commit\n")
	git(t, cfg.Repository, "commit", "-qam", "include submodule")
	cfg.Revision = git(t, cfg.Repository, "rev-parse", "HEAD")
	script, err := os.ReadFile(cfg.ORTBinary)
	if err != nil {
		t.Fatal(err)
	}
	updated := strings.Replace(string(script), "analyze)\n", "analyze)\n    test -s \"$input/vendor/lib/go.mod\"\n", 1)
	write(t, cfg.ORTBinary, updated)
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
}

func TestRunResolvesDefaultBranchToRecordedCommit(t *testing.T) {
	cfg := setup(t, "completed")
	git(t, cfg.Repository, "branch", "-M", "fixture-default")
	write(t, filepath.Join(cfg.Repository, "README.md"), "requested commit\n")
	git(t, cfg.Repository, "commit", "-qam", "default branch fixture")
	want := git(t, cfg.Repository, "rev-parse", "HEAD")
	cfg.Revision = ""
	got, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != want {
		t.Fatalf("actual commit not recorded: got %q, want %q", got.Revision, want)
	}
}

func TestRegisterCredentialsDoNotReachScannedProcesses(t *testing.T) {
	cfg := setup(t, "completed")
	for _, name := range []string{"ORT_REGISTER_API_KEY", "AUTH_TOKEN_URL", "AUTH_CLIENT_ID", "AUTH_CLIENT_SECRET", "AUTH_SCOPES"} {
		t.Setenv(name, "test-credential")
	}
	script, err := os.ReadFile(cfg.ORTBinary)
	if err != nil {
		t.Fatal(err)
	}
	guard := "set -eu\ntest -z \"${ORT_REGISTER_API_KEY:-}${AUTH_TOKEN_URL:-}${AUTH_CLIENT_ID:-}${AUTH_CLIENT_SECRET:-}${AUTH_SCOPES:-}\""
	write(t, cfg.ORTBinary, strings.Replace(string(script), "set -eu", guard, 1))
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
}
