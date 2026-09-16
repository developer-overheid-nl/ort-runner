package runner

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAdvisorVulnerabilitiesAreReturned(t *testing.T) {
	file := filepath.Join(t.TempDir(), "advisor-result.yml")
	data := `advisor:
  environment: {ort_version: 92.4.0}
  provider_issues: []
  results:
    "Go::example.org/module:1.0.0":
    - advisor: {name: OSV}
      summary: {issues: []}
      vulnerabilities:
      - id: GO-2026-1234
        summary: Example vulnerability
        references:
        - url: https://example.org/GO-2026-1234
          scoring_system: CVSS3
          severity: HIGH
          score: 7.5
          vector: CVSS:3.1/example
        first_fixed_versions: [1.1.0]
`
	if err := os.WriteFile(file, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}

	got, err := readResult(file, "advise", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Vulnerabilities) != 1 {
		t.Fatalf("vulnerabilities = %#v, want one", got.Vulnerabilities)
	}
	want := Vulnerability{
		PackageID: "Go::example.org/module:1.0.0", ID: "GO-2026-1234",
		Summary: "Example vulnerability", Severity: "HIGH", Score: float64Pointer(7.5),
		ScoringSystem: "CVSS3", FirstFixedVersions: []string{"1.1.0"},
	}
	if diff := vulnerabilityDiff(got.Vulnerabilities[0], want); diff != "" {
		t.Fatal(diff)
	}
}

func TestAdvisorVulnerabilitiesHaveStableOrder(t *testing.T) {
	file := filepath.Join(t.TempDir(), "advisor-result.yml")
	data := `advisor:
  environment: {ort_version: 92.4.0}
  results:
    "Go::z.example/module:1.0.0": [{vulnerabilities: [{id: Z-2}, {id: Z-1}]}]
    "Go::m.example/module:1.0.0": [{vulnerabilities: [{id: M-1}]}]
    "Go::a.example/module:1.0.0": [{vulnerabilities: [{id: A-1}]}]
`
	if err := os.WriteFile(file, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}

	want := []string{"Go::a.example/module:1.0.0/A-1", "Go::m.example/module:1.0.0/M-1", "Go::z.example/module:1.0.0/Z-1", "Go::z.example/module:1.0.0/Z-2"}
	for range 20 {
		got, err := readResult(file, "advise", 0)
		if err != nil {
			t.Fatal(err)
		}
		for index, vulnerability := range got.Vulnerabilities {
			if value := vulnerability.PackageID + "/" + vulnerability.ID; value != want[index] {
				t.Fatalf("vulnerability %d = %q, want %q", index, value, want[index])
			}
		}
	}
}

func float64Pointer(value float64) *float64 { return &value }

func vulnerabilityDiff(got, want Vulnerability) string {
	if got.PackageID != want.PackageID || got.ID != want.ID || got.Summary != want.Summary ||
		got.Severity != want.Severity || got.ScoringSystem != want.ScoringSystem ||
		got.Score == nil || want.Score == nil || *got.Score != *want.Score ||
		len(got.FirstFixedVersions) != 1 || got.FirstFixedVersions[0] != want.FirstFixedVersions[0] {
		return "vulnerability does not match expected value"
	}
	return ""
}

func TestResultSeparatesFindingsFromExecutionFailures(t *testing.T) {
	tests := []struct {
		name, stage, yaml, status string
		exit, findings            int
		wantError                 bool
	}{
		{name: "policy violations are a completed evaluation", stage: "evaluate", exit: 2,
			yaml:   "evaluator:\n  environment: {ort_version: 92.4.0}\n  violations:\n  - {rule: MISSING_SECURITY_FILE, severity: ERROR, message: missing}\n",
			status: "completed", findings: 1},
		{name: "analyzer issues stay incomplete despite exit zero", stage: "analyze", exit: 0,
			yaml: "analyzer:\n  environment: {ort_version: 92.4.0}\n  result:\n    packages: []\n    issues:\n      'GoMod::example:':\n      - {severity: ERROR, message: download failed}\n", status: "incomplete"},
		{name: "dependency graph warning counts", stage: "analyze", exit: 0,
			yaml: "analyzer:\n  environment: {ort_version: 92.4.0}\n  result:\n    packages: []\n    dependency_graphs:\n      GoMod:\n        nodes: [{pkg: 0, issues: [{severity: WARNING}]}]\n", status: "incomplete"},
		{name: "nested scope root issue counts", stage: "analyze", exit: 0,
			yaml: "analyzer:\n  environment: {ort_version: 92.4.0}\n  result:\n    packages: []\n    dependency_graphs:\n      GoMod:\n        scope_roots: [{pkg: 0, dependencies: [{pkg: 1, issues: [{severity: WARNING}]}]}]\n", status: "incomplete"},
		{name: "legacy nested project dependency issue counts", stage: "analyze", exit: 0,
			yaml: "analyzer:\n  environment: {ort_version: 92.4.0}\n  result:\n    packages: []\n    projects:\n    - scopes:\n      - dependencies:\n        - dependencies: [{issues: [{severity: WARNING}]}]\n", status: "incomplete"},
		{name: "advisor provider failure is not no vulnerabilities", stage: "advise", exit: 0,
			yaml: "advisor:\n  environment: {ort_version: 92.4.0}\n  provider_issues: [{severity: ERROR, message: unavailable}]\n  results: {}\n", status: "incomplete"},
		{name: "per package advisor issues count", stage: "advise", exit: 0,
			yaml: "advisor:\n  environment: {ort_version: 92.4.0}\n  results:\n    'GoMod::example:':\n    - summary:\n        issues: [{severity: WARNING, message: unavailable}]\n", status: "incomplete"},
		{name: "no dependencies can be a completed analysis", stage: "analyze", exit: 0,
			yaml: "analyzer:\n  environment: {ort_version: 92.4.0}\n  result: {packages: [], issues: {}}\n", status: "completed"},
		{name: "exit two without findings is unexpected", stage: "evaluate", exit: 2,
			yaml: "evaluator:\n  environment: {ort_version: 92.4.0}\n  violations: []\n", wantError: true},
		{name: "crash with an artifact still fails", stage: "evaluate", exit: 1,
			yaml: "evaluator:\n  environment: {ort_version: 92.4.0}\n  violations: []\n", wantError: true},
		{name: "missing stage is invalid", stage: "evaluate", yaml: "evaluator: null\n", wantError: true},
		{name: "empty document is invalid", stage: "analyze", yaml: "", wantError: true},
		{name: "malformed YAML is invalid", stage: "analyze", yaml: "analyzer: [\n", wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "result.yml")
			if err := os.WriteFile(file, []byte(tt.yaml), 0600); err != nil {
				t.Fatal(err)
			}
			got, err := readResult(file, tt.stage, tt.exit)
			if (err != nil) != tt.wantError {
				t.Fatalf("error = %v, wantError %v", err, tt.wantError)
			}
			if tt.wantError {
				return
			}
			if got.Status != tt.status || len(got.Findings) != tt.findings {
				t.Fatalf("got status=%q findings=%d, want %q / %d", got.Status, len(got.Findings), tt.status, tt.findings)
			}
		})
	}
}

func TestMissingArtifactFails(t *testing.T) {
	if _, err := readResult(filepath.Join(t.TempDir(), "missing.yml"), "analyze", 0); err == nil {
		t.Fatal("missing result file was accepted")
	}
}
