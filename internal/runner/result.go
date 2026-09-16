package runner

import (
	"fmt"
	"os"
	"sort"

	"gopkg.in/yaml.v3"
)

type resultInfo struct {
	Status          string
	Version         string
	Packages        int
	Findings        []Finding
	Vulnerabilities []Vulnerability
}

type environment struct {
	Version string `yaml:"ort_version"`
}
type issue struct {
	Severity string `yaml:"severity"`
}

type dependency struct {
	Issues       []issue      `yaml:"issues"`
	Dependencies []dependency `yaml:"dependencies"`
}

type vulnerabilityReference struct {
	ScoringSystem string   `yaml:"scoring_system"`
	Severity      string   `yaml:"severity"`
	Score         *float64 `yaml:"score"`
}

type advisorVulnerability struct {
	ID                 string                   `yaml:"id"`
	Summary            string                   `yaml:"summary"`
	References         []vulnerabilityReference `yaml:"references"`
	FirstFixedVersions []string                 `yaml:"first_fixed_versions"`
}

type ortResult struct {
	Analyzer *struct {
		Environment environment `yaml:"environment"`
		Result      *struct {
			Packages         []yaml.Node        `yaml:"packages"`
			Issues           map[string][]issue `yaml:"issues"`
			DependencyGraphs map[string]struct {
				Nodes      []dependency `yaml:"nodes"`
				ScopeRoots []dependency `yaml:"scope_roots"`
			} `yaml:"dependency_graphs"`
			Projects []struct {
				Scopes []struct {
					Dependencies []dependency `yaml:"dependencies"`
				} `yaml:"scopes"`
			} `yaml:"projects"`
		} `yaml:"result"`
	} `yaml:"analyzer"`
	Advisor *struct {
		Environment    environment `yaml:"environment"`
		ProviderIssues []issue     `yaml:"provider_issues"`
		Results        map[string][]struct {
			Summary struct {
				Issues []issue `yaml:"issues"`
			} `yaml:"summary"`
			Vulnerabilities []advisorVulnerability `yaml:"vulnerabilities"`
		} `yaml:"results"`
	} `yaml:"advisor"`
	Evaluator *struct {
		Environment environment `yaml:"environment"`
		Violations  []Finding   `yaml:"violations"`
	} `yaml:"evaluator"`
}

// Completion means the command produced its expected data, not that the repository complies.
func readResult(path, stage string, exitCode int) (resultInfo, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return resultInfo{}, fmt.Errorf("read %s result: %w", stage, err)
	}
	var result ortResult
	if err := yaml.Unmarshal(data, &result); err != nil {
		return resultInfo{}, fmt.Errorf("decode %s result: %w", stage, err)
	}
	info := resultInfo{Status: "completed"}
	var issues []issue
	switch stage {
	case "analyze":
		if result.Analyzer == nil || result.Analyzer.Result == nil {
			return info, fmt.Errorf("analyzer section missing from result")
		}
		info.Version = result.Analyzer.Environment.Version
		info.Packages = len(result.Analyzer.Result.Packages)
		for _, entries := range result.Analyzer.Result.Issues {
			issues = append(issues, entries...)
		}
		for _, graph := range result.Analyzer.Result.DependencyGraphs {
			issues = appendDependencyIssues(issues, graph.Nodes)
			issues = appendDependencyIssues(issues, graph.ScopeRoots)
		}
		for _, project := range result.Analyzer.Result.Projects {
			for _, scope := range project.Scopes {
				issues = appendDependencyIssues(issues, scope.Dependencies)
			}
		}
	case "advise":
		if result.Advisor == nil {
			return info, fmt.Errorf("advisor section missing from result")
		}
		info.Version = result.Advisor.Environment.Version
		issues = append(issues, result.Advisor.ProviderIssues...)
		for packageID, entries := range result.Advisor.Results {
			for _, entry := range entries {
				issues = append(issues, entry.Summary.Issues...)
				for _, vulnerability := range entry.Vulnerabilities {
					item := Vulnerability{
						PackageID: packageID, ID: vulnerability.ID, Summary: vulnerability.Summary,
						FirstFixedVersions: vulnerability.FirstFixedVersions,
					}
					for _, reference := range vulnerability.References {
						if reference.Score != nil && (item.Score == nil || *reference.Score > *item.Score) {
							item.Score = reference.Score
							item.Severity = reference.Severity
							item.ScoringSystem = reference.ScoringSystem
						}
					}
					info.Vulnerabilities = append(info.Vulnerabilities, item)
				}
			}
		}
		sort.Slice(info.Vulnerabilities, func(i, j int) bool {
			left, right := info.Vulnerabilities[i], info.Vulnerabilities[j]
			if left.PackageID != right.PackageID {
				return left.PackageID < right.PackageID
			}
			return left.ID < right.ID
		})
	case "evaluate":
		if result.Evaluator == nil {
			return info, fmt.Errorf("evaluator section missing from result")
		}
		info.Version = result.Evaluator.Environment.Version
		info.Findings = result.Evaluator.Violations
	default:
		return info, fmt.Errorf("unknown ORT stage %q", stage)
	}
	if info.Version == "" {
		return info, fmt.Errorf("ORT version missing from %s result", stage)
	}
	if exitCode != 0 && exitCode != 2 {
		return info, fmt.Errorf("%s exited with code %d", stage, exitCode)
	}
	if stage == "evaluate" {
		if exitCode == 2 && len(info.Findings) == 0 {
			return info, fmt.Errorf("evaluation exited with code 2 without rule violations")
		}
		return info, nil
	}
	if exitCode == 2 {
		info.Status = "incomplete"
	}
	for _, issue := range issues {
		if issue.Severity == "ERROR" || issue.Severity == "WARNING" {
			info.Status = "incomplete"
		}
	}
	return info, nil
}

func appendDependencyIssues(issues []issue, dependencies []dependency) []issue {
	for _, dependency := range dependencies {
		issues = append(issues, dependency.Issues...)
		issues = appendDependencyIssues(issues, dependency.Dependencies)
	}
	return issues
}
