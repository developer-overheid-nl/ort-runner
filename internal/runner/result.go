package runner

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type resultInfo struct {
	Status          string
	Version         string
	Packages        int
	Dependencies    Dependencies
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
	Pkg          int          `yaml:"pkg"`
	Fragment     int          `yaml:"fragment"`
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

type dependencyGraph struct {
	Packages []string `yaml:"packages"`
	Scopes   map[string][]struct {
		Root     int `yaml:"root"`
		Fragment int `yaml:"fragment"`
	} `yaml:"scopes"`
	Nodes      []dependency `yaml:"nodes"`
	ScopeRoots []dependency `yaml:"scope_roots"`
	Edges      []struct {
		From int `yaml:"from"`
		To   int `yaml:"to"`
	} `yaml:"edges"`
}

type ortResult struct {
	Analyzer *struct {
		Environment environment `yaml:"environment"`
		Result      *struct {
			Packages         []yaml.Node                `yaml:"packages"`
			Issues           map[string][]issue         `yaml:"issues"`
			DependencyGraphs map[string]dependencyGraph `yaml:"dependency_graphs"`
			Projects         []struct {
				ID             string `yaml:"id"`
				DefinitionFile string `yaml:"definition_file_path"`
				Scopes         []struct {
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
		info.Dependencies = Dependencies{Projects: []DependencyProject{}}
		direct, all := map[string]bool{}, map[string]bool{}
		for _, project := range result.Analyzer.Result.Projects {
			manager, key, _ := strings.Cut(project.ID, ":")
			// ORT adds an Unmanaged project for files outside any package manager.
			if manager == "Unmanaged" {
				continue
			}
			projectDirect, projectAll := map[string]bool{}, map[string]bool{}
			graph := result.Analyzer.Result.DependencyGraphs[manager]
			graph.collect(key, projectDirect, projectAll)
			for id := range projectDirect {
				direct[id] = true
			}
			for id := range projectAll {
				all[id] = true
			}
			info.Dependencies.Projects = append(info.Dependencies.Projects, DependencyProject{
				PackageManager: manager, DefinitionFile: project.DefinitionFile,
				DirectCount: len(projectDirect), TransitiveCount: len(projectAll) - len(projectDirect),
			})
		}
		info.Dependencies.DirectCount, info.Dependencies.TransitiveCount = len(direct), len(all)-len(direct)
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

// collect adds the package IDs of the project's scope roots to direct and of every
// package reachable from them to all. Scope keys are the project ID without its type,
// followed by ":" and the scope name.
func (graph dependencyGraph) collect(project string, direct, all map[string]bool) {
	nodes := map[[2]int]int{}
	for index, node := range graph.Nodes {
		nodes[[2]int{node.Pkg, node.Fragment}] = index
	}
	edges := map[int][]int{}
	for _, edge := range graph.Edges {
		edges[edge.From] = append(edges[edge.From], edge.To)
	}
	visited := map[int]bool{}
	var visit func(int)
	visit = func(index int) {
		if visited[index] || index < 0 || index >= len(graph.Nodes) {
			return
		}
		visited[index] = true
		if pkg := graph.Nodes[index].Pkg; pkg >= 0 && pkg < len(graph.Packages) {
			all[graph.Packages[pkg]] = true
		}
		for _, next := range edges[index] {
			visit(next)
		}
	}
	for scope, roots := range graph.Scopes {
		if index := strings.LastIndex(scope, ":"); index < 0 || scope[:index] != project {
			continue
		}
		for _, root := range roots {
			if root.Root >= 0 && root.Root < len(graph.Packages) {
				direct[graph.Packages[root.Root]], all[graph.Packages[root.Root]] = true, true
			}
			if index, ok := nodes[[2]int{root.Root, root.Fragment}]; ok {
				visit(index)
			}
		}
	}
}
