package runner

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

func checkout(ctx context.Context, cfg Config, project, log string) (string, int, error) {
	ref := cfg.Revision
	if ref == "" {
		ref = "HEAD"
	}
	commands := [][]string{
		{"init", "--quiet"},
		{"remote", "add", "origin", cfg.Repository},
		{"fetch", "--depth=1", "origin", ref},
		{"checkout", "--quiet", "--detach", "FETCH_HEAD"},
	}
	for _, args := range commands {
		code, err := runCommand(ctx, "git", args, project, []string{"GIT_TERMINAL_PROMPT=0"}, log)
		if err != nil {
			return "", code, err
		}
	}
	head, err := os.ReadFile(filepath.Join(project, ".git", "HEAD"))
	if err != nil {
		return "", -1, err
	}
	revision := strings.TrimSpace(string(head))
	if cfg.Revision != "" && revision != cfg.Revision {
		return revision, -1, fmt.Errorf("checkout does not match requested commit")
	}
	code, err := runCommand(ctx, "git", []string{"submodule", "update", "--init", "--recursive", "--depth=1"},
		project, []string{"GIT_TERMINAL_PROMPT=0"}, log)
	return revision, code, err
}

// Snapshot configuration so a local edit cannot change the files halfway through the ORT pipeline.
// The rules manifest and ORT configuration are optional; without a manifest the
// report only lists violations, without config.yml ORT uses its defaults.
func snapshotConfig(source, target string) (string, []ruleDefinition, error) {
	if err := os.MkdirAll(target, 0700); err != nil {
		return "", nil, err
	}
	digest := sha256.New()
	var rules []ruleDefinition
	for _, name := range []string{"evaluator.rules.kts", rulesManifest, ortConfig} {
		info, err := os.Stat(filepath.Join(source, name))
		if name != "evaluator.rules.kts" && errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return "", nil, err
		}
		if !info.Mode().IsRegular() {
			return "", nil, fmt.Errorf("unexpected file type for %s", name)
		}
		data, err := os.ReadFile(filepath.Join(source, name))
		if err != nil {
			return "", nil, err
		}
		if name == rulesManifest {
			if rules, err = parseRules(data); err != nil {
				return "", nil, fmt.Errorf("%s: %w", name, err)
			}
		}
		if err := os.WriteFile(filepath.Join(target, name), data, 0600); err != nil {
			return "", nil, err
		}
		fmt.Fprintf(digest, "%d:%s%d:", len(name), name, len(data))
		digest.Write(data)
	}
	return fmt.Sprintf("sha256:%x", digest.Sum(nil)), rules, nil
}

const (
	rulesManifest = "rules.yml"
	ortConfig     = "config.yml"
)

type ruleDefinition struct {
	ID       string `yaml:"id"`
	Severity string `yaml:"severity"`
	Scope    string `yaml:"scope"`
}

func parseRules(data []byte) ([]ruleDefinition, error) {
	var manifest struct {
		Rules []ruleDefinition `yaml:"rules"`
	}
	if err := yaml.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, rule := range manifest.Rules {
		if rule.ID == "" || rule.Severity == "" || (rule.Scope != "repository" && rule.Scope != "dependency") {
			return nil, fmt.Errorf("rule %q needs an id, a severity and scope repository or dependency", rule.ID)
		}
		if seen[rule.ID] {
			return nil, fmt.Errorf("rule %q is listed twice", rule.ID)
		}
		seen[rule.ID] = true
	}
	return manifest.Rules, nil
}

func ruleResults(rules []ruleDefinition, findings []Finding) []RuleResult {
	if rules == nil {
		return nil
	}
	violated := map[string]bool{}
	for _, finding := range findings {
		violated[finding.Rule] = true
	}
	results := make([]RuleResult, 0, len(rules))
	for _, rule := range rules {
		results = append(results, RuleResult{Rule: rule.ID, Severity: rule.Severity, Scope: rule.Scope, Passed: !violated[rule.ID]})
	}
	return results
}
