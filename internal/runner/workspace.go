package runner

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
func snapshotConfig(source, target string) (string, error) {
	rules := filepath.Join(source, "evaluator.rules.kts")
	info, err := os.Stat(rules)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("unexpected file type for evaluator.rules.kts")
	}
	data, err := os.ReadFile(rules)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(target, 0700); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(target, "evaluator.rules.kts"), data, 0600); err != nil {
		return "", err
	}

	digest := sha256.New()
	name := "evaluator.rules.kts"
	fmt.Fprintf(digest, "%d:%s%d:", len(name), name, len(data))
	digest.Write(data)
	return fmt.Sprintf("sha256:%x", digest.Sum(nil)), nil
}
