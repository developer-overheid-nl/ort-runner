package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCLIRejectsMissingAndUnknownInputs(t *testing.T) {
	clearAuthEnv(t)
	t.Setenv("ORT_REPOSITORIES_URL", "")
	t.Setenv("ORT_RESULTS_URL", "")
	for _, args := range [][]string{nil, {"--unknown"}} {
		var output bytes.Buffer
		if code := execute(context.Background(), args, &output, &output); code != 2 {
			t.Fatalf("args %v: exit %d, want usage error", args, code)
		}
		if output.Len() == 0 {
			t.Fatal("usage error without explanation")
		}
	}
}

func TestCLIAcceptsRepositoryWithoutRevision(t *testing.T) {
	clearAuthEnv(t)
	t.Setenv("ORT_REPOSITORIES_URL", "")
	t.Setenv("ORT_RESULTS_URL", "")
	configDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(configDir, "evaluator.rules.kts"), []byte("// test\n"), 0600); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	code := execute(context.Background(), []string{
		"--repository", filepath.Join(t.TempDir(), "missing-repository"),
		"--config-dir", configDir,
		"--output-dir", t.TempDir(),
		"--stage-timeout", time.Second.String(),
	}, &output, &output)
	if code != 1 || !strings.Contains(output.String(), "checkout failed") {
		t.Fatalf("repository without revision was rejected as invalid CLI input: code=%d, output=%s", code, output.String())
	}
}

func TestCLIRegisterModeUsesEnvironmentAndWritesBatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"access_token":"read-token","token_type":"Bearer","expires_in":3600}`)
			return
		}
		if r.URL.Path != "/repositories" || r.Header.Get("X-Api-Key") != "read-key" || r.Header.Get("Authorization") != "" {
			t.Errorf("incorrect register request")
		}
		w.Header().Set("Total-Pages", "0")
		fmt.Fprint(w, `[]`)
	}))
	defer server.Close()
	t.Setenv("ORT_REPOSITORIES_URL", server.URL+"/repositories")
	t.Setenv("ORT_RESULTS_URL", "")
	t.Setenv("ORT_REGISTER_API_KEY", "read-key")
	t.Setenv("AUTH_TOKEN_URL", server.URL+"/token")
	t.Setenv("AUTH_CLIENT_ID", "runner")
	t.Setenv("AUTH_CLIENT_SECRET", "secret")
	t.Setenv("AUTH_SCOPES", "repositories:write")
	var output bytes.Buffer
	code := execute(context.Background(), []string{"--config-dir", t.TempDir(), "--output-dir", t.TempDir()}, &output, &output)
	if code != 0 || !strings.Contains(output.String(), "batch.json") {
		t.Fatalf("register mode failed: code=%d, output=%s", code, output.String())
	}
}

func TestCLIRejectsMixedSingleAndRegisterOptions(t *testing.T) {
	clearAuthEnv(t)
	t.Setenv("ORT_REPOSITORIES_URL", "")
	t.Setenv("ORT_RESULTS_URL", "")
	for _, args := range [][]string{
		{"--repositories-url", "https://example.invalid/repositories", "--repository", "https://example.invalid/repo"},
		{"--repositories-url", "https://example.invalid/repositories", "--revision", "0123456789012345678901234567890123456789"},
		{"--repository", "https://example.invalid/repo", "--revision", "0123456789012345678901234567890123456789", "--results-url", "https://example.invalid/results"},
	} {
		var output bytes.Buffer
		if code := execute(context.Background(), args, &output, &output); code != 2 {
			t.Fatalf("mixed modes accepted: %v, exit=%d", args, code)
		}
	}
}

func clearAuthEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{"AUTH_TOKEN_URL", "AUTH_CLIENT_ID", "AUTH_CLIENT_SECRET", "AUTH_SCOPES"} {
		t.Setenv(name, "")
	}
}

func TestHelpDoesNotStartScan(t *testing.T) {
	var output bytes.Buffer
	if code := execute(context.Background(), []string{"--help"}, &output, &output); code != 0 {
		t.Fatal(code)
	}
	if !strings.Contains(output.String(), "-revision") {
		t.Fatal("help does not explain revision input")
	}
}
