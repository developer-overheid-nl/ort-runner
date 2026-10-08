package main

import (
	"bytes"
	"context"
	"testing"
)

func TestWorkerCommandHelpAndRequiredEnvironment(t *testing.T) {
	clearWorkerEnv(t)
	var output bytes.Buffer
	if code := execute(context.Background(), []string{"worker", "--help"}, &output, &output); code != 2 {
		t.Fatalf("help exit=%d output=%s", code, output.String())
	}
	for _, tc := range []struct {
		name, variable string
	}{
		{name: "batch", variable: "ORT_BATCH_ID"},
		{name: "manifest", variable: "ORT_MANIFEST_DIR"},
		{name: "index", variable: "JOB_COMPLETION_INDEX"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearWorkerEnv(t)
			t.Setenv("ORT_BATCH_ID", "batch-1")
			t.Setenv("ORT_MANIFEST_DIR", "/manifest")
			t.Setenv("JOB_COMPLETION_INDEX", "0")
			t.Setenv(tc.variable, "")
			var output bytes.Buffer
			if code := execute(context.Background(), []string{"worker"}, &output, &output); code != 2 {
				t.Fatalf("missing %s exit=%d output=%s", tc.name, code, output.String())
			}
		})
	}
	clearWorkerEnv(t)
	t.Setenv("ORT_BATCH_ID", "batch-1")
	t.Setenv("ORT_MANIFEST_DIR", "/manifest")
	t.Setenv("JOB_COMPLETION_INDEX", "invalid")
	if code := execute(context.Background(), []string{"worker"}, &output, &output); code != 2 {
		t.Fatalf("malformed index accepted: exit=%d", code)
	}
}

func TestDeliverCommandRequiresBatchAndSubmission(t *testing.T) {
	clearWorkerEnv(t)
	var output bytes.Buffer
	if code := execute(context.Background(), []string{"deliver", "--help"}, &output, &output); code != 2 {
		t.Fatalf("help exit=%d", code)
	}
	if code := execute(context.Background(), []string{"deliver"}, &output, &output); code != 2 {
		t.Fatalf("missing batch accepted: exit=%d", code)
	}
	t.Setenv("ORT_BATCH_ID", "batch-1")
	t.Setenv("ORT_OUTPUT_DIR", t.TempDir())
	if code := execute(context.Background(), []string{"deliver"}, &output, &output); code != 1 {
		t.Fatalf("missing submission accepted: exit=%d", code)
	}
}

func clearWorkerEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{"ORT_BATCH_ID", "ORT_MANIFEST_DIR", "JOB_COMPLETION_INDEX", "ORT_RESULTS_URL", "ORT_CONFIG_DIR", "ORT_OUTPUT_DIR", "ORT_REPOSITORY_TIMEOUT"} {
		t.Setenv(name, "")
	}
	clearAuthEnv(t)
}
