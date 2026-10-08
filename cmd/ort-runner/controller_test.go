package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	k8sversion "k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

func TestControllerCommandHelpAndRequiredEnvironment(t *testing.T) {
	clearControllerEnv(t)
	var output bytes.Buffer
	if code := execute(context.Background(), []string{"controller", "--help"}, &output, &output); code != 2 {
		t.Fatalf("help exit=%d output=%s", code, output.String())
	}
	for _, tc := range []struct {
		name, variable, value string
	}{
		{name: "namespace", variable: "POD_NAMESPACE", value: "oss"},
		{name: "pod", variable: "POD_NAME", value: "controller"},
		{name: "repository URL", variable: "ORT_REPOSITORIES_URL", value: "https://example.test/repositories"},
		{name: "config", variable: "ORT_CONFIG_DIR", value: "/config"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearControllerEnv(t)
			for _, pair := range [][2]string{{"POD_NAMESPACE", "oss"}, {"POD_NAME", "controller"}, {"ORT_BATCH_ID", "batch-1"}, {"ORT_REPOSITORIES_URL", "https://example.test/repositories"}, {"ORT_CONFIG_DIR", "/config"}} {
				t.Setenv(pair[0], pair[1])
			}
			t.Setenv(tc.variable, "")
			var output bytes.Buffer
			if code := execute(context.Background(), []string{"controller"}, &output, &output); code != 2 {
				t.Fatalf("missing %s exit=%d output=%s", tc.name, code, output.String())
			}
		})
	}
}

func TestControllerCommandRejectsKubernetesBefore133(t *testing.T) {
	clearControllerEnv(t)
	for _, pair := range [][2]string{{"POD_NAMESPACE", "oss"}, {"POD_NAME", "controller"}, {"ORT_BATCH_ID", "batch-1"}, {"ORT_REPOSITORIES_URL", "http://127.0.0.1:1/repositories"}, {"ORT_CONFIG_DIR", "/config"}} {
		t.Setenv(pair[0], pair[1])
	}
	original := controllerDependencies
	controllerDependencies = func() (kubernetes.Interface, *k8sversion.Info, error) {
		return fake.NewSimpleClientset(), &k8sversion.Info{Major: "1", Minor: "32", GitVersion: "v1.32.9"}, nil
	}
	defer func() { controllerDependencies = original }()
	var output bytes.Buffer
	if code := execute(context.Background(), []string{"controller"}, &output, &output); code != 2 || !bytes.Contains(output.Bytes(), []byte("1.33")) {
		t.Fatalf("exit=%d output=%s", code, output.String())
	}
}

func TestControllerVersionValidation(t *testing.T) {
	for _, tc := range []struct {
		version *k8sversion.Info
		valid   bool
	}{
		{version: &k8sversion.Info{Major: "1", Minor: "33+", GitVersion: "v1.33.0"}, valid: true},
		{version: &k8sversion.Info{Major: "1", Minor: "35", GitVersion: "v1.35.1"}, valid: true},
		{version: &k8sversion.Info{Major: "1", Minor: "32", GitVersion: "v1.32.9"}, valid: false},
		{version: &k8sversion.Info{Major: "2", Minor: "0", GitVersion: "v2.0.0"}, valid: false},
	} {
		if err := requireSupportedKubernetes(tc.version); (err == nil) != tc.valid {
			t.Fatalf("version=%+v valid=%t err=%v", tc.version, tc.valid, err)
		}
	}
}

func TestControllerCommandRejectsInvalidSettingsAndArguments(t *testing.T) {
	for _, tc := range []struct {
		name, variable, value string
		args                  []string
	}{
		{name: "parallelism", variable: "ORT_PARALLELISM", value: "101"},
		{name: "deadline", variable: "ORT_BATCH_DEADLINE", value: "-1h"},
		{name: "storage", variable: "ORT_WORKER_EPHEMERAL_STORAGE_LIMIT", value: "lots"},
		{name: "arguments", args: []string{"--parallelism", "5"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearControllerEnv(t)
			for _, pair := range [][2]string{{"POD_NAMESPACE", "oss"}, {"POD_NAME", "controller"}, {"ORT_BATCH_ID", "batch-1"}, {"ORT_REPOSITORIES_URL", "https://example.test/repositories"}, {"ORT_CONFIG_DIR", "/config"}} {
				t.Setenv(pair[0], pair[1])
			}
			if tc.variable != "" {
				t.Setenv(tc.variable, tc.value)
			}
			var output bytes.Buffer
			if code := execute(context.Background(), append([]string{"controller"}, tc.args...), &output, &output); code != 2 {
				t.Fatalf("exit=%d output=%s", code, output.String())
			}
		})
	}
}

func clearControllerEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{"POD_NAMESPACE", "POD_NAME", "ORT_BATCH_ID", "ORT_REPOSITORIES_URL", "ORT_RESULTS_URL", "ORT_CONFIG_DIR", "ORT_PARALLELISM", "ORT_BATCH_DEADLINE", "ORT_WORKER_CPU_REQUEST", "ORT_WORKER_CPU_LIMIT", "ORT_WORKER_MEMORY_REQUEST", "ORT_WORKER_MEMORY_LIMIT", "ORT_WORKER_EPHEMERAL_STORAGE_REQUEST", "ORT_WORKER_EPHEMERAL_STORAGE_LIMIT", "ORT_REGISTER_API_KEY"} {
		t.Setenv(name, "")
	}
	clearAuthEnv(t)
}

func TestControllerConfigReportsAllProblemsAtOnce(t *testing.T) {
	clearControllerEnv(t)
	t.Setenv("ORT_PARALLELISM", "ten")
	_, err := loadControllerConfig()
	for _, want := range []string{"POD_NAMESPACE", "POD_NAME", "ORT_BATCH_ID", "ORT_REPOSITORIES_URL", "ORT_CONFIG_DIR", "ORT_PARALLELISM"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("error does not mention %s: %v", want, err)
		}
	}
}
