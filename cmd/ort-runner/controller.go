package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/developer-overheid-nl/ort-runner/internal/orchestration"
	"github.com/developer-overheid-nl/ort-runner/internal/register"
	"github.com/developer-overheid-nl/ort-runner/internal/runner"
	"github.com/developer-overheid-nl/ort-runner/internal/worker"
	k8sversion "k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const controllerUsage = `Usage: ort-runner controller

Creates one Kubernetes Indexed Job for all register repositories and waits for it.
Configured through the environment:

  POD_NAMESPACE, POD_NAME          controller Pod (downward API)
  ORT_BATCH_ID                     stable batch identifier (controller Job name)
  ORT_REPOSITORIES_URL             OSS-register GET endpoint
  ORT_REGISTER_API_KEY             API key for the register GET
  ORT_RESULTS_URL                  result POST endpoint; empty disables delivery
  AUTH_TOKEN_URL, AUTH_CLIENT_ID,
  AUTH_CLIENT_SECRET, AUTH_SCOPES  OAuth client credentials for result delivery
  ORT_CONFIG_DIR                   directory with evaluator.rules.kts
  ORT_PARALLELISM                  concurrent workers, 1-100 (default 10)
  ORT_BATCH_DEADLINE               maximum batch duration (default 46h)
  ORT_REPOSITORY_TIMEOUT           time budget per repository scan (default 10m)
  ORT_WORKER_CPU_REQUEST/LIMIT     (default 200m / 2000m)
  ORT_WORKER_MEMORY_REQUEST/LIMIT  (default 4Gi / 8Gi)
  ORT_WORKER_EPHEMERAL_STORAGE_REQUEST/LIMIT  (default 10Gi / 30Gi)
`

// Kubernetes 1.33 is the first release with stable backoffLimitPerIndex.
const minimumKubernetesMinor = 33

var controllerDependencies = newControllerDependencies

func executeController(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if code, done := internalCommandArgs(args, controllerUsage, stdout, stderr); done {
		return code
	}
	parallelism, err := strconv.ParseInt(envDefault("ORT_PARALLELISM", "10"), 10, 32)
	if err != nil {
		fmt.Fprintln(stderr, "ORT_PARALLELISM must be an integer")
		return 2
	}
	deadline, err := envDuration("ORT_BATCH_DEADLINE", 46*time.Hour)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	repositoryTimeout, err := envDuration("ORT_REPOSITORY_TIMEOUT", 10*time.Minute)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	httpTimeout, err := envDuration("ORT_HTTP_TIMEOUT", 30*time.Second)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	cfg := orchestration.ControllerConfig{
		Namespace:       os.Getenv("POD_NAMESPACE"),
		PodName:         os.Getenv("POD_NAME"),
		ContainerName:   envDefault("ORT_CONTROLLER_CONTAINER", "ort-runner"),
		BatchName:       os.Getenv("ORT_BATCH_ID"),
		RepositoriesURL: os.Getenv("ORT_REPOSITORIES_URL"),
		ResultsURL:      os.Getenv(register.ResultsURLVariable),
		ConfigDir:       os.Getenv("ORT_CONFIG_DIR"),
		Settings: orchestration.WorkerSettings{
			Parallelism:             int32(parallelism),
			Deadline:                deadline,
			RepositoryTimeout:       repositoryTimeout,
			CPURequest:              envDefault("ORT_WORKER_CPU_REQUEST", "200m"),
			CPULimit:                envDefault("ORT_WORKER_CPU_LIMIT", "2000m"),
			MemoryRequest:           envDefault("ORT_WORKER_MEMORY_REQUEST", "4Gi"),
			MemoryLimit:             envDefault("ORT_WORKER_MEMORY_LIMIT", "8Gi"),
			EphemeralStorageRequest: envDefault("ORT_WORKER_EPHEMERAL_STORAGE_REQUEST", "10Gi"),
			EphemeralStorageLimit:   envDefault("ORT_WORKER_EPHEMERAL_STORAGE_LIMIT", "30Gi"),
		},
		RegisterClient: register.Client{HTTP: &http.Client{Timeout: httpTimeout}, APIKey: os.Getenv(register.APIKeyVariable)},
	}
	if cfg.Namespace == "" || cfg.PodName == "" || cfg.BatchName == "" || cfg.RepositoriesURL == "" || cfg.ConfigDir == "" {
		fmt.Fprintln(stderr, "POD_NAMESPACE, POD_NAME, ORT_BATCH_ID, ORT_REPOSITORIES_URL and ORT_CONFIG_DIR are required.")
		return 2
	}
	if err := cfg.Settings.Validate(); err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if cfg.ResultsURL != "" {
		resultsHTTP, err := newResultsHTTPClient(ctx, httpTimeout)
		if err != nil {
			fmt.Fprintf(stderr, "Configure result credentials: %v\n", err)
			return 2
		}
		cfg.FailureReporter = failedIndexReporter{endpoint: cfg.ResultsURL, client: register.Client{HTTP: resultsHTTP}}
	}

	client, serverVersion, err := controllerDependencies()
	if err != nil {
		fmt.Fprintf(stderr, "Configure Kubernetes client: %v\n", err)
		return 1
	}
	if err := requireSupportedKubernetes(serverVersion); err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	summary, err := orchestration.RunController(ctx, client, cfg)
	if encodeErr := json.NewEncoder(stdout).Encode(summary); encodeErr != nil && err == nil {
		err = encodeErr
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func newControllerDependencies() (kubernetes.Interface, *k8sversion.Info, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, nil, err
	}
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return nil, nil, err
	}
	serverVersion, err := discoveryClient.ServerVersion()
	if err != nil {
		return nil, nil, err
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, err
	}
	return client, serverVersion, nil
}

func requireSupportedKubernetes(info *k8sversion.Info) error {
	if info == nil {
		return fmt.Errorf("Kubernetes server version is unavailable; version 1.%d or newer is required", minimumKubernetesMinor)
	}
	// Managed clusters report minors such as "33+".
	major, majorErr := strconv.Atoi(info.Major)
	minor, minorErr := strconv.Atoi(strings.TrimSuffix(info.Minor, "+"))
	if majorErr != nil || minorErr != nil || major != 1 || minor < minimumKubernetesMinor {
		return fmt.Errorf("Kubernetes server %s is unsupported; version 1.%d or newer is required", info.GitVersion, minimumKubernetesMinor)
	}
	return nil
}

type failedIndexReporter struct {
	endpoint string
	client   register.Client
}

// ReportFailedIndex records a repository whose worker Pod never delivered a
// result, for example after two OOM kills or an expired batch deadline.
func (reporter failedIndexReporter) ReportFailedIndex(ctx context.Context, batchID string, repository register.Repository) error {
	now := time.Now().UTC()
	submission := runner.Submission{SchemaVersion: 1, RepositoryID: repository.ID, Scan: runner.Report{
		SchemaVersion: 1, Status: "failed", Error: "repository scan did not finish", Repository: repository.URL,
		StartedAt: now, FinishedAt: &now, Findings: []runner.Finding{}, Vulnerabilities: []runner.Vulnerability{}, Stages: []runner.Stage{},
	}}
	data, err := json.Marshal(submission)
	if err != nil {
		return err
	}
	return reporter.client.PostResult(ctx, reporter.endpoint, data, worker.ResultIdempotencyKey(batchID, repository.ID))
}

// internalCommandArgs handles --help; the Kubernetes commands take no arguments.
func internalCommandArgs(args []string, usage string, stdout, stderr io.Writer) (int, bool) {
	switch {
	case len(args) == 0:
		return 0, false
	case len(args) == 1 && (args[0] == "-h" || args[0] == "--help"):
		fmt.Fprint(stdout, usage)
		return 0, true
	default:
		fmt.Fprint(stderr, usage)
		return 2, true
	}
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envDuration(name string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return parsed, nil
}
