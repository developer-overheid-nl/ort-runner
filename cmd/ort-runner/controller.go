package main

import (
	"context"
	"encoding/json"
	"flag"
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

var controllerDependencies = newControllerDependencies

func init() {
	controllerCommand = executeController
}

func executeController(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	parallelism, err := envInt32("ORT_PARALLELISM", 10)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	retryLimit, err := envInt32("ORT_REPOSITORY_RETRY_LIMIT", 1)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	httpTimeout, err := envDuration("ORT_HTTP_TIMEOUT", 30*time.Second)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}

	flags := flag.NewFlagSet("ort-runner controller", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cfg := orchestration.ControllerConfig{
		Namespace: os.Getenv("POD_NAMESPACE"), PodName: os.Getenv("POD_NAME"), ContainerName: envDefault("ORT_CONTROLLER_CONTAINER", "ort-runner"),
		BatchName: os.Getenv("ORT_BATCH_ID"), RepositoriesURL: os.Getenv("ORT_REPOSITORIES_URL"), ResultsURL: os.Getenv("ORT_RESULTS_URL"),
		ConfigDir: os.Getenv("ORT_CONFIG_DIR"), Parallelism: parallelism, RetryLimit: retryLimit,
	}
	parallelismFlag := int(cfg.Parallelism)
	retryLimitFlag := int(cfg.RetryLimit)
	flags.StringVar(&cfg.Namespace, "namespace", cfg.Namespace, "Kubernetes namespace")
	flags.StringVar(&cfg.PodName, "pod-name", cfg.PodName, "Controller Pod name")
	flags.StringVar(&cfg.ContainerName, "container-name", cfg.ContainerName, "Controller container name")
	flags.StringVar(&cfg.BatchName, "batch-id", cfg.BatchName, "Stable batch identifier")
	flags.StringVar(&cfg.RepositoriesURL, "repositories-url", cfg.RepositoriesURL, "OSS-register GET endpoint")
	flags.StringVar(&cfg.ResultsURL, "results-url", cfg.ResultsURL, "Result POST endpoint")
	flags.StringVar(&cfg.ConfigDir, "config-dir", cfg.ConfigDir, "Directory containing the ORT configuration")
	flags.IntVar(&parallelismFlag, "parallelism", parallelismFlag, "Concurrent repository workers (1-100)")
	flags.IntVar(&retryLimitFlag, "retry-limit", retryLimitFlag, "Retries per repository; must equal 1")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if parallelismFlag < 1 || parallelismFlag > 100 || retryLimitFlag != 1 {
		fmt.Fprintln(stderr, "Controller requires parallelism 1-100 and retry limit 1.")
		return 2
	}
	cfg.Parallelism = int32(parallelismFlag)
	cfg.RetryLimit = int32(retryLimitFlag)
	if flags.NArg() != 0 || cfg.Namespace == "" || cfg.PodName == "" || cfg.ContainerName == "" || cfg.BatchName == "" || cfg.RepositoriesURL == "" || cfg.ConfigDir == "" || cfg.RetryLimit != 1 || cfg.Parallelism < 1 || cfg.Parallelism > 100 {
		fmt.Fprintln(stderr, "Controller requires namespace, Pod name, batch ID, repositories URL, config directory, parallelism 1-100 and retry limit 1.")
		return 2
	}

	client, serverVersion, err := controllerDependencies()
	if err != nil {
		fmt.Fprintf(stderr, "Configure Kubernetes client: %v\n", err)
		return 1
	}
	if err := requireKubernetes133(serverVersion); err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	base := &http.Client{Timeout: httpTimeout}
	cfg.RegisterClient = register.Client{HTTP: base, APIKey: os.Getenv("ORT_REGISTER_API_KEY")}
	resultsHTTP, err := newResultsHTTPClient(ctx, httpTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "Configure result credentials: %v\n", err)
		return 2
	}
	cfg.ResultsClient = register.Client{HTTP: resultsHTTP}
	cfg.WorkerResources = orchestration.WorkerResources{
		CPURequest: envDefault("ORT_WORKER_CPU_REQUEST", "200m"), CPULimit: envDefault("ORT_WORKER_CPU_LIMIT", "2000m"),
		MemoryRequest: envDefault("ORT_WORKER_MEMORY_REQUEST", "4Gi"), MemoryLimit: envDefault("ORT_WORKER_MEMORY_LIMIT", "8Gi"),
	}
	cfg.FailureReporter = failedIndexReporter{endpoint: cfg.ResultsURL, client: cfg.ResultsClient}
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

func requireKubernetes133(info *k8sversion.Info) error {
	if info == nil {
		return fmt.Errorf("Kubernetes server version is unavailable; version 1.33 or newer is required")
	}
	major, majorErr := strconv.Atoi(strings.TrimRightFunc(info.Major, func(r rune) bool { return r < '0' || r > '9' }))
	minorText := strings.TrimRightFunc(info.Minor, func(r rune) bool { return r < '0' || r > '9' })
	minor, minorErr := strconv.Atoi(minorText)
	if majorErr != nil || minorErr != nil || major != 1 || minor < 33 {
		return fmt.Errorf("Kubernetes server %s is unsupported; version 1.33 or newer is required", info.GitVersion)
	}
	return nil
}

type failedIndexReporter struct {
	endpoint string
	client   register.Client
}

func (reporter failedIndexReporter) ReportFailedIndex(ctx context.Context, batchID string, repository register.Repository) error {
	now := time.Now().UTC()
	submission := runner.Submission{SchemaVersion: 1, RepositoryID: repository.ID, Scan: runner.Report{
		SchemaVersion: 1, Status: "failed", Error: "worker failed after Kubernetes retry", Repository: repository.URL,
		StartedAt: now, FinishedAt: &now, Findings: []runner.Finding{}, Vulnerabilities: []runner.Vulnerability{}, Stages: []runner.Stage{},
	}}
	data, err := json.Marshal(submission)
	if err != nil {
		return err
	}
	return reporter.client.PostResult(ctx, reporter.endpoint, data, worker.ResultIdempotencyKey(batchID, repository.ID))
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envInt32(name string, fallback int32) (int32, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	return int32(parsed), nil
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
