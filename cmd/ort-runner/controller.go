package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

// Kubernetes 1.33 is the first release with stable backoffLimitPerIndex.
const minimumKubernetesMinor = 33

var controllerDependencies = newControllerDependencies

func executeController(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if !noArguments("controller", args, stderr) {
		return 2
	}
	cfg, err := loadControllerConfig()
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	httpClient, err := newHTTPClient(ctx, cfg.HTTP)
	if err != nil {
		fmt.Fprintf(stderr, "Configure client credentials: %v\n", err)
		return 2
	}
	run := cfg.Orchestration
	run.RegisterClient = register.Client{HTTP: httpClient, APIKey: cfg.HTTP.APIKey}
	if run.ResultsURL != "" {
		run.FailureReporter = failedIndexReporter{endpoint: run.ResultsURL, client: register.Client{HTTP: httpClient}}
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
	summary, err := orchestration.RunController(ctx, client, run)
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

// noArguments rejects arguments: the Kubernetes commands are configured
// through environment variables only (see README).
func noArguments(command string, args []string, stderr io.Writer) bool {
	if len(args) == 0 {
		return true
	}
	fmt.Fprintf(stderr, "ort-runner %s takes no arguments; it is configured through environment variables (see README)\n", command)
	return false
}
