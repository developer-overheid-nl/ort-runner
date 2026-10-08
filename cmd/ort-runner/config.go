package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	commonauth "github.com/developer-overheid-nl/don-register-common/auth"
	"github.com/developer-overheid-nl/ort-runner/internal/orchestration"
	"github.com/developer-overheid-nl/ort-runner/internal/register"
	"github.com/developer-overheid-nl/ort-runner/internal/runner"
	"github.com/developer-overheid-nl/ort-runner/internal/worker"
	"github.com/joho/godotenv"
)

// localEnvFile is loaded for local runs. Variables that are already set, such as
// those of a Kubernetes Pod, take precedence.
const localEnvFile = ".env.local"

func loadLocalEnv() error {
	// No file, or a working directory this user may not read (such as the
	// image home directory under another uid): there is nothing to load.
	if _, err := os.Stat(localEnvFile); err != nil {
		return nil
	}
	if err := godotenv.Load(localEnvFile); err != nil {
		return fmt.Errorf("load %s: %w", localEnvFile, err)
	}
	return nil
}

// env reads environment variables and collects every problem, so a
// misconfigured Pod reports all of them at once.
type env struct {
	errs []error
}

func (e *env) string(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func (e *env) required(name string) string {
	value := os.Getenv(name)
	if value == "" {
		e.errs = append(e.errs, fmt.Errorf("%s is required", name))
	}
	return value
}

func (e *env) duration(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		e.errs = append(e.errs, fmt.Errorf("%s must be a positive duration", name))
	}
	return parsed
}

func (e *env) int(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s must be an integer", name))
	}
	return parsed
}

func (e *env) err() error { return errors.Join(e.errs...) }

func (e *env) auth() commonauth.ClientCredentialsConfig {
	return commonauth.ClientCredentialsConfig{
		TokenURL:     os.Getenv("AUTH_TOKEN_URL"),
		ClientID:     os.Getenv("AUTH_CLIENT_ID"),
		ClientSecret: os.Getenv("AUTH_CLIENT_SECRET"),
		Scopes:       strings.Fields(os.Getenv("AUTH_SCOPES")),
	}
}

// httpSettings configure the client for the register and the result endpoint.
type httpSettings struct {
	Timeout time.Duration
	APIKey  string
	Auth    commonauth.ClientCredentialsConfig
}

type controllerConfig struct {
	Orchestration orchestration.ControllerConfig
	HTTP          httpSettings
}

func loadControllerConfig() (controllerConfig, error) {
	var e env
	cfg := controllerConfig{
		Orchestration: orchestration.ControllerConfig{
			Namespace:       e.required("POD_NAMESPACE"),
			PodName:         e.required("POD_NAME"),
			ContainerName:   e.string("ORT_CONTROLLER_CONTAINER", "ort-runner"),
			BatchName:       e.required("ORT_BATCH_ID"),
			RepositoriesURL: e.required("ORT_REPOSITORIES_URL"),
			ResultsURL:      os.Getenv(register.ResultsURLVariable),
			ConfigDir:       e.required("ORT_CONFIG_DIR"),
			Settings: orchestration.WorkerSettings{
				Parallelism:             int32(e.int("ORT_PARALLELISM", 10)),
				Deadline:                e.duration("ORT_BATCH_DEADLINE", 46*time.Hour),
				RepositoryTimeout:       e.duration("ORT_REPOSITORY_TIMEOUT", 10*time.Minute),
				CPURequest:              e.string("ORT_WORKER_CPU_REQUEST", "200m"),
				CPULimit:                e.string("ORT_WORKER_CPU_LIMIT", "2000m"),
				MemoryRequest:           e.string("ORT_WORKER_MEMORY_REQUEST", "4Gi"),
				MemoryLimit:             e.string("ORT_WORKER_MEMORY_LIMIT", "8Gi"),
				EphemeralStorageRequest: e.string("ORT_WORKER_EPHEMERAL_STORAGE_REQUEST", "10Gi"),
				EphemeralStorageLimit:   e.string("ORT_WORKER_EPHEMERAL_STORAGE_LIMIT", "30Gi"),
			},
		},
		HTTP: httpSettings{
			Timeout: e.duration("ORT_HTTP_TIMEOUT", 30*time.Second),
			APIKey:  os.Getenv(register.APIKeyVariable),
			Auth:    e.auth(),
		},
	}
	if err := e.err(); err != nil {
		return cfg, err
	}
	return cfg, cfg.Orchestration.Settings.Validate()
}

func loadWorkerConfig() (worker.ScanConfig, error) {
	var e env
	timeout := e.duration("ORT_REPOSITORY_TIMEOUT", 10*time.Minute)
	index := e.int("JOB_COMPLETION_INDEX", -1)
	if index < 0 {
		e.errs = append(e.errs, errors.New("JOB_COMPLETION_INDEX must be a non-negative integer"))
	}
	cfg := worker.ScanConfig{
		BatchID:         e.required("ORT_BATCH_ID"),
		CompletionIndex: index,
		ManifestDir:     e.required("ORT_MANIFEST_DIR"),
		Timeout:         timeout,
		Runner: runner.Config{
			ConfigDir:     e.string("ORT_CONFIG_DIR", "/config"),
			OutputDir:     e.string("ORT_OUTPUT_DIR", "/output"),
			ORTImage:      os.Getenv("ORT_RUNNER_ORT_IMAGE"),
			RunnerVersion: version,
			// No single stage can outlast the repository budget.
			StageTimeout: timeout,
		},
	}
	return cfg, e.err()
}

type deliverConfig struct {
	Worker worker.DeliverConfig
	HTTP   httpSettings
}

func loadDeliverConfig() (deliverConfig, error) {
	var e env
	cfg := deliverConfig{
		Worker: worker.DeliverConfig{
			BatchID:    e.required("ORT_BATCH_ID"),
			OutputDir:  e.string("ORT_OUTPUT_DIR", "/output"),
			ResultsURL: os.Getenv(register.ResultsURLVariable),
		},
		HTTP: httpSettings{Timeout: e.duration("ORT_HTTP_TIMEOUT", 30*time.Second), Auth: e.auth()},
	}
	return cfg, e.err()
}
