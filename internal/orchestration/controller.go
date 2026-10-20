package orchestration

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/developer-overheid-nl/ort-runner/internal/register"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
)

// A ConfigMap holds at most 1 MiB; 700 KiB per chunk leaves room for metadata.
const manifestChunkLimit = 700 * 1024

type ControllerConfig struct {
	Namespace       string
	PodName         string
	ContainerName   string
	BatchName       string
	RepositoriesURL string
	ResultsURL      string
	ConfigDir       string
	Settings        WorkerSettings
	RegisterClient  register.Client
	FailureReporter FailureReporter
}

type BatchSummary struct {
	BatchID             string   `json:"batchId"`
	Total               int      `json:"total"`
	Completed           int      `json:"completed"`
	Failed              int      `json:"failed"`
	FailedRepositoryIDs []string `json:"failedRepositoryIds"`
	Rounds              int      `json:"rounds"`
}

// FailureReporter records repositories whose worker never delivered a result.
type FailureReporter interface {
	ReportFailedIndex(context.Context, string, register.Repository) error
}

// RunController runs the batch in one or more Indexed Job rounds and waits until
// every repository has completed or failed. A restarted controller resumes the
// rounds that an earlier attempt of the same controller Job created.
func RunController(ctx context.Context, client kubernetes.Interface, cfg ControllerConfig) (BatchSummary, error) {
	if client == nil {
		return BatchSummary{}, fmt.Errorf("Kubernetes client is required")
	}
	if cfg.Namespace == "" || cfg.PodName == "" || cfg.ContainerName == "" || cfg.BatchName == "" || cfg.RepositoriesURL == "" || cfg.ConfigDir == "" {
		return BatchSummary{}, fmt.Errorf("namespace, Pod, container, batch, repository URL and config directory are required")
	}
	if cfg.ResultsURL != "" && cfg.FailureReporter == nil {
		return BatchSummary{}, fmt.Errorf("failure reporter is required when results are configured")
	}
	if err := cfg.Settings.Validate(); err != nil {
		return BatchSummary{}, err
	}
	pod, err := client.CoreV1().Pods(cfg.Namespace).Get(ctx, cfg.PodName, metav1.GetOptions{})
	if err != nil {
		return BatchSummary{}, fmt.Errorf("read controller Pod: %w", err)
	}
	owner, image, environment, err := inspectControllerPod(pod, cfg.ContainerName)
	if err != nil {
		return BatchSummary{}, err
	}
	b := batch{client: client, cfg: cfg, owner: owner}

	manifest, chunks, err := b.roundManifest(ctx, 0, func() ([]register.Repository, error) {
		return cfg.RegisterClient.RepositorySets(ctx, cfg.RepositoriesURL)
	})
	if err != nil {
		return BatchSummary{}, err
	}
	summary := BatchSummary{BatchID: cfg.BatchName, Total: len(manifest.Repositories), FailedRepositoryIDs: []string{}}
	if summary.Total == 0 {
		return summary, nil
	}
	rules, err := readRules(cfg.ConfigDir)
	if err != nil {
		return BatchSummary{}, err
	}
	template := WorkerResources{
		Image:             image,
		BatchID:           cfg.BatchName,
		Rules:             rules,
		ResultEnvironment: ResultEnvironment(environment),
		Settings:          cfg.Settings,
	}

	var failed []register.Repository
	var batchEnd time.Time
	for round := 0; ; round++ {
		if round > 0 {
			manifest, chunks, err = b.roundManifest(ctx, round, func() ([]register.Repository, error) {
				return manifest.Repositories, nil
			})
			if err != nil {
				return summary, err
			}
		}
		resources := template
		resources.Round = round
		resources.RepositoryCount = len(manifest.Repositories)
		resources.ManifestChunks = chunks
		resources.RoundDeadline = cfg.Settings.Deadline
		if round > 0 {
			// At least one second: a restarted controller may adopt a round after
			// the batch deadline; a new Job then stops immediately.
			resources.RoundDeadline = max(time.Until(batchEnd).Truncate(time.Second), time.Second)
		}
		job, err := b.runRound(ctx, resources)
		if err != nil {
			return summary, err
		}
		if round == 0 {
			batchEnd = job.CreationTimestamp.Add(cfg.Settings.Deadline)
		}
		outcome, err := roundOutcome(job, manifest)
		if err != nil {
			return summary, err
		}
		summary.Rounds = round + 1
		summary.Completed += outcome.completed
		failed = append(failed, outcome.failed...)
		slog.Info("ORT round finished", "batch_id", cfg.BatchName, "round", round, "completed", outcome.completed,
			"failed", len(outcome.failed), "unfinished", len(outcome.unfinished), "reason", outcome.reason)

		progressed := outcome.completed > 0 || len(outcome.failed) > 0
		retry := outcome.reason == batchv1.JobReasonBackoffLimitExceeded && progressed &&
			time.Until(batchEnd) >= time.Minute
		if len(outcome.unfinished) == 0 || !retry {
			failed = append(failed, outcome.unfinished...)
			break
		}
		manifest = Manifest{Repositories: outcome.unfinished}
	}

	summary.Failed = len(failed)
	for _, repository := range failed {
		summary.FailedRepositoryIDs = append(summary.FailedRepositoryIDs, repository.ID)
		if cfg.ResultsURL == "" {
			continue
		}
		if err := cfg.FailureReporter.ReportFailedIndex(ctx, cfg.BatchName, repository); err != nil {
			return summary, fmt.Errorf("report failed repository %s: %w", repository.ID, err)
		}
	}
	sort.Strings(summary.FailedRepositoryIDs)
	return summary, nil
}

type batch struct {
	client kubernetes.Interface
	cfg    ControllerConfig
	owner  metav1.OwnerReference
}

// runRound creates or adopts the ConfigMaps and worker Job of one round and
// waits until that Job finishes.
func (b batch) runRound(ctx context.Context, resources WorkerResources) (*batchv1.Job, error) {
	configMaps, err := BuildConfigMaps(b.owner, b.cfg.Namespace, resources)
	if err != nil {
		return nil, err
	}
	// Chunk 0 is created last: its presence proves all other chunks exist, which
	// roundManifest relies on after a controller restart.
	slices.Reverse(configMaps[1:])
	for _, desired := range configMaps {
		if err := createOrVerifyConfigMap(ctx, b.client, desired); err != nil {
			return nil, err
		}
	}
	desired, err := BuildIndexedJob(b.owner, b.cfg.Namespace, workerJobName(b.owner, resources.Round), resources)
	if err != nil {
		return nil, err
	}
	if _, err := b.client.BatchV1().Jobs(desired.Namespace).Get(ctx, desired.Name, metav1.GetOptions{}); apierrors.IsNotFound(err) {
		// A later round only exists once this round finished. Recreating this
		// round would rescan everything in it, so refuse instead.
		later, err := storedManifestChunks(ctx, b.client, b.cfg.Namespace, b.owner, resources.Round+1)
		if err != nil {
			return nil, err
		}
		if later != nil {
			return nil, fmt.Errorf("Job of round %d no longer exists while round %d does; refusing to rerun round %d", resources.Round, resources.Round+1, resources.Round)
		}
	}
	if err := createOrVerifyJob(ctx, b.client, desired); err != nil {
		return nil, err
	}
	return waitForTerminalJob(ctx, b.client, b.cfg.Namespace, desired.Name)
}

// roundManifest reuses the repository list an earlier controller attempt stored
// for this round, so a register change or restart cannot alter a running batch.
// Only when nothing is stored yet does it call repositories.
func (b batch) roundManifest(ctx context.Context, round int, repositories func() ([]register.Repository, error)) (Manifest, []Chunk, error) {
	stored, err := storedManifestChunks(ctx, b.client, b.cfg.Namespace, b.owner, round)
	if err != nil {
		return Manifest{}, nil, err
	}
	if stored != nil {
		manifest, err := DecodeManifest(stored)
		if err != nil {
			return Manifest{}, nil, fmt.Errorf("stored manifest of round %d: %w", round, err)
		}
		if manifest.BatchID != b.cfg.BatchName {
			return Manifest{}, nil, fmt.Errorf("stored manifest belongs to batch %q", manifest.BatchID)
		}
		return manifest, stored, nil
	}

	list, err := repositories()
	if err != nil {
		return Manifest{}, nil, fmt.Errorf("read repositories: %w", err)
	}
	manifest, err := NewManifest(b.cfg.BatchName, list)
	if err != nil {
		return Manifest{}, nil, err
	}
	chunks, err := EncodeManifest(manifest, manifestChunkLimit)
	if err != nil {
		return Manifest{}, nil, fmt.Errorf("encode repository manifest: %w", err)
	}
	return manifest, chunks, nil
}

func storedManifestChunks(ctx context.Context, client kubernetes.Interface, namespace string, owner metav1.OwnerReference, round int) ([]Chunk, error) {
	first, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, manifestConfigMapName(owner, round, 0), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	count, err := strconv.Atoi(first.Annotations[chunkCountAnnotation])
	if err != nil || count <= 0 {
		return nil, fmt.Errorf("stored manifest chunk count %q is invalid", first.Annotations[chunkCountAnnotation])
	}
	chunks := make([]Chunk, 0, count)
	for index := range count {
		configMap := first
		if index > 0 {
			configMap, err = client.CoreV1().ConfigMaps(namespace).Get(ctx, manifestConfigMapName(owner, round, index), metav1.GetOptions{})
			if err != nil {
				return nil, fmt.Errorf("read stored manifest chunk %d: %w", index, err)
			}
		}
		if !sameOwner(configMap.OwnerReferences, owner) {
			return nil, fmt.Errorf("ConfigMap %s conflict: not owned by this batch", configMap.Name)
		}
		data := []byte(configMap.Data[manifestChunkKey])
		chunks = append(chunks, Chunk{Index: index, Count: count, Digest: digest(data), Data: data})
	}
	return chunks, nil
}

// ParseIndexes parses the compact index list of a Job status, such as "0,2-4".
func ParseIndexes(value string, total int) ([]int, error) {
	if total < 0 {
		return nil, fmt.Errorf("total must not be negative")
	}
	if value == "" {
		return []int{}, nil
	}
	seen := make(map[int]struct{})
	indexes := make([]int, 0)
	for _, part := range strings.Split(value, ",") {
		bounds := strings.Split(part, "-")
		if len(bounds) > 2 {
			return nil, fmt.Errorf("invalid index expression %q", part)
		}
		first, err := strconv.Atoi(bounds[0])
		if err != nil || first < 0 || first >= total {
			return nil, fmt.Errorf("index %q outside range", part)
		}
		last := first
		if len(bounds) == 2 {
			last, err = strconv.Atoi(bounds[1])
			if err != nil || last < first || last >= total {
				return nil, fmt.Errorf("index range %q outside range", part)
			}
		}
		for index := first; index <= last; index++ {
			if _, ok := seen[index]; ok {
				return nil, fmt.Errorf("duplicate index %d", index)
			}
			seen[index] = struct{}{}
			indexes = append(indexes, index)
		}
	}
	sort.Ints(indexes)
	return indexes, nil
}

func inspectControllerPod(pod *corev1.Pod, containerName string) (metav1.OwnerReference, string, []corev1.EnvVar, error) {
	var owners []metav1.OwnerReference
	for _, owner := range pod.OwnerReferences {
		if owner.Controller != nil && *owner.Controller && owner.Kind == "Job" && owner.UID != "" {
			owners = append(owners, owner)
		}
	}
	if len(owners) != 1 {
		return metav1.OwnerReference{}, "", nil, fmt.Errorf("controller Pod must have exactly one controlling Job owner")
	}
	for _, container := range pod.Spec.Containers {
		if container.Name == containerName {
			if container.Image == "" {
				return metav1.OwnerReference{}, "", nil, fmt.Errorf("controller image is empty")
			}
			return owners[0], container.Image, container.Env, nil
		}
	}
	return metav1.OwnerReference{}, "", nil, fmt.Errorf("controller container %q not found", containerName)
}

// readRules reads the evaluator rules; it is the only file ORT is given.
func readRules(configDir string) ([]byte, error) {
	rules, err := os.ReadFile(filepath.Join(configDir, rulesFile))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", rulesFile, err)
	}
	if len(rules) == 0 {
		return nil, fmt.Errorf("%s is empty", rulesFile)
	}
	return rules, nil
}

// Existing batch resources are never updated: a difference means another
// snapshot, so the controller fails instead of mixing two batches.
func createOrVerifyConfigMap(ctx context.Context, client kubernetes.Interface, desired *corev1.ConfigMap) error {
	existing, err := client.CoreV1().ConfigMaps(desired.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = client.CoreV1().ConfigMaps(desired.Namespace).Create(ctx, desired, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if !sameOwner(existing.OwnerReferences, desired.OwnerReferences[0]) || existing.Immutable == nil || !*existing.Immutable ||
		!containsLabels(existing.Labels, desired.Labels) || !reflect.DeepEqual(existing.Annotations, desired.Annotations) ||
		!reflect.DeepEqual(existing.Data, desired.Data) || !reflect.DeepEqual(existing.BinaryData, desired.BinaryData) {
		return fmt.Errorf("ConfigMap %s conflict", desired.Name)
	}
	return nil
}

func createOrVerifyJob(ctx context.Context, client kubernetes.Interface, desired *batchv1.Job) error {
	existing, err := client.BatchV1().Jobs(desired.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = client.BatchV1().Jobs(desired.Namespace).Create(ctx, desired, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if !sameOwner(existing.OwnerReferences, desired.OwnerReferences[0]) || existing.Annotations[specHashAnnotation] != desired.Annotations[specHashAnnotation] {
		return fmt.Errorf("Job %s conflict", desired.Name)
	}
	return nil
}

func sameOwner(owners []metav1.OwnerReference, desired metav1.OwnerReference) bool {
	return slices.ContainsFunc(owners, func(owner metav1.OwnerReference) bool {
		return owner.UID == desired.UID && owner.Name == desired.Name && owner.Kind == desired.Kind && owner.Controller != nil && *owner.Controller
	})
}

func containsLabels(existing, desired map[string]string) bool {
	for key, value := range desired {
		if existing[key] != value {
			return false
		}
	}
	return true
}

// jobPollInterval is how often the controller checks a running round.
var jobPollInterval = 30 * time.Second

// waitForTerminalJob polls the Job until it finishes. Transient API errors are
// logged and retried; a removed Job stops the controller.
func waitForTerminalJob(ctx context.Context, client kubernetes.Interface, namespace, name string) (*batchv1.Job, error) {
	var terminal *batchv1.Job
	err := wait.PollUntilContextCancel(ctx, jobPollInterval, true, func(ctx context.Context) (bool, error) {
		job, err := client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return false, fmt.Errorf("worker Job %s was removed while running", name)
		}
		if err != nil {
			slog.Warn("Reading worker Job failed; retrying", "job", name, "error", err)
			return false, nil
		}
		if jobTerminal(job) {
			terminal = job
			return true, nil
		}
		return false, nil
	})
	return terminal, err
}

func jobTerminal(job *batchv1.Job) bool {
	return slices.ContainsFunc(job.Status.Conditions, func(condition batchv1.JobCondition) bool {
		return condition.Status == corev1.ConditionTrue && (condition.Type == batchv1.JobComplete || condition.Type == batchv1.JobFailed)
	})
}

type outcome struct {
	completed  int
	failed     []register.Repository
	unfinished []register.Repository
	reason     string
}

// roundOutcome splits the repositories of a finished round into completed,
// failed (their Pod failed) and unfinished (never ran, or stopped because the
// round ended early).
func roundOutcome(job *batchv1.Job, manifest Manifest) (outcome, error) {
	total := len(manifest.Repositories)
	completed, err := ParseIndexes(job.Status.CompletedIndexes, total)
	if err != nil {
		return outcome{}, fmt.Errorf("parse completed indexes: %w", err)
	}
	failed, err := ParseIndexes(ptr.Deref(job.Status.FailedIndexes, ""), total)
	if err != nil {
		return outcome{}, fmt.Errorf("parse failed indexes: %w", err)
	}
	state := make([]byte, total)
	for _, index := range completed {
		state[index] = 'c'
	}
	result := outcome{completed: len(completed)}
	for _, index := range failed {
		if state[index] == 'c' {
			return outcome{}, fmt.Errorf("index %d is both completed and failed", index)
		}
		state[index] = 'f'
		result.failed = append(result.failed, manifest.Repositories[index])
	}
	for index, value := range state {
		if value == 0 {
			result.unfinished = append(result.unfinished, manifest.Repositories[index])
		}
	}
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			result.reason = condition.Reason
		}
	}
	return result, nil
}
