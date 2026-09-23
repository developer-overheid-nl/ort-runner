package orchestration

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/developer-overheid-nl/ort-runner/internal/register"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

const manifestChunkLimit = 700 << 10

type ControllerConfig struct {
	Namespace       string
	PodName         string
	ContainerName   string
	BatchName       string
	RepositoriesURL string
	ResultsURL      string
	ConfigDir       string
	Parallelism     int32
	RetryLimit      int32
	WorkerResources WorkerResources
	RegisterClient  register.Client
	ResultsClient   register.Client
	FailureReporter FailureReporter
}

type BatchSummary struct {
	BatchID             string   `json:"batchId"`
	Total               int      `json:"total"`
	Completed           int      `json:"completed"`
	Failed              int      `json:"failed"`
	FailedRepositoryIDs []string `json:"failedRepositoryIds"`
}

type FailureReporter interface {
	ReportFailedIndex(context.Context, string, register.Repository) error
}

func RunController(ctx context.Context, client kubernetes.Interface, cfg ControllerConfig) (BatchSummary, error) {
	if client == nil {
		return BatchSummary{}, fmt.Errorf("Kubernetes client is required")
	}
	if cfg.Namespace == "" || cfg.PodName == "" || cfg.ContainerName == "" || cfg.BatchName == "" || cfg.RepositoriesURL == "" || cfg.ConfigDir == "" {
		return BatchSummary{}, fmt.Errorf("namespace, Pod, container, batch, repository URL and config directory are required")
	}
	pod, err := client.CoreV1().Pods(cfg.Namespace).Get(ctx, cfg.PodName, metav1.GetOptions{})
	if err != nil {
		return BatchSummary{}, fmt.Errorf("read controller Pod: %w", err)
	}
	owner, image, environment, err := inspectControllerPod(pod, cfg.ContainerName)
	if err != nil {
		return BatchSummary{}, err
	}

	repositories, err := cfg.RegisterClient.RepositorySets(ctx, cfg.RepositoriesURL)
	if err != nil {
		return BatchSummary{}, fmt.Errorf("read repositories: %w", err)
	}
	if len(repositories) == 0 {
		return BatchSummary{BatchID: cfg.BatchName, FailedRepositoryIDs: []string{}}, nil
	}
	manifest := Manifest{SchemaVersion: manifestSchemaVersion, BatchID: cfg.BatchName, Repositories: repositories}
	chunks, err := EncodeManifest(manifest, manifestChunkLimit)
	if err != nil {
		return BatchSummary{}, fmt.Errorf("encode repository manifest: %w", err)
	}
	manifest, err = DecodeManifest(chunks)
	if err != nil {
		return BatchSummary{}, fmt.Errorf("verify repository manifest: %w", err)
	}
	configData, err := readConfigData(cfg.ConfigDir)
	if err != nil {
		return BatchSummary{}, err
	}

	resources := cfg.WorkerResources.deepCopy()
	resources.Image = image
	resources.BatchID = cfg.BatchName
	resources.RepositoryCount = len(manifest.Repositories)
	resources.Parallelism = cfg.Parallelism
	resources.RetryLimit = cfg.RetryLimit
	resources.ConfigData = configData
	resources.ManifestChunks = chunks
	resources.Environment = environment
	configMaps, err := BuildConfigMaps(owner, cfg.Namespace, resources)
	if err != nil {
		return BatchSummary{}, err
	}
	for _, desired := range configMaps {
		if err := createOrVerifyConfigMap(ctx, client, desired); err != nil {
			return BatchSummary{}, err
		}
	}
	jobName := resourceName(owner, "workers")
	desiredJob, err := BuildIndexedJob(owner, cfg.Namespace, jobName, resources)
	if err != nil {
		return BatchSummary{}, err
	}
	if err := createOrVerifyJob(ctx, client, desiredJob); err != nil {
		return BatchSummary{}, err
	}

	terminal, err := waitForTerminalJob(ctx, client, cfg.Namespace, jobName)
	if err != nil {
		return BatchSummary{}, err
	}
	summary, failedIndexes, err := summarizeJob(terminal, len(manifest.Repositories), cfg.BatchName)
	if err != nil {
		return BatchSummary{}, err
	}
	for _, index := range failedIndexes {
		repository, lookupErr := manifest.Repository(index)
		if lookupErr != nil {
			return summary, lookupErr
		}
		summary.FailedRepositoryIDs = append(summary.FailedRepositoryIDs, repository.ID)
		if cfg.ResultsURL != "" {
			if cfg.FailureReporter == nil {
				return summary, fmt.Errorf("failed index reporter is required when results are configured")
			}
			if reportErr := cfg.FailureReporter.ReportFailedIndex(ctx, cfg.BatchName, repository); reportErr != nil {
				return summary, fmt.Errorf("report failed repository %s: %w", repository.ID, reportErr)
			}
		}
	}
	sort.Strings(summary.FailedRepositoryIDs)
	return summary, nil
}

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
		if part == "" || strings.TrimSpace(part) != part {
			return nil, fmt.Errorf("invalid index expression %q", value)
		}
		bounds := strings.Split(part, "-")
		if len(bounds) > 2 || bounds[0] == "" || len(bounds) == 2 && bounds[1] == "" {
			return nil, fmt.Errorf("invalid index expression %q", part)
		}
		first, err := strconv.Atoi(bounds[0])
		if err != nil || first < 0 || first >= total {
			return nil, fmt.Errorf("index %q outside range", bounds[0])
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
	owners := make([]metav1.OwnerReference, 0, 1)
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
			return owners[0], container.Image, selectWorkerEnvironment(container.Env), nil
		}
	}
	return metav1.OwnerReference{}, "", nil, fmt.Errorf("controller container %q not found", containerName)
}

func selectWorkerEnvironment(environment []corev1.EnvVar) []corev1.EnvVar {
	allowed := map[string]struct{}{
		"ORT_RESULTS_URL": {}, "AUTH_TOKEN_URL": {}, "AUTH_CLIENT_ID": {}, "AUTH_CLIENT_SECRET": {}, "AUTH_SCOPES": {},
		"KEYCLOAK_BASE_URL": {}, "KEYCLOAK_REALM": {},
	}
	result := make([]corev1.EnvVar, 0, len(environment))
	for _, variable := range environment {
		if _, ok := allowed[variable.Name]; ok {
			result = append(result, *variable.DeepCopy())
		}
	}
	return result
}

func readConfigData(root string) (map[string][]byte, error) {
	data := make(map[string][]byte)
	err := filepath.WalkDir(root, func(file string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if file == root || entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
			return fmt.Errorf("config entry %s is not a regular file", file)
		}
		relative, err := filepath.Rel(root, file)
		if err != nil {
			return err
		}
		contents, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		data[filepath.ToSlash(relative)] = contents
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read config directory: %w", err)
	}
	if rules, ok := data["evaluator.rules.kts"]; !ok || len(rules) == 0 {
		return nil, fmt.Errorf("config directory must contain evaluator.rules.kts")
	}
	return data, nil
}

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
	if !sameOwner(existing.OwnerReferences, desired.OwnerReferences[0]) || !containsLabels(existing.Labels, desired.Labels) || !sameJobSpec(existing.Spec, desired.Spec) {
		return fmt.Errorf("Job %s conflict", desired.Name)
	}
	return nil
}

func sameJobSpec(existing, desired batchv1.JobSpec) bool {
	if !reflect.DeepEqual(existing.Completions, desired.Completions) || !reflect.DeepEqual(existing.Parallelism, desired.Parallelism) ||
		!reflect.DeepEqual(existing.CompletionMode, desired.CompletionMode) || !reflect.DeepEqual(existing.BackoffLimitPerIndex, desired.BackoffLimitPerIndex) ||
		!reflect.DeepEqual(existing.MaxFailedIndexes, desired.MaxFailedIndexes) || !reflect.DeepEqual(existing.TTLSecondsAfterFinished, desired.TTLSecondsAfterFinished) ||
		existing.Template.Spec.RestartPolicy != desired.Template.Spec.RestartPolicy ||
		!reflect.DeepEqual(existing.Template.Spec.AutomountServiceAccountToken, desired.Template.Spec.AutomountServiceAccountToken) ||
		!reflect.DeepEqual(existing.Template.Spec.SecurityContext, desired.Template.Spec.SecurityContext) || !sameVolumes(existing.Template.Spec.Volumes, desired.Template.Spec.Volumes) ||
		len(existing.Template.Spec.Containers) != len(desired.Template.Spec.Containers) || !containsLabels(existing.Template.Labels, desired.Template.Labels) {
		return false
	}
	for index := range desired.Template.Spec.Containers {
		a, b := existing.Template.Spec.Containers[index], desired.Template.Spec.Containers[index]
		if a.Name != b.Name || a.Image != b.Image || !reflect.DeepEqual(a.Command, b.Command) || !reflect.DeepEqual(a.Args, b.Args) ||
			!reflect.DeepEqual(a.Env, b.Env) || !apiequality.Semantic.DeepEqual(a.Resources, b.Resources) || !reflect.DeepEqual(a.VolumeMounts, b.VolumeMounts) ||
			!reflect.DeepEqual(a.SecurityContext, b.SecurityContext) {
			return false
		}
	}
	return true
}

func sameVolumes(existing, desired []corev1.Volume) bool {
	if len(existing) != len(desired) {
		return false
	}
	for index := range desired {
		a, b := existing[index], desired[index]
		if a.Name != b.Name {
			return false
		}
		if b.EmptyDir != nil {
			if !reflect.DeepEqual(a.EmptyDir, b.EmptyDir) {
				return false
			}
			continue
		}
		if b.Projected == nil || a.Projected == nil || len(a.Projected.Sources) != len(b.Projected.Sources) {
			return false
		}
		for source := range b.Projected.Sources {
			if !reflect.DeepEqual(a.Projected.Sources[source], b.Projected.Sources[source]) {
				return false
			}
		}
	}
	return true
}

func sameOwner(owners []metav1.OwnerReference, desired metav1.OwnerReference) bool {
	for _, owner := range owners {
		if owner.UID == desired.UID && owner.Name == desired.Name && owner.Kind == desired.Kind && owner.Controller != nil && *owner.Controller {
			return true
		}
	}
	return false
}

func containsLabels(existing, desired map[string]string) bool {
	for key, value := range desired {
		if existing[key] != value {
			return false
		}
	}
	return true
}

func waitForTerminalJob(ctx context.Context, client kubernetes.Interface, namespace, name string) (*batchv1.Job, error) {
	for {
		job, err := client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if jobTerminal(job) {
			return job, nil
		}
		watcher, err := client.BatchV1().Jobs(namespace).Watch(ctx, metav1.ListOptions{FieldSelector: fields.OneTermEqualSelector("metadata.name", name).String(), ResourceVersion: job.ResourceVersion})
		if err != nil {
			return nil, err
		}
		ticker := time.NewTicker(15 * time.Second)
		resync := false
		for !resync {
			select {
			case <-ctx.Done():
				watcher.Stop()
				ticker.Stop()
				return nil, ctx.Err()
			case <-ticker.C:
				resync = true
			case event, open := <-watcher.ResultChan():
				if !open || event.Type == watch.Error {
					resync = true
					continue
				}
				updated, ok := event.Object.(*batchv1.Job)
				if ok && updated.Name == name && jobTerminal(updated) {
					watcher.Stop()
					ticker.Stop()
					return updated, nil
				}
			}
		}
		watcher.Stop()
		ticker.Stop()
	}
}

func jobTerminal(job *batchv1.Job) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Status == corev1.ConditionTrue && (condition.Type == batchv1.JobComplete || condition.Type == batchv1.JobFailed) {
			return true
		}
	}
	return false
}

func summarizeJob(job *batchv1.Job, total int, batchID string) (BatchSummary, []int, error) {
	completed, err := ParseIndexes(job.Status.CompletedIndexes, total)
	if err != nil {
		return BatchSummary{}, nil, fmt.Errorf("parse completed indexes: %w", err)
	}
	failedValue := ""
	if job.Status.FailedIndexes != nil {
		failedValue = *job.Status.FailedIndexes
	}
	failed, err := ParseIndexes(failedValue, total)
	if err != nil {
		return BatchSummary{}, nil, fmt.Errorf("parse failed indexes: %w", err)
	}
	seen := make(map[int]struct{}, len(completed))
	for _, index := range completed {
		seen[index] = struct{}{}
	}
	for _, index := range failed {
		if _, ok := seen[index]; ok {
			return BatchSummary{}, nil, fmt.Errorf("index %d is both completed and failed", index)
		}
	}
	if len(completed)+len(failed) != total {
		return BatchSummary{}, nil, fmt.Errorf("terminal Job accounts for %d of %d indexes", len(completed)+len(failed), total)
	}
	return BatchSummary{BatchID: batchID, Total: total, Completed: len(completed), Failed: len(failed), FailedRepositoryIDs: []string{}}, failed, nil
}
