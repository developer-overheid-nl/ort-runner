package orchestration

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/developer-overheid-nl/ort-runner/internal/register"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	resourcePurposeLabel = "ort-runner.developer.overheid.nl/resource-purpose"
	resourceDigestLabel  = "ort-runner.developer.overheid.nl/digest"
	chunkIndexAnnotation = "ort-runner.developer.overheid.nl/chunk-index"
	chunkCountAnnotation = "ort-runner.developer.overheid.nl/chunk-count"
	manifestChunkKey     = "chunk.json"
	rulesVolumeName      = "rules"
	manifestVolumeName   = "manifest"
	gradleVolumeName     = "gradle"
	outputVolumeName     = "output"
	scanContainerName    = "scan"
	deliverContainerName = "deliver"

	// One retry per repository: an OOM or crash may be transient, a second one is not.
	retriesPerRepository = 1
	// ORT gets half of the default 8Gi limit; Gradle, package managers and JVM native memory share the rest.
	ortJavaOptions   = "-Xmx4g"
	gradleProperties = "org.gradle.jvmargs=-Xmx2g -XX:MaxMetaspaceSize=512m\norg.gradle.daemon=false\n"
	finishedJobTTL   = 24 * 60 * 60
	ortUserGroup     = 1000
)

// WorkerResources describes one batch of repository workers.
type WorkerResources struct {
	Image           string
	BatchID         string
	RepositoryCount int
	ConfigData      map[string][]byte
	ManifestChunks  []Chunk
	// ResultEnvironment is passed only to the delivery container; see ResultEnvironment.
	ResultEnvironment []corev1.EnvVar
	Settings          WorkerSettings
}

// WorkerSettings are the operator-tunable limits of a batch.
type WorkerSettings struct {
	Parallelism             int32
	Deadline                time.Duration
	RepositoryTimeout       time.Duration
	CPURequest              string
	CPULimit                string
	MemoryRequest           string
	MemoryLimit             string
	EphemeralStorageRequest string
	EphemeralStorageLimit   string
}

type workerQuantities struct {
	requests, limits corev1.ResourceList
}

func (settings WorkerSettings) Validate() error {
	_, err := settings.quantities()
	return err
}

func (settings WorkerSettings) quantities() (workerQuantities, error) {
	if settings.Parallelism < 1 || settings.Parallelism > 100 {
		return workerQuantities{}, fmt.Errorf("parallelism must be between 1 and 100")
	}
	if settings.Deadline <= 0 || settings.RepositoryTimeout <= 0 {
		return workerQuantities{}, fmt.Errorf("batch deadline and repository timeout must be positive")
	}
	parse := func(name, value string) (resource.Quantity, error) {
		quantity, err := resource.ParseQuantity(value)
		if err != nil || quantity.Sign() <= 0 {
			return resource.Quantity{}, fmt.Errorf("invalid worker %s %q", name, value)
		}
		return quantity, nil
	}
	result := workerQuantities{requests: corev1.ResourceList{}, limits: corev1.ResourceList{}}
	for _, field := range []struct {
		list  corev1.ResourceList
		name  corev1.ResourceName
		label string
		value string
	}{
		{result.requests, corev1.ResourceCPU, "CPU request", settings.CPURequest},
		{result.limits, corev1.ResourceCPU, "CPU limit", settings.CPULimit},
		{result.requests, corev1.ResourceMemory, "memory request", settings.MemoryRequest},
		{result.limits, corev1.ResourceMemory, "memory limit", settings.MemoryLimit},
		{result.requests, corev1.ResourceEphemeralStorage, "ephemeral storage request", settings.EphemeralStorageRequest},
		{result.limits, corev1.ResourceEphemeralStorage, "ephemeral storage limit", settings.EphemeralStorageLimit},
	} {
		quantity, err := parse(field.label, field.value)
		if err != nil {
			return workerQuantities{}, err
		}
		field.list[field.name] = quantity
	}
	return result, nil
}

// ResultEnvironment selects the controller variables that the delivery
// container needs. Credentials are only passed on when a result endpoint is set.
func ResultEnvironment(environment []corev1.EnvVar) []corev1.EnvVar {
	enabled := slices.ContainsFunc(environment, func(variable corev1.EnvVar) bool {
		return variable.Name == register.ResultsURLVariable && (variable.Value != "" || variable.ValueFrom != nil)
	})
	result := make([]corev1.EnvVar, 0, len(environment))
	for _, variable := range environment {
		isCredential := slices.Contains(register.ResultCredentialVariables, variable.Name)
		if variable.Name == register.ResultsURLVariable || enabled && isCredential {
			result = append(result, *variable.DeepCopy())
		}
	}
	return result
}

func BuildConfigMaps(owner metav1.OwnerReference, namespace string, resources WorkerResources) ([]*corev1.ConfigMap, error) {
	if err := validateResources(owner, namespace, resources); err != nil {
		return nil, err
	}
	immutable := true
	rulesData := map[string]string{"gradle.properties": gradleProperties}
	for index, entry := range sortedConfigEntries(resources.ConfigData) {
		rulesData[configMapKey(index)] = string(entry.data)
	}
	maps := []*corev1.ConfigMap{{
		ObjectMeta: metav1.ObjectMeta{
			Name:            rulesConfigMapName(owner),
			Namespace:       namespace,
			OwnerReferences: []metav1.OwnerReference{owner},
			Labels:          resourceLabels("rules", digestConfigData(resources.ConfigData)),
		},
		Immutable: &immutable,
		Data:      rulesData,
	}}
	for _, chunk := range resources.ManifestChunks {
		maps = append(maps, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:            manifestConfigMapName(owner, chunk.Index),
				Namespace:       namespace,
				OwnerReferences: []metav1.OwnerReference{owner},
				Labels:          resourceLabels("manifest", chunk.Digest),
				Annotations: map[string]string{
					chunkIndexAnnotation: fmt.Sprint(chunk.Index),
					chunkCountAnnotation: fmt.Sprint(chunk.Count),
				},
			},
			Immutable: &immutable,
			Data:      map[string]string{manifestChunkKey: string(chunk.Data)},
		})
	}
	return maps, nil
}

// BuildIndexedJob creates one Pod per repository. The scan runs as an init
// container without any credentials; only the small delivery container that
// runs afterwards receives the result credentials.
func BuildIndexedJob(owner metav1.OwnerReference, namespace, name string, resources WorkerResources) (*batchv1.Job, error) {
	if name == "" {
		return nil, fmt.Errorf("job name is required")
	}
	if err := validateResources(owner, namespace, resources); err != nil {
		return nil, err
	}
	quantities, err := resources.Settings.quantities()
	if err != nil {
		return nil, err
	}

	rulesItems := make([]corev1.KeyToPath, 0, len(resources.ConfigData))
	for index, entry := range sortedConfigEntries(resources.ConfigData) {
		rulesItems = append(rulesItems, corev1.KeyToPath{Key: configMapKey(index), Path: entry.path})
	}
	manifestSources := make([]corev1.VolumeProjection, 0, len(resources.ManifestChunks))
	for _, chunk := range resources.ManifestChunks {
		manifestSources = append(manifestSources, configMapProjection(manifestConfigMapName(owner, chunk.Index),
			corev1.KeyToPath{Key: manifestChunkKey, Path: fmt.Sprintf("chunk-%04d-%s.json", chunk.Index, chunk.Digest)}))
	}

	batch := []corev1.EnvVar{{Name: "ORT_BATCH_ID", Value: resources.BatchID}, {Name: "ORT_OUTPUT_DIR", Value: "/output"}}
	scan := corev1.Container{
		Name:    scanContainerName,
		Image:   resources.Image,
		Command: []string{"ort-runner", "worker"},
		Env: append(slices.Clone(batch),
			corev1.EnvVar{Name: "ORT_MANIFEST_DIR", Value: "/manifest"},
			corev1.EnvVar{Name: "ORT_CONFIG_DIR", Value: "/config"},
			corev1.EnvVar{Name: "ORT_OPTS", Value: ortJavaOptions},
			corev1.EnvVar{Name: "ORT_REPOSITORY_TIMEOUT", Value: resources.Settings.RepositoryTimeout.String()},
		),
		Resources:       corev1.ResourceRequirements{Requests: quantities.requests, Limits: quantities.limits},
		SecurityContext: restrictedContainer(),
		VolumeMounts: []corev1.VolumeMount{
			{Name: rulesVolumeName, MountPath: "/config", ReadOnly: true},
			{Name: manifestVolumeName, MountPath: "/manifest", ReadOnly: true},
			{Name: gradleVolumeName, MountPath: "/home/ort/.gradle/gradle.properties", SubPath: "gradle.properties", ReadOnly: true},
			{Name: outputVolumeName, MountPath: "/output"},
		},
	}
	deliver := corev1.Container{
		Name:    deliverContainerName,
		Image:   resources.Image,
		Command: []string{"ort-runner", "deliver"},
		Env:     append(slices.Clone(batch), resources.ResultEnvironment...),
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("32Mi")},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
		},
		SecurityContext: restrictedContainer(),
		VolumeMounts:    []corev1.VolumeMount{{Name: outputVolumeName, MountPath: "/output", ReadOnly: true}},
	}

	labels := map[string]string{
		"app.kubernetes.io/name":                 "ort-runner",
		"app.kubernetes.io/component":            "worker",
		"ort-runner.developer.overheid.nl/batch": safeLabel(resources.BatchID),
	}
	completions := int32(resources.RepositoryCount)
	parallelism := resources.Settings.Parallelism
	completionMode := batchv1.IndexedCompletion
	retries := int32(retriesPerRepository)
	deadline := int64(resources.Settings.Deadline / time.Second)
	ttl := int32(finishedJobTTL)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, OwnerReferences: []metav1.OwnerReference{owner}, Labels: labels},
		Spec: batchv1.JobSpec{
			Completions:             &completions,
			Parallelism:             &parallelism,
			CompletionMode:          &completionMode,
			BackoffLimitPerIndex:    &retries,
			ActiveDeadlineSeconds:   &deadline,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: ptr(false),
					EnableServiceLinks:           ptr(false),
					RestartPolicy:                corev1.RestartPolicyNever,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot:   ptr(true),
						FSGroup:        ptr(int64(ortUserGroup)),
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					InitContainers: []corev1.Container{scan},
					Containers:     []corev1.Container{deliver},
					Volumes: []corev1.Volume{
						projectedVolume(rulesVolumeName, configMapProjection(rulesConfigMapName(owner), rulesItems...)),
						projectedVolume(manifestVolumeName, manifestSources...),
						projectedVolume(gradleVolumeName, configMapProjection(rulesConfigMapName(owner), corev1.KeyToPath{Key: "gradle.properties", Path: "gradle.properties"})),
						{Name: outputVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					},
				},
			},
		},
	}, nil
}

func validateResources(owner metav1.OwnerReference, namespace string, resources WorkerResources) error {
	if namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	if owner.UID == "" || owner.Name == "" {
		return fmt.Errorf("owner name and UID are required")
	}
	if resources.Image == "" || resources.BatchID == "" {
		return fmt.Errorf("worker image and batch ID are required")
	}
	if resources.RepositoryCount <= 0 || len(resources.ManifestChunks) == 0 {
		return fmt.Errorf("a batch needs at least one repository")
	}
	if len(resources.ConfigData["evaluator.rules.kts"]) == 0 {
		return fmt.Errorf("evaluator.rules.kts is required")
	}
	for file := range resources.ConfigData {
		if file == "" || path.IsAbs(file) || path.Clean(file) != file || file == ".." || strings.HasPrefix(file, "../") {
			return fmt.Errorf("unsafe config path %q", file)
		}
	}
	return resources.Settings.Validate()
}

func restrictedContainer() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: ptr(false),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
}

func configMapProjection(name string, items ...corev1.KeyToPath) corev1.VolumeProjection {
	return corev1.VolumeProjection{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: name}, Items: items}}
}

func projectedVolume(name string, sources ...corev1.VolumeProjection) corev1.Volume {
	return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: sources}}}
}

func ptr[T any](value T) *T { return &value }

type configEntry struct {
	path string
	data []byte
}

func sortedConfigEntries(data map[string][]byte) []configEntry {
	paths := make([]string, 0, len(data))
	for file := range data {
		paths = append(paths, file)
	}
	sort.Strings(paths)
	entries := make([]configEntry, 0, len(paths))
	for _, file := range paths {
		entries = append(entries, configEntry{path: file, data: data[file]})
	}
	return entries
}

func configMapKey(index int) string { return fmt.Sprintf("file-%04d", index) }

const (
	dnsLabelLength = 63
	hashLength     = 10
	// Indexed Job Pods use "<job>-<index>" as hostname; an int32 index has at most 10 digits.
	workerJobNameLength = dnsLabelLength - len("-") - 10
)

func rulesConfigMapName(owner metav1.OwnerReference) string {
	return resourceName(owner, "rules", dnsLabelLength)
}

func manifestConfigMapName(owner metav1.OwnerReference, index int) string {
	return resourceName(owner, fmt.Sprintf("manifest-%04d", index), dnsLabelLength)
}

func workerJobName(owner metav1.OwnerReference) string {
	return resourceName(owner, "workers", workerJobNameLength)
}

// resourceName derives a stable DNS-1123 name of at most maxLength characters
// from the controller Job, so a restarted controller finds its own resources.
func resourceName(owner metav1.OwnerReference, purpose string, maxLength int) string {
	sum := sha256.Sum256([]byte(string(owner.UID) + "\x00" + purpose))
	prefix := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			return r
		}
		return '-'
	}, strings.ToLower(owner.Name+"-"+purpose))
	prefix = strings.Trim(prefix, "-")
	if limit := maxLength - len("-") - hashLength; len(prefix) > limit {
		prefix = strings.Trim(prefix[:limit], "-")
	}
	return prefix + "-" + hex.EncodeToString(sum[:])[:hashLength]
}

func resourceLabels(purpose, digest string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "ort-runner",
		"app.kubernetes.io/component": "batch-resource",
		resourcePurposeLabel:          purpose,
		resourceDigestLabel:           digest[:min(len(digest), 63)],
	}
}

func digestConfigData(data map[string][]byte) string {
	hash := sha256.New()
	for _, entry := range sortedConfigEntries(data) {
		hash.Write([]byte(entry.path))
		hash.Write([]byte{0})
		hash.Write(entry.data)
		hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func safeLabel(value string) string {
	value = strings.Trim(value, "-_.")
	if len(value) <= 63 {
		return value
	}
	sum := sha256.Sum256([]byte(value))
	return strings.Trim(value[:52], "-_.") + "-" + hex.EncodeToString(sum[:])[:10]
}
