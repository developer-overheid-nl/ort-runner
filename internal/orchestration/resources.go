package orchestration

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"sort"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	resourcePurposeLabel = "ort-runner.developer.overheid.nl/resource-purpose"
	resourceDigestLabel  = "ort-runner.developer.overheid.nl/digest"
	manifestChunkKey     = "chunk.gz"
	rulesVolumeName      = "rules"
	manifestVolumeName   = "manifest"
	gradleVolumeName     = "gradle"
	outputVolumeName     = "output"
	gradleProperties     = "org.gradle.jvmargs=-Xmx2g -XX:MaxMetaspaceSize=512m\norg.gradle.daemon=false\n"
)

type WorkerResources struct {
	Image           string
	BatchID         string
	RepositoryCount int
	Parallelism     int32
	RetryLimit      int32
	CPURequest      string
	CPULimit        string
	MemoryRequest   string
	MemoryLimit     string
	ConfigData      map[string][]byte
	ManifestChunks  []Chunk
	Environment     []corev1.EnvVar
}

func (resources WorkerResources) deepCopy() WorkerResources {
	copy := resources
	copy.ConfigData = make(map[string][]byte, len(resources.ConfigData))
	for key, value := range resources.ConfigData {
		copy.ConfigData[key] = append([]byte(nil), value...)
	}
	copy.ManifestChunks = append([]Chunk(nil), resources.ManifestChunks...)
	for index := range copy.ManifestChunks {
		copy.ManifestChunks[index].Data = append([]byte(nil), copy.ManifestChunks[index].Data...)
	}
	copy.Environment = make([]corev1.EnvVar, len(resources.Environment))
	for index := range resources.Environment {
		resources.Environment[index].DeepCopyInto(&copy.Environment[index])
	}
	return copy
}

func BuildConfigMaps(owner metav1.OwnerReference, namespace string, resources WorkerResources) ([]*corev1.ConfigMap, error) {
	if err := validateResources(owner, namespace, resources); err != nil {
		return nil, err
	}
	immutable := true
	ownerReferences := []metav1.OwnerReference{owner}

	rulesData := make(map[string]string, len(resources.ConfigData)+1)
	for index, entry := range sortedConfigEntries(resources.ConfigData) {
		rulesData[configMapKey(index)] = string(entry.data)
	}
	rulesData["gradle.properties"] = gradleProperties
	rulesDigest := digestConfigData(resources.ConfigData)
	maps := []*corev1.ConfigMap{{
		ObjectMeta: metav1.ObjectMeta{
			Name:            resourceName(owner, "rules"),
			Namespace:       namespace,
			OwnerReferences: ownerReferences,
			Labels:          resourceLabels("rules", rulesDigest),
		},
		Immutable: &immutable,
		Data:      rulesData,
	}}
	for _, chunk := range resources.ManifestChunks {
		purpose := fmt.Sprintf("manifest-%04d", chunk.Index)
		maps = append(maps, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:            resourceName(owner, purpose),
				Namespace:       namespace,
				OwnerReferences: ownerReferences,
				Labels:          resourceLabels("manifest", chunk.Digest),
				Annotations: map[string]string{
					"ort-runner.developer.overheid.nl/chunk-index": fmt.Sprint(chunk.Index),
					"ort-runner.developer.overheid.nl/chunk-count": fmt.Sprint(chunk.Count),
				},
			},
			Immutable:  &immutable,
			BinaryData: map[string][]byte{manifestChunkKey: append([]byte(nil), chunk.Data...)},
		})
	}
	return maps, nil
}

func BuildIndexedJob(owner metav1.OwnerReference, namespace, name string, resources WorkerResources) (*batchv1.Job, error) {
	if name == "" {
		return nil, fmt.Errorf("job name is required")
	}
	if err := validateResources(owner, namespace, resources); err != nil {
		return nil, err
	}
	quantities, err := parseWorkerQuantities(resources)
	if err != nil {
		return nil, err
	}

	completionMode := batchv1.IndexedCompletion
	completions := int32(resources.RepositoryCount)
	parallelism := resources.Parallelism
	retryLimit := resources.RetryLimit
	ttlSecondsAfterFinished := int32(24 * 60 * 60)
	automount := false
	runAsNonRoot := true
	fsGroup := int64(1000)
	allowPrivilegeEscalation := false

	rulesName := resourceName(owner, "rules")
	rulesItems := make([]corev1.KeyToPath, 0, len(resources.ConfigData))
	for index, entry := range sortedConfigEntries(resources.ConfigData) {
		rulesItems = append(rulesItems, corev1.KeyToPath{Key: configMapKey(index), Path: entry.path})
	}
	manifestSources := make([]corev1.VolumeProjection, 0, len(resources.ManifestChunks))
	for _, chunk := range resources.ManifestChunks {
		manifestSources = append(manifestSources, corev1.VolumeProjection{ConfigMap: &corev1.ConfigMapProjection{
			LocalObjectReference: corev1.LocalObjectReference{Name: resourceName(owner, fmt.Sprintf("manifest-%04d", chunk.Index))},
			Items:                []corev1.KeyToPath{{Key: manifestChunkKey, Path: fmt.Sprintf("chunk-%04d-%s.gz", chunk.Index, chunk.Digest)}},
		}})
	}

	labels := map[string]string{
		"app.kubernetes.io/name":                 "ort-runner",
		"app.kubernetes.io/component":            "worker",
		"ort-runner.developer.overheid.nl/batch": safeLabel(resources.BatchID),
	}
	container := corev1.Container{
		Name:    "ort-runner",
		Image:   resources.Image,
		Command: []string{"ort-runner", "worker"},
		Env:     workerEnvironment(resources),
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: quantities.cpuRequest, corev1.ResourceMemory: quantities.memoryRequest},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: quantities.cpuLimit, corev1.ResourceMemory: quantities.memoryLimit},
		},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: &allowPrivilegeEscalation,
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: rulesVolumeName, MountPath: "/config", ReadOnly: true},
			{Name: manifestVolumeName, MountPath: "/manifest", ReadOnly: true},
			{Name: gradleVolumeName, MountPath: "/home/ort/.gradle/gradle.properties", SubPath: "gradle.properties", ReadOnly: true},
			{Name: outputVolumeName, MountPath: "/output"},
		},
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, OwnerReferences: []metav1.OwnerReference{owner}, Labels: labels},
		Spec: batchv1.JobSpec{
			Completions:             &completions,
			Parallelism:             &parallelism,
			CompletionMode:          &completionMode,
			BackoffLimitPerIndex:    &retryLimit,
			TTLSecondsAfterFinished: &ttlSecondsAfterFinished,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: &automount,
					RestartPolicy:                corev1.RestartPolicyNever,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot:   &runAsNonRoot,
						FSGroup:        &fsGroup,
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{container},
					Volumes: []corev1.Volume{
						{Name: rulesVolumeName, VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: rulesName}, Items: rulesItems}}}}}},
						{Name: manifestVolumeName, VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: manifestSources}}},
						{Name: gradleVolumeName, VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: rulesName}, Items: []corev1.KeyToPath{{Key: "gradle.properties", Path: "gradle.properties"}}}}}}}},
						{Name: outputVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					},
				},
			},
		},
	}, nil
}

type workerQuantities struct {
	cpuRequest, cpuLimit, memoryRequest, memoryLimit resource.Quantity
}

func parseWorkerQuantities(resources WorkerResources) (workerQuantities, error) {
	values := []struct {
		name  string
		value string
	}{
		{name: "CPU request", value: resources.CPURequest},
		{name: "CPU limit", value: resources.CPULimit},
		{name: "memory request", value: resources.MemoryRequest},
		{name: "memory limit", value: resources.MemoryLimit},
	}
	parsed := make([]resource.Quantity, len(values))
	for index := range values {
		quantity, err := resource.ParseQuantity(values[index].value)
		if err != nil || quantity.Sign() <= 0 {
			return workerQuantities{}, fmt.Errorf("invalid %s %q", values[index].name, values[index].value)
		}
		parsed[index] = quantity
	}
	return workerQuantities{cpuRequest: parsed[0], cpuLimit: parsed[1], memoryRequest: parsed[2], memoryLimit: parsed[3]}, nil
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
	if resources.RepositoryCount <= 0 {
		return fmt.Errorf("repository count must be positive")
	}
	if resources.Parallelism < 1 || resources.Parallelism > 100 {
		return fmt.Errorf("parallelism must be between 1 and 100")
	}
	if resources.RetryLimit != 1 {
		return fmt.Errorf("retry limit must equal 1")
	}
	if len(resources.ManifestChunks) == 0 {
		return fmt.Errorf("manifest chunks are required")
	}
	if data, ok := resources.ConfigData["evaluator.rules.kts"]; !ok || len(data) == 0 {
		return fmt.Errorf("evaluator.rules.kts is required")
	}
	for file := range resources.ConfigData {
		if file == "" || path.IsAbs(file) || path.Clean(file) != file || strings.HasPrefix(file, "../") || file == ".." {
			return fmt.Errorf("unsafe config path %q", file)
		}
	}
	_, err := parseWorkerQuantities(resources)
	return err
}

func workerEnvironment(resources WorkerResources) []corev1.EnvVar {
	environment := []corev1.EnvVar{
		{Name: "ORT_BATCH_ID", Value: resources.BatchID},
		{Name: "ORT_MANIFEST_DIR", Value: "/manifest"},
		{Name: "ORT_CONFIG_DIR", Value: "/config"},
		{Name: "ORT_OUTPUT_DIR", Value: "/output"},
		{Name: "ORT_OPTS", Value: "-Xmx4g"},
		{Name: "JOB_COMPLETION_INDEX", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.annotations['batch.kubernetes.io/job-completion-index']"}}},
	}
	allowed := map[string]struct{}{
		"ORT_RESULTS_URL": {}, "AUTH_TOKEN_URL": {}, "AUTH_CLIENT_ID": {}, "AUTH_CLIENT_SECRET": {}, "AUTH_SCOPES": {},
		"KEYCLOAK_BASE_URL": {}, "KEYCLOAK_REALM": {},
	}
	resultsEnabled := false
	for _, variable := range resources.Environment {
		if variable.Name == "ORT_RESULTS_URL" && (variable.Value != "" || variable.ValueFrom != nil) {
			resultsEnabled = true
		}
	}
	for _, variable := range resources.Environment {
		if _, ok := allowed[variable.Name]; !ok {
			continue
		}
		if variable.Name != "ORT_RESULTS_URL" && !resultsEnabled {
			continue
		}
		environment = append(environment, *variable.DeepCopy())
	}
	return environment
}

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

func resourceName(owner metav1.OwnerReference, purpose string) string {
	sum := sha256.Sum256([]byte(string(owner.UID) + "\x00" + purpose))
	suffix := hex.EncodeToString(sum[:])[:10]
	prefix := strings.Trim(strings.ToLower(owner.Name+"-"+purpose), "-")
	prefix = strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' {
			return r
		}
		return '-'
	}, prefix)
	prefix = strings.Trim(prefix, "-")
	if len(prefix) > 52 {
		prefix = strings.Trim(prefix[:52], "-")
	}
	return prefix + "-" + suffix
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
