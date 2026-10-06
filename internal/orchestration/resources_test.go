package orchestration

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/developer-overheid-nl/ort-runner/internal/register"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestBuildConfigMapsCreatesImmutableOwnedSnapshots(t *testing.T) {
	owner := testOwner()
	resources := testWorkerResources(t)
	maps, err := BuildConfigMaps(owner, "oss", resources)
	if err != nil {
		t.Fatal(err)
	}
	if len(maps) != 1+len(resources.ManifestChunks) {
		t.Fatalf("config maps=%d", len(maps))
	}
	names := map[string]bool{}
	foundRules, foundGradle, foundManifest := false, false, false
	for _, configMap := range maps {
		if configMap.Namespace != "oss" || configMap.Immutable == nil || !*configMap.Immutable || len(configMap.OwnerReferences) != 1 || configMap.OwnerReferences[0].UID != owner.UID {
			t.Fatalf("config map is mutable or unowned: %+v", configMap.ObjectMeta)
		}
		if len(configMap.Name) > 63 || names[configMap.Name] {
			t.Fatalf("invalid or duplicate deterministic name %q", configMap.Name)
		}
		names[configMap.Name] = true
		if configMap.Labels[resourcePurposeLabel] == "rules" {
			for _, value := range configMap.Data {
				foundRules = foundRules || strings.Contains(value, "licenseRule")
				foundGradle = foundGradle || strings.Contains(value, "org.gradle.jvmargs=-Xmx2g -XX:MaxMetaspaceSize=512m")
			}
		} else {
			foundManifest = foundManifest || len(configMap.Data[manifestChunkKey]) > 0
			if len(configMap.Data) != 1 || len(configMap.BinaryData) != 0 {
				t.Fatalf("manifest config map contains unexpected data: %+v", configMap)
			}
		}
	}
	if !foundRules || !foundGradle || !foundManifest {
		t.Fatalf("rules=%t gradle=%t manifest=%t", foundRules, foundGradle, foundManifest)
	}
}

func TestBuildIndexedJobUsesBoundedIsolatedWorkers(t *testing.T) {
	resources := testWorkerResources(t)
	job, err := BuildIndexedJob(testOwner(), "oss", "scan-batch", resources)
	if err != nil {
		t.Fatal(err)
	}
	spec := job.Spec
	if *spec.Completions != int32(resources.RepositoryCount) || *spec.Parallelism != 10 || *spec.CompletionMode != batchv1.IndexedCompletion {
		t.Fatalf("incorrect indexed job: %+v", spec)
	}
	if *spec.BackoffLimit != 10 || *spec.BackoffLimitPerIndex != 0 || spec.MaxFailedIndexes != nil {
		t.Fatalf("incorrect retry settings: backoff=%v maxFailed=%v", spec.BackoffLimitPerIndex, spec.MaxFailedIndexes)
	}
	if *spec.ActiveDeadlineSeconds != int64(46*time.Hour/time.Second) || *spec.TTLSecondsAfterFinished != 86400 {
		t.Fatalf("deadline=%v ttl=%v", *spec.ActiveDeadlineSeconds, *spec.TTLSecondsAfterFinished)
	}
	pod := spec.Template.Spec
	if pod.RestartPolicy != corev1.RestartPolicyNever || *pod.AutomountServiceAccountToken || *pod.EnableServiceLinks {
		t.Fatal("worker Pod can restart, has an API token or receives service links")
	}
	if !*pod.SecurityContext.RunAsNonRoot || *pod.SecurityContext.FSGroup != 1000 || pod.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Fatalf("pod security context=%+v", pod.SecurityContext)
	}
	if len(pod.InitContainers) != 1 || len(pod.Containers) != 1 {
		t.Fatalf("expected scan init container and delivery container: %d/%d", len(pod.InitContainers), len(pod.Containers))
	}

	scan := pod.InitContainers[0]
	if !slices.Equal(scan.Command, []string{"ort-runner", "worker"}) {
		t.Fatalf("scan command=%v", scan.Command)
	}
	limits := scan.Resources.Limits
	if limits.Memory().String() != "8Gi" || limits.Cpu().String() != "2" || limits.StorageEphemeral().String() != "30Gi" || scan.Resources.Requests.StorageEphemeral().String() != "10Gi" {
		t.Fatalf("scan resources=%+v", scan.Resources)
	}
	scanEnv := environment(scan)
	if scanEnv["ORT_OPTS"].Value != "-Xmx4g" || scanEnv["ORT_MANIFEST_DIR"].Value != "/manifest" || scanEnv["ORT_REPOSITORY_TIMEOUT"].Value != "10m0s" {
		t.Fatalf("scan environment=%+v", scanEnv)
	}
	for _, name := range append([]string{register.APIKeyVariable, register.ResultsURLVariable}, register.ResultCredentialVariables...) {
		if _, ok := scanEnv[name]; ok {
			t.Fatalf("scan container receives %s", name)
		}
	}
	for _, mount := range scan.VolumeMounts {
		if mount.Name != outputVolumeName && !mount.ReadOnly {
			t.Fatalf("snapshot mount is writable: %+v", mount)
		}
	}

	deliver := pod.Containers[0]
	deliverEnv := environment(deliver)
	if !slices.Equal(deliver.Command, []string{"ort-runner", "deliver"}) || deliverEnv["ORT_RESULTS_URL"].Value != "https://example.test/results" || deliverEnv["AUTH_CLIENT_SECRET"].ValueFrom == nil {
		t.Fatalf("delivery container=%+v", deliver)
	}
	if _, ok := deliverEnv[register.APIKeyVariable]; ok {
		t.Fatal("register API key passed to worker")
	}
	for _, container := range []corev1.Container{scan, deliver} {
		if *container.SecurityContext.AllowPrivilegeEscalation || !slices.Equal(container.SecurityContext.Capabilities.Drop, []corev1.Capability{"ALL"}) {
			t.Fatalf("%s security context=%+v", container.Name, container.SecurityContext)
		}
	}
}

func TestResultEnvironmentOmitsCredentialsWithoutEndpoint(t *testing.T) {
	controller := []corev1.EnvVar{
		{Name: "ORT_RESULTS_URL", Value: ""},
		{Name: "AUTH_CLIENT_SECRET", Value: "secret"},
		{Name: "ORT_REGISTER_API_KEY", Value: "key"},
	}
	got := ResultEnvironment(controller)
	if len(got) != 1 || got[0].Name != "ORT_RESULTS_URL" {
		t.Fatalf("result environment=%+v", got)
	}
	controller[0].Value = "https://example.test/results"
	if got := ResultEnvironment(controller); len(got) != 2 || got[1].Name != "AUTH_CLIENT_SECRET" {
		t.Fatalf("result environment=%+v", got)
	}
}

func TestBuildResourcesRejectsInvalidConfiguration(t *testing.T) {
	valid := testWorkerResources(t)
	for _, tc := range []struct {
		name      string
		namespace string
		jobName   string
		owner     metav1.OwnerReference
		mutate    func(*WorkerResources)
	}{
		{name: "namespace", namespace: "", jobName: "job", owner: testOwner()},
		{name: "job name", namespace: "oss", jobName: "", owner: testOwner()},
		{name: "owner UID", namespace: "oss", jobName: "job", owner: metav1.OwnerReference{Name: "parent"}},
		{name: "image", namespace: "oss", jobName: "job", owner: testOwner(), mutate: func(r *WorkerResources) { r.Image = "" }},
		{name: "repositories", namespace: "oss", jobName: "job", owner: testOwner(), mutate: func(r *WorkerResources) { r.RepositoryCount = 0 }},
		{name: "parallelism", namespace: "oss", jobName: "job", owner: testOwner(), mutate: func(r *WorkerResources) { r.Settings.Parallelism = 101 }},
		{name: "deadline", namespace: "oss", jobName: "job", owner: testOwner(), mutate: func(r *WorkerResources) { r.Settings.Deadline = 0 }},
		{name: "round deadline", namespace: "oss", jobName: "job", owner: testOwner(), mutate: func(r *WorkerResources) { r.RoundDeadline = 0 }},
		{name: "repository timeout", namespace: "oss", jobName: "job", owner: testOwner(), mutate: func(r *WorkerResources) { r.Settings.RepositoryTimeout = 0 }},
		{name: "rules", namespace: "oss", jobName: "job", owner: testOwner(), mutate: func(r *WorkerResources) { delete(r.ConfigData, "evaluator.rules.kts") }},
		{name: "quantity", namespace: "oss", jobName: "job", owner: testOwner(), mutate: func(r *WorkerResources) { r.Settings.MemoryLimit = "many" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resources := valid
			resources.ConfigData = maps.Clone(valid.ConfigData)
			if tc.mutate != nil {
				tc.mutate(&resources)
			}
			if _, err := BuildIndexedJob(tc.owner, tc.namespace, tc.jobName, resources); err == nil {
				t.Fatal("invalid resources accepted")
			}
		})
	}
}

func testOwner() metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{APIVersion: "batch/v1", Kind: "Job", Name: "controller-123", UID: types.UID("12345678-1234-1234-1234-123456789abc"), Controller: &controller}
}

func testSettings() WorkerSettings {
	return WorkerSettings{
		Parallelism: 10, Deadline: 46 * time.Hour, RepositoryTimeout: 10 * time.Minute,
		CPURequest: "200m", CPULimit: "2000m", MemoryRequest: "4Gi", MemoryLimit: "8Gi",
		EphemeralStorageRequest: "10Gi", EphemeralStorageLimit: "30Gi",
	}
}

func testWorkerResources(t *testing.T) WorkerResources {
	t.Helper()
	manifest, err := NewManifest("batch-1", []register.Repository{
		{ID: "a", URL: "https://example.test/a.git"},
		{ID: "b", URL: "https://example.test/b.git"},
	})
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := EncodeManifest(manifest, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return WorkerResources{
		Image:           "ghcr.io/example/ort-runner@sha256:1234",
		BatchID:         "batch-1",
		RepositoryCount: 2,
		ConfigData:      map[string][]byte{"evaluator.rules.kts": []byte("licenseRule {}")},
		ManifestChunks:  chunks,
		ResultEnvironment: ResultEnvironment([]corev1.EnvVar{
			{Name: "ORT_RESULTS_URL", Value: "https://example.test/results"},
			{Name: "AUTH_CLIENT_ID", Value: "runner"},
			{Name: "AUTH_CLIENT_SECRET", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "auth"}, Key: "secret"}}},
			{Name: "ORT_REGISTER_API_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "register"}, Key: "key"}}},
		}),
		Settings:      testSettings(),
		RoundDeadline: 46 * time.Hour,
	}
}

func environment(container corev1.Container) map[string]corev1.EnvVar {
	result := map[string]corev1.EnvVar{}
	for _, variable := range container.Env {
		result[variable.Name] = variable
	}
	return result
}

func TestWorkerJobNameLeavesRoomForPodHostnames(t *testing.T) {
	// Indexed Job Pods use "<job>-<index>" as hostname, which must be a DNS label.
	controller := true
	owner := metav1.OwnerReference{APIVersion: "batch/v1", Kind: "Job", Name: "don-oss-ort-runner-manual-spawn-muwnn2k9-fqbp7", UID: types.UID("12345678-1234-1234-1234-123456789abc"), Controller: &controller}
	hostname := fmt.Sprintf("%s-%d", workerJobName(owner, 0), math.MaxInt32)
	if len(hostname) > 63 {
		t.Fatalf("hostname %q has %d characters", hostname, len(hostname))
	}
	if workerJobName(owner, 0) == workerJobName(testOwner(), 0) {
		t.Fatal("worker Job names are not unique per controller")
	}
}
