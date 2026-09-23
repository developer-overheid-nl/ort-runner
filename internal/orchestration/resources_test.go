package orchestration

import (
	"strings"
	"testing"

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
			foundManifest = foundManifest || len(configMap.BinaryData[manifestChunkKey]) > 0
			if len(configMap.Data) != 0 || len(configMap.BinaryData) != 1 {
				t.Fatalf("manifest config map contains unexpected data: %+v", configMap)
			}
		}
	}
	if !foundRules || !foundGradle || !foundManifest {
		t.Fatalf("rules=%t gradle=%t manifest=%t", foundRules, foundGradle, foundManifest)
	}
}

func TestBuildIndexedJobUsesBoundedIsolatedWorkers(t *testing.T) {
	owner := testOwner()
	resources := testWorkerResources(t)
	job, err := BuildIndexedJob(owner, "oss", "scan-batch", resources)
	if err != nil {
		t.Fatal(err)
	}
	if *job.Spec.Completions != int32(resources.RepositoryCount) || *job.Spec.Parallelism != 10 || job.Spec.CompletionMode == nil || *job.Spec.CompletionMode != batchv1.IndexedCompletion {
		t.Fatalf("incorrect indexed job: %+v", job.Spec)
	}
	if job.Spec.BackoffLimitPerIndex == nil || *job.Spec.BackoffLimitPerIndex != 1 || job.Spec.MaxFailedIndexes != nil {
		t.Fatalf("incorrect retry settings: backoff=%v maxFailed=%v", job.Spec.BackoffLimitPerIndex, job.Spec.MaxFailedIndexes)
	}
	if job.Spec.TTLSecondsAfterFinished == nil || *job.Spec.TTLSecondsAfterFinished != 86400 {
		t.Fatalf("completed worker Pods are not bounded by a TTL: %v", job.Spec.TTLSecondsAfterFinished)
	}
	pod := job.Spec.Template.Spec
	if pod.RestartPolicy != corev1.RestartPolicyNever || pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
		t.Fatal("worker pod can restart or has an API token")
	}
	if pod.SecurityContext == nil || pod.SecurityContext.RunAsNonRoot == nil || !*pod.SecurityContext.RunAsNonRoot || pod.SecurityContext.SeccompProfile == nil || pod.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Fatalf("pod security context=%+v", pod.SecurityContext)
	}
	if pod.SecurityContext.FSGroup == nil || *pod.SecurityContext.FSGroup != 1000 {
		t.Fatalf("worker output volume is not writable by the ORT image user: fsGroup=%v", pod.SecurityContext.FSGroup)
	}
	container := pod.Containers[0]
	if len(container.Command) != 2 || container.Command[0] != "ort-runner" || container.Command[1] != "worker" {
		t.Fatalf("command=%v", container.Command)
	}
	if container.SecurityContext == nil || container.SecurityContext.AllowPrivilegeEscalation == nil || *container.SecurityContext.AllowPrivilegeEscalation || len(container.SecurityContext.Capabilities.Drop) != 1 || container.SecurityContext.Capabilities.Drop[0] != "ALL" {
		t.Fatalf("container security context=%+v", container.SecurityContext)
	}
	if container.Resources.Requests.Memory().String() != "4Gi" || container.Resources.Limits.Memory().String() != "8Gi" || container.Resources.Requests.Cpu().String() != "200m" || container.Resources.Limits.Cpu().String() != "2" {
		t.Fatalf("resources=%+v", container.Resources)
	}
	environment := map[string]corev1.EnvVar{}
	for _, variable := range container.Env {
		environment[variable.Name] = variable
	}
	if environment["ORT_OPTS"].Value != "-Xmx4g" || environment["ORT_RESULTS_URL"].Value != "https://example.test/results" || environment["AUTH_CLIENT_SECRET"].ValueFrom == nil {
		t.Fatalf("worker environment=%+v", environment)
	}
	if _, ok := environment["ORT_REGISTER_API_KEY"]; ok {
		t.Fatal("register API key leaked to worker")
	}
	index := environment["JOB_COMPLETION_INDEX"]
	if index.ValueFrom == nil || index.ValueFrom.FieldRef == nil || index.ValueFrom.FieldRef.FieldPath != "metadata.annotations['batch.kubernetes.io/job-completion-index']" {
		t.Fatalf("completion index=%+v", index)
	}
	for _, mount := range container.VolumeMounts {
		if (mount.Name == rulesVolumeName || mount.Name == manifestVolumeName) && !mount.ReadOnly {
			t.Fatalf("snapshot mount is writable: %+v", mount)
		}
	}
}

func TestBuildIndexedJobOmitsResultCredentialsWithoutEndpoint(t *testing.T) {
	resources := testWorkerResources(t)
	resources.Environment[0].Value = ""
	job, err := BuildIndexedJob(testOwner(), "oss", "scan-batch", resources)
	if err != nil {
		t.Fatal(err)
	}
	for _, variable := range job.Spec.Template.Spec.Containers[0].Env {
		if variable.Name == "AUTH_CLIENT_SECRET" || variable.Name == "AUTH_CLIENT_ID" {
			t.Fatalf("result credential copied without endpoint: %s", variable.Name)
		}
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
		{name: "parallelism", namespace: "oss", jobName: "job", owner: testOwner(), mutate: func(r *WorkerResources) { r.Parallelism = 101 }},
		{name: "retry", namespace: "oss", jobName: "job", owner: testOwner(), mutate: func(r *WorkerResources) { r.RetryLimit = 2 }},
		{name: "rules", namespace: "oss", jobName: "job", owner: testOwner(), mutate: func(r *WorkerResources) { delete(r.ConfigData, "evaluator.rules.kts") }},
		{name: "quantity", namespace: "oss", jobName: "job", owner: testOwner(), mutate: func(r *WorkerResources) { r.MemoryLimit = "many" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resources := valid.deepCopy()
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

func testWorkerResources(t *testing.T) WorkerResources {
	t.Helper()
	chunks, err := EncodeManifest(Manifest{SchemaVersion: 1, BatchID: "batch-1", Repositories: []register.Repository{
		{ID: "a", URL: "https://example.test/a.git"},
		{ID: "b", URL: "https://example.test/b.git"},
	}}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return WorkerResources{
		Image:           "ghcr.io/example/ort-runner@sha256:1234",
		BatchID:         "batch-1",
		RepositoryCount: 2,
		Parallelism:     10,
		RetryLimit:      1,
		CPURequest:      "200m",
		CPULimit:        "2000m",
		MemoryRequest:   "4Gi",
		MemoryLimit:     "8Gi",
		ConfigData:      map[string][]byte{"evaluator.rules.kts": []byte("licenseRule {}")},
		ManifestChunks:  chunks,
		Environment: []corev1.EnvVar{
			{Name: "ORT_RESULTS_URL", Value: "https://example.test/results"},
			{Name: "AUTH_CLIENT_ID", Value: "runner"},
			{Name: "AUTH_CLIENT_SECRET", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "auth"}, Key: "secret"}}},
			{Name: "ORT_REGISTER_API_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "register"}, Key: "key"}}},
		},
	}
}
