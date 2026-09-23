package orchestration

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/developer-overheid-nl/ort-runner/internal/register"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestParseIndexes(t *testing.T) {
	got, err := ParseIndexes("0,2-4,7", 8)
	if err != nil || !slices.Equal(got, []int{0, 2, 3, 4, 7}) {
		t.Fatalf("indexes=%v err=%v", got, err)
	}
	empty, err := ParseIndexes("", 8)
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty indexes=%v err=%v", empty, err)
	}
	for _, invalid := range []string{"-1", "3-2", "1,,2", "8", "1-999999999", "1,1"} {
		if _, err := ParseIndexes(invalid, 8); err == nil {
			t.Fatalf("accepted %q", invalid)
		}
	}
}

func TestRunControllerSkipsEmptyRepositorySet(t *testing.T) {
	server := repositoryServer(t, nil)
	defer server.Close()
	client := fake.NewSimpleClientset(controllerPod())
	cfg := controllerConfig(t, server.URL)

	summary, err := RunController(context.Background(), client, cfg)
	if err != nil || summary.Total != 0 || summary.Completed != 0 || summary.Failed != 0 {
		t.Fatalf("summary=%+v err=%v", summary, err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" {
			t.Fatalf("empty register created %s", action.GetResource().Resource)
		}
	}
}

func TestRunControllerCreatesAndAdoptsIndexedBatch(t *testing.T) {
	repositories := []register.Repository{{ID: "b", URL: "https://example.test/b.git"}, {ID: "a", URL: "https://example.test/a.git"}}
	server := repositoryServer(t, repositories)
	defer server.Close()
	client := fake.NewSimpleClientset(controllerPod())
	completeNextJob(t, client, "0-1", "", batchv1.JobComplete)
	cfg := controllerConfig(t, server.URL)

	first, err := RunController(context.Background(), client, cfg)
	if err != nil || first.Total != 2 || first.Completed != 2 || first.Failed != 0 {
		t.Fatalf("first summary=%+v err=%v", first, err)
	}
	created := countCreates(client.Actions())
	if created < 3 {
		t.Fatalf("expected config maps and job, creates=%d actions=%v", created, client.Actions())
	}
	jobs, err := client.BatchV1().Jobs("oss").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 1 {
		t.Fatalf("jobs=%d err=%v", len(jobs.Items), err)
	}
	defaultMode := int32(0644)
	job := jobs.Items[0].DeepCopy()
	for index := range job.Spec.Template.Spec.Volumes {
		if job.Spec.Template.Spec.Volumes[index].Projected != nil {
			job.Spec.Template.Spec.Volumes[index].Projected.DefaultMode = &defaultMode
		}
	}
	for index := range job.Spec.Template.Spec.Containers[0].Env {
		field := job.Spec.Template.Spec.Containers[0].Env[index].ValueFrom
		if field != nil && field.FieldRef != nil {
			field.FieldRef.APIVersion = "v1"
		}
	}
	if _, err := client.BatchV1().Jobs("oss").Update(context.Background(), job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	second, err := RunController(context.Background(), client, cfg)
	if err != nil || second.Total != first.Total || second.Completed != first.Completed || second.Failed != first.Failed || !slices.Equal(second.FailedRepositoryIDs, first.FailedRepositoryIDs) {
		t.Fatalf("adopted summary=%+v err=%v", second, err)
	}
	if got := countCreates(client.Actions()); got != created {
		t.Fatalf("adoption created extra resources: before=%d after=%d", created, got)
	}
}

func TestRunControllerRejectsConflictingResources(t *testing.T) {
	repositories := []register.Repository{{ID: "a", URL: "https://example.test/a.git"}}
	server := repositoryServer(t, repositories)
	defer server.Close()
	cfg := controllerConfig(t, server.URL)
	base := fake.NewSimpleClientset(controllerPod())
	completeNextJob(t, base, "0", "", batchv1.JobComplete)
	if _, err := RunController(context.Background(), base, cfg); err != nil {
		t.Fatal(err)
	}
	maps, _ := base.CoreV1().ConfigMaps("oss").List(context.Background(), metav1.ListOptions{})
	jobs, _ := base.BatchV1().Jobs("oss").List(context.Background(), metav1.ListOptions{})

	for _, tc := range []struct {
		name   string
		mutate func([]*corev1.ConfigMap, *batchv1.Job)
	}{
		{name: "digest", mutate: func(maps []*corev1.ConfigMap, _ *batchv1.Job) { maps[0].Labels[resourceDigestLabel] = "wrong" }},
		{name: "owner", mutate: func(maps []*corev1.ConfigMap, _ *batchv1.Job) { maps[0].OwnerReferences[0].UID = "other" }},
		{name: "image", mutate: func(_ []*corev1.ConfigMap, job *batchv1.Job) { job.Spec.Template.Spec.Containers[0].Image = "other" }},
		{name: "completions", mutate: func(_ []*corev1.ConfigMap, job *batchv1.Job) { *job.Spec.Completions = 2 }},
		{name: "cleanup TTL", mutate: func(_ []*corev1.ConfigMap, job *batchv1.Job) { *job.Spec.TTLSecondsAfterFinished = 60 }},
		{name: "environment", mutate: func(_ []*corev1.ConfigMap, job *batchv1.Job) {
			job.Spec.Template.Spec.Containers[0].Env[0].Value = "other"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objects := []runtime.Object{controllerPod().DeepCopy()}
			copiedMaps := make([]*corev1.ConfigMap, len(maps.Items))
			for i := range maps.Items {
				copiedMaps[i] = maps.Items[i].DeepCopy()
				objects = append(objects, copiedMaps[i])
			}
			job := jobs.Items[0].DeepCopy()
			tc.mutate(copiedMaps, job)
			objects = append(objects, job)
			client := fake.NewSimpleClientset(objects...)
			if _, err := RunController(context.Background(), client, cfg); err == nil || !strings.Contains(err.Error(), "conflict") {
				t.Fatalf("conflict accepted: %v", err)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() == "update" || action.GetVerb() == "patch" {
					t.Fatalf("conflict mutated resource: %v", action)
				}
			}
		})
	}
}

func TestRunControllerReportsTerminalFailedIndex(t *testing.T) {
	repositories := []register.Repository{{ID: "a", URL: "https://example.test/a.git"}, {ID: "b", URL: "https://example.test/b.git"}}
	server := repositoryServer(t, repositories)
	defer server.Close()
	client := fake.NewSimpleClientset(controllerPod())
	completeNextJob(t, client, "0", "1", batchv1.JobFailed)
	reporter := &recordingFailureReporter{}
	cfg := controllerConfig(t, server.URL)
	cfg.ResultsURL = "https://example.test/results"
	cfg.FailureReporter = reporter

	summary, err := RunController(context.Background(), client, cfg)
	if err != nil || summary.Completed != 1 || summary.Failed != 1 || !slices.Equal(summary.FailedRepositoryIDs, []string{"b"}) {
		t.Fatalf("summary=%+v err=%v", summary, err)
	}
	if !slices.Equal(reporter.repositories, []string{"b"}) {
		t.Fatalf("reported=%v", reporter.repositories)
	}
}

func TestRunControllerCancellationStopsObservation(t *testing.T) {
	server := repositoryServer(t, []register.Repository{{ID: "a", URL: "https://example.test/a.git"}})
	defer server.Close()
	client := fake.NewSimpleClientset(controllerPod())
	ctx, cancel := context.WithCancel(context.Background())
	client.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
		cancel()
		return false, nil, nil
	})
	_, err := RunController(ctx, client, controllerConfig(t, server.URL))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation not returned: %v", err)
	}
}

func TestRunControllerRejectsInvalidControllerPod(t *testing.T) {
	server := repositoryServer(t, nil)
	defer server.Close()
	for _, tc := range []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{name: "no job owner", mutate: func(pod *corev1.Pod) { pod.OwnerReferences = nil }},
		{name: "runner container missing", mutate: func(pod *corev1.Pod) { pod.Spec.Containers[0].Name = "other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := controllerPod()
			tc.mutate(pod)
			if _, err := RunController(context.Background(), fake.NewSimpleClientset(pod), controllerConfig(t, server.URL)); err == nil {
				t.Fatal("invalid controller pod accepted")
			}
		})
	}
}

type recordingFailureReporter struct {
	mu           sync.Mutex
	repositories []string
}

func (r *recordingFailureReporter) ReportFailedIndex(_ context.Context, _ string, repository register.Repository) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.repositories = append(r.repositories, repository.ID)
	return nil
}

func controllerPod() *corev1.Pod {
	controller := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "controller-pod", Namespace: "oss", OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: "controller-job", UID: "controller-uid", Controller: &controller}}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "ort-runner", Image: "ghcr.io/example/ort-runner@sha256:1234",
			Env: []corev1.EnvVar{
				{Name: "ORT_RESULTS_URL", Value: "https://example.test/results"},
				{Name: "AUTH_CLIENT_ID", Value: "runner"},
				{Name: "AUTH_CLIENT_SECRET", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "auth"}, Key: "secret"}}},
				{Name: "ORT_REGISTER_API_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "register"}, Key: "key"}}},
			},
		}}},
	}
}

func controllerConfig(t *testing.T, repositoriesURL string) ControllerConfig {
	t.Helper()
	configDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(configDir, "evaluator.rules.kts"), []byte("licenseRule {}"), 0600); err != nil {
		t.Fatal(err)
	}
	return ControllerConfig{
		Namespace: "oss", PodName: "controller-pod", ContainerName: "ort-runner", BatchName: "batch-1",
		RepositoriesURL: repositoriesURL, ConfigDir: configDir, Parallelism: 10, RetryLimit: 1,
		WorkerResources: WorkerResources{CPURequest: "200m", CPULimit: "2000m", MemoryRequest: "4Gi", MemoryLimit: "8Gi"},
		RegisterClient:  register.Client{HTTP: http.DefaultClient},
	}
}

func repositoryServer(t *testing.T, repositories []register.Repository) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Total-Pages", "1")
		if repositories == nil {
			w.Header().Set("Total-Pages", "0")
			fmt.Fprint(w, `[]`)
			return
		}
		fmt.Fprint(w, `[`)
		for index, repository := range repositories {
			if index > 0 {
				fmt.Fprint(w, `,`)
			}
			fmt.Fprintf(w, `{"id":%q,"url":%q}`, repository.ID, repository.URL)
		}
		fmt.Fprint(w, `]`)
	}))
}

func completeNextJob(t *testing.T, client *fake.Clientset, completed, failed string, condition batchv1.JobConditionType) {
	t.Helper()
	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			jobs, _ := client.BatchV1().Jobs("oss").List(context.Background(), metav1.ListOptions{})
			if len(jobs.Items) == 1 {
				job := jobs.Items[0].DeepCopy()
				job.Status.CompletedIndexes = completed
				if failed != "" {
					job.Status.FailedIndexes = &failed
				}
				job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{Type: condition, Status: corev1.ConditionTrue})
				if _, err := client.BatchV1().Jobs("oss").UpdateStatus(context.Background(), job, metav1.UpdateOptions{}); err != nil {
					t.Errorf("complete job: %v", err)
				}
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Error("job was not created")
	}()
}

func countCreates(actions []ktesting.Action) int {
	count := 0
	for _, action := range actions {
		if action.GetVerb() == "create" {
			count++
		}
	}
	return count
}
