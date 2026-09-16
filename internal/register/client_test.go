package register

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRepositoriesFetchesAllPagesAndPreservesFilters(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != "GET" || r.URL.Path != "/oss-register/v1/repositories" || r.URL.Query().Get("archived") != "false" || r.URL.Query().Get("perPage") != "100" {
			t.Errorf("unexpected list request: %s %s", r.Method, r.URL)
		}
		if r.Header.Get("X-Api-Key") != "test-read-key" || r.Header.Get("Authorization") != "" {
			t.Errorf("incorrect authentication headers")
		}
		w.Header().Set("Total-Pages", "2")
		switch r.URL.Query().Get("page") {
		case "1":
			fmt.Fprint(w, `[{"id":"repo-1","url":"https://github.com/example/one","archived":false,"name":"One"}]`)
		case "2":
			// An overlapping page must not cause a repository to be scanned twice.
			fmt.Fprint(w, `[{"id":"repo-1","url":"https://github.com/example/one"},{"id":"repo-2","url":"https://gitlab.com/example/two"}]`)
		default:
			t.Errorf("unexpected page: %s", r.URL.Query().Get("page"))
		}
	}))
	defer server.Close()
	client := Client{HTTP: server.Client(), APIKey: "test-read-key"}
	repos, err := client.Repositories(context.Background(), server.URL+"/oss-register/v1/repositories?archived=false")
	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 || len(repos) != 2 || repos[0].ID != "repo-1" || repos[1].URL != "https://gitlab.com/example/two" {
		t.Fatalf("incomplete repository list: %+v, requests=%d", repos, requests)
	}
}

func TestRepositoriesRejectsBrokenResponses(t *testing.T) {
	for _, tc := range []struct {
		name, pages, body string
		status            int
	}{
		{"unauthorized", "1", `[]`, 401},
		{"not an array", "1", `{"items":[]}`, 200},
		{"null", "1", `null`, 200},
		{"missing pagination", "", `[]`, 200},
		{"invalid pagination", "invalid", `[]`, 200},
		{"empty intermediate page", "2", `[]`, 200},
		{"empty page with nonzero total", "1", `[]`, 200},
		{"missing identifier", "1", `[{"url":"https://github.com/example/one"}]`, 200},
		{"local path from register", "1", `[{"id":"one","url":"file:///private/source"}]`, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Total-Pages", tc.pages)
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			if _, err := (Client{HTTP: server.Client()}).Repositories(context.Background(), server.URL); err == nil {
				t.Fatal("invalid response accepted")
			}
		})
	}
}

func TestEmptyRegisterIsValid(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Total-Pages", "0")
		fmt.Fprint(w, `[]`)
	}))
	defer server.Close()
	repos, err := (Client{HTTP: server.Client()}).Repositories(context.Background(), server.URL)
	if err != nil || len(repos) != 0 {
		t.Fatalf("empty register: %+v, %v", repos, err)
	}
}

func TestPostSendsJSONAndReportsRejectedDelivery(t *testing.T) {
	for _, status := range []int{201, 204, 400, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			body := `{"repositoryId":"repo-1","scan":{"status":"completed"}}`
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				data, _ := io.ReadAll(r.Body)
				if r.Method != "POST" || string(data) != body || r.Header.Get("Content-Type") != "application/json" || r.Header.Get("X-Api-Key") != "" {
					t.Errorf("incorrect result delivery")
				}
				w.WriteHeader(status)
			}))
			defer server.Close()
			err := (Client{HTTP: server.Client()}).Post(context.Background(), server.URL, []byte(body))
			if (err != nil) != (status >= 400) {
				t.Fatalf("status %d: error=%v", status, err)
			}
		})
	}
}

func TestRedirectDoesNotChangePOSTToGET(t *testing.T) {
	followed := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { followed = true }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer server.Close()
	err := (Client{HTTP: server.Client()}).Post(context.Background(), server.URL, []byte(`{}`))
	if err == nil || followed {
		t.Fatalf("redirect followed or treated as success: %v, %t", err, followed)
	}
}

func TestHTTPRequestsRespectCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := Client{HTTP: &http.Client{Timeout: time.Second}}
	if _, err := client.Repositories(ctx, "http://127.0.0.1:1"); err == nil || !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("cancellation not propagated: %v", err)
	}
}
