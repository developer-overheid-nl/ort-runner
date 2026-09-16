package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/developer-overheid-nl/ort-runner/internal/runner"
)

func TestConfigureBatchHTTPClientsAuthenticatesResultsOnly(t *testing.T) {
	tokenRequests := 0
	registerAuthorization := "not-called"
	resultsAuthorization := "not-called"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			tokenRequests++
			if err := r.ParseForm(); err != nil {
				t.Error(err)
			}
			if r.Form.Get("grant_type") != "client_credentials" || r.Form.Get("scope") != "repositories:write" {
				t.Errorf("token form = %v", r.Form)
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"access_token":"runner-token","token_type":"Bearer","expires_in":3600}`)
			return
		}
		if r.URL.Path == "/register" {
			registerAuthorization = r.Header.Get("Authorization")
		} else if r.URL.Path == "/results" {
			resultsAuthorization = r.Header.Get("Authorization")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	t.Setenv("AUTH_TOKEN_URL", server.URL+"/token")
	t.Setenv("AUTH_CLIENT_ID", "runner")
	t.Setenv("AUTH_CLIENT_SECRET", "secret")
	t.Setenv("AUTH_SCOPES", "repositories:write")

	config := runner.BatchConfig{HTTPTimeout: 7 * time.Second, ResultsURL: server.URL + "/results"}
	if err := configureBatchHTTPClients(context.Background(), &config); err != nil {
		t.Fatal(err)
	}
	response, err := config.RegisterHTTPClient.Get(server.URL + "/register")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/results", nil)
	response, err = config.ResultsHTTPClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if registerAuthorization != "" {
		t.Fatalf("register request token = %q", registerAuthorization)
	}
	if resultsAuthorization != "Bearer runner-token" || tokenRequests != 1 {
		t.Fatalf("results token = %q, token requests = %d", resultsAuthorization, tokenRequests)
	}
}

func TestConfigureBatchHTTPClientsKeepsAPIKeyGetOutOfOAuth(t *testing.T) {
	registerAuthorization := "not-called"
	resultsAuthorization := "not-called"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"access_token":"runner-token","token_type":"Bearer","expires_in":3600}`)
			return
		}
		if r.URL.Path == "/register" {
			registerAuthorization = r.Header.Get("Authorization")
		} else if r.URL.Path == "/results" {
			resultsAuthorization = r.Header.Get("Authorization")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	t.Setenv("AUTH_TOKEN_URL", server.URL+"/token")
	t.Setenv("AUTH_CLIENT_ID", "runner")
	t.Setenv("AUTH_CLIENT_SECRET", "secret")
	t.Setenv("AUTH_SCOPES", "")
	config := runner.BatchConfig{HTTPTimeout: time.Second, RegisterAPIKey: "read-key", ResultsURL: server.URL + "/results"}
	if err := configureBatchHTTPClients(context.Background(), &config); err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/register", nil)
	response, err := config.RegisterHTTPClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if registerAuthorization != "" {
		t.Fatal("OAuth token added to an API-key register request")
	}
	request, _ = http.NewRequest(http.MethodPost, server.URL+"/results", nil)
	response, err = config.ResultsHTTPClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if resultsAuthorization != "Bearer runner-token" {
		t.Fatalf("results request token = %q", resultsAuthorization)
	}
}

func TestConfigureBatchHTTPClientsRejectsPartialAuthEnvironment(t *testing.T) {
	t.Setenv("AUTH_TOKEN_URL", "https://auth.example.test/token")
	t.Setenv("AUTH_CLIENT_ID", "runner")
	t.Setenv("AUTH_CLIENT_SECRET", "")
	t.Setenv("AUTH_SCOPES", "")
	config := runner.BatchConfig{HTTPTimeout: time.Second, ResultsURL: "https://api.example.test/results"}
	if err := configureBatchHTTPClients(context.Background(), &config); err == nil {
		t.Fatal("partial client credentials accepted")
	}
}

func TestConfigureBatchHTTPClientsAllowsUnprotectedLocalEndpoints(t *testing.T) {
	for _, name := range []string{"AUTH_TOKEN_URL", "AUTH_CLIENT_ID", "AUTH_CLIENT_SECRET", "AUTH_SCOPES"} {
		t.Setenv(name, "")
	}
	config := runner.BatchConfig{HTTPTimeout: time.Second, ResultsURL: "http://127.0.0.1/results"}
	if err := configureBatchHTTPClients(context.Background(), &config); err != nil {
		t.Fatal(err)
	}
	if config.RegisterHTTPClient == nil || config.ResultsHTTPClient == nil {
		t.Fatal("plain HTTP clients not configured")
	}
}

func TestConfigureBatchHTTPClientsIgnoresResultAuthWithoutResultEndpoint(t *testing.T) {
	t.Setenv("AUTH_TOKEN_URL", "https://auth.example.test/token")
	t.Setenv("AUTH_CLIENT_ID", "")
	t.Setenv("AUTH_CLIENT_SECRET", "")
	t.Setenv("AUTH_SCOPES", "")
	config := runner.BatchConfig{HTTPTimeout: time.Second, RegisterAPIKey: "read-key"}
	if err := configureBatchHTTPClients(context.Background(), &config); err != nil {
		t.Fatalf("unused result authentication rejected: %v", err)
	}
}
