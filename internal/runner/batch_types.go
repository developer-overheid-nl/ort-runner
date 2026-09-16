package runner

import (
	"net/http"
	"time"
)

type BatchConfig struct {
	RepositoriesURL    string
	ResultsURL         string
	RegisterAPIKey     string
	RegisterHTTPClient *http.Client
	ResultsHTTPClient  *http.Client
	HTTPTimeout        time.Duration
	Runner             Config
}

type Submission struct {
	SchemaVersion int    `json:"schemaVersion"`
	RepositoryID  string `json:"repositoryId"`
	Scan          Report `json:"scan"`
}

type BatchItem struct {
	RepositoryID  string `json:"repositoryId"`
	RepositoryURL string `json:"repositoryUrl"`
	Directory     string `json:"directory"`
	ScanStatus    string `json:"scanStatus"`
	Delivery      string `json:"delivery"`
	Error         string `json:"error,omitempty"`
}

type BatchReport struct {
	Status     string      `json:"status"`
	Error      string      `json:"error,omitempty"`
	OutputDir  string      `json:"outputDirectory"`
	StartedAt  time.Time   `json:"startedAt"`
	FinishedAt *time.Time  `json:"finishedAt,omitempty"`
	Total      int         `json:"total"`
	Items      []BatchItem `json:"items"`
}
