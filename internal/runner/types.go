package runner

import "time"

type Config struct {
	Repository    string
	Revision      string
	ConfigDir     string
	OutputDir     string
	ORTBinary     string
	ORTImage      string
	RunnerVersion string
	StageTimeout  time.Duration
}

type Stage struct {
	Name       string     `json:"name"`
	Status     string     `json:"status"`
	ExitCode   *int       `json:"exit_code,omitempty"`
	Log        string     `json:"log,omitempty"`
	Result     string     `json:"result,omitempty"`
	ORTVersion string     `json:"ort_version,omitempty"`
	Error      string     `json:"error,omitempty"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

type Report struct {
	SchemaVersion   int             `json:"schema_version"`
	Status          string          `json:"status"`
	Error           string          `json:"error,omitempty"`
	Repository      string          `json:"repository"`
	Revision        string          `json:"revision"`
	ConfigDigest    string          `json:"config_digest"`
	ORTImage        string          `json:"ort_image,omitempty"`
	RunnerVersion   string          `json:"runner_version"`
	OutputDir       string          `json:"output_directory"`
	StartedAt       time.Time       `json:"started_at"`
	FinishedAt      *time.Time      `json:"finished_at,omitempty"`
	PackageCount    *int            `json:"package_count,omitempty"`
	Stages          []Stage         `json:"stages"`
	Findings        []Finding       `json:"findings"`
	Vulnerabilities []Vulnerability `json:"vulnerabilities"`
}

type Finding struct {
	Rule     string `json:"rule" yaml:"rule"`
	Severity string `json:"severity" yaml:"severity"`
	Message  string `json:"message" yaml:"message"`
}

type Vulnerability struct {
	PackageID          string   `json:"package_id"`
	ID                 string   `json:"id"`
	Summary            string   `json:"summary"`
	Severity           string   `json:"severity,omitempty"`
	Score              *float64 `json:"score,omitempty"`
	ScoringSystem      string   `json:"scoring_system,omitempty"`
	FirstFixedVersions []string `json:"first_fixed_versions,omitempty"`
}
