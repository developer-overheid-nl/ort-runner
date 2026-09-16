package main

import (
	"context"
	"net/http"
	"os"
	"strings"

	commonauth "github.com/developer-overheid-nl/don-register-common/auth"
	"github.com/developer-overheid-nl/ort-runner/internal/runner"
)

func configureBatchHTTPClients(ctx context.Context, config *runner.BatchConfig) error {
	base := &http.Client{Timeout: config.HTTPTimeout}
	config.RegisterHTTPClient = base
	config.ResultsHTTPClient = base
	if config.ResultsURL == "" {
		return nil
	}

	authConfig := commonauth.ClientCredentialsConfig{
		TokenURL:     os.Getenv("AUTH_TOKEN_URL"),
		ClientID:     os.Getenv("AUTH_CLIENT_ID"),
		ClientSecret: os.Getenv("AUTH_CLIENT_SECRET"),
		Scopes:       strings.Fields(os.Getenv("AUTH_SCOPES")),
	}
	if authConfig.TokenURL == "" && authConfig.ClientID == "" && authConfig.ClientSecret == "" {
		return nil
	}
	authenticated, err := commonauth.NewClientCredentialsHTTPClient(ctx, authConfig, base)
	if err != nil {
		return err
	}
	config.ResultsHTTPClient = authenticated
	return nil
}
