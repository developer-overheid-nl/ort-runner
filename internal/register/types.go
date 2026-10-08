package register

type Repository struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

// Environment variables that grant access to the register. Scanned repository
// code must never see them.
const (
	APIKeyVariable     = "ORT_REGISTER_API_KEY"
	ResultsURLVariable = "ORT_RESULTS_URL"
)

// ResultCredentialVariables configure the OAuth client for result delivery.
var ResultCredentialVariables = []string{
	"AUTH_TOKEN_URL", "AUTH_CLIENT_ID", "AUTH_CLIENT_SECRET", "AUTH_SCOPES", "KEYCLOAK_BASE_URL", "KEYCLOAK_REALM",
}
