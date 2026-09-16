package register

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type Client struct {
	HTTP   *http.Client
	APIKey string
}

func (c Client) Repositories(ctx context.Context, endpoint string) ([]Repository, error) {
	u, err := ParseURL(endpoint)
	if err != nil {
		return nil, err
	}
	repositories := []Repository{}
	seen := make(map[string]string)
	pages := -1
	for page := 1; ; page++ {
		query := u.Query()
		query.Set("page", strconv.Itoa(page))
		query.Set("perPage", "100")
		u.RawQuery = query.Encode()
		response, err := c.request(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(io.LimitReader(response.Body, (16<<20)+1))
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("list repositories: HTTP %s", response.Status)
		}
		if readErr != nil {
			return nil, readErr
		}
		if len(data) > 16<<20 {
			return nil, fmt.Errorf("repository page exceeds 16 MiB")
		}
		total, err := strconv.Atoi(response.Header.Get("Total-Pages"))
		if err != nil || total < 0 {
			return nil, fmt.Errorf("missing or invalid Total-Pages header")
		}
		if pages == -1 {
			pages = total
		} else if pages != total {
			return nil, fmt.Errorf("repository page count changed during listing; run again")
		}
		var items []Repository
		if err := json.Unmarshal(data, &items); err != nil || items == nil {
			return nil, fmt.Errorf("repository page must be a JSON array")
		}
		if len(items) == 0 && pages > 0 || len(items) > 0 && pages == 0 {
			return nil, fmt.Errorf("repository page does not match pagination headers")
		}
		previousCount := len(repositories)
		for _, item := range items {
			if item.ID == "" {
				return nil, fmt.Errorf("repository ID missing")
			}
			if _, err := ParseURL(item.URL); err != nil {
				return nil, fmt.Errorf("repository %s: %w", item.ID, err)
			}
			if existing, ok := seen[item.ID]; ok {
				if existing != item.URL {
					return nil, fmt.Errorf("repository %s changed URL during listing", item.ID)
				}
				continue
			}
			seen[item.ID] = item.URL
			repositories = append(repositories, item)
		}
		if len(items) > 0 && len(repositories) == previousCount {
			return nil, fmt.Errorf("repository page %d contains only repeated IDs", page)
		}
		if page >= pages {
			return repositories, nil
		}
	}
}

func (c Client) Post(ctx context.Context, endpoint string, data []byte) error {
	response, err := c.request(ctx, http.MethodPost, endpoint, data)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("post result: HTTP %s", response.Status)
	}
	return nil
}

func (c Client) request(ctx context.Context, method, endpoint string, data []byte) (*http.Response, error) {
	if _, err := ParseURL(endpoint); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.APIKey != "" {
		req.Header.Set("X-Api-Key", c.APIKey)
	}
	client := http.Client{Timeout: 30 * time.Second}
	if c.HTTP != nil {
		client = *c.HTTP
	}
	// A redirect must not leak headers or turn a result POST into a GET.
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return client.Do(req)
}

// ParseURL validates registry endpoints and public repository URLs before they are used.
func ParseURL(value string) (*url.URL, error) {
	u, err := url.Parse(value)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Fragment != "" {
		return nil, fmt.Errorf("expected an HTTP(S) URL without embedded credentials or fragment")
	}
	return u, nil
}
