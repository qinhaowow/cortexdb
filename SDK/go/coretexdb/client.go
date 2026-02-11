package coretexdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

type ClientOption func(*Client)

func WithAPIKey(apiKey string) ClientOption {
	return func(c *Client) {
		c.apiKey = apiKey
	}
}

func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.httpClient.Timeout = timeout
	}
}

func NewClient(host string, port int, opts ...ClientOption) *Client {
	baseURL := fmt.Sprintf("http://%s:%d", host, port)
	client := &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

	for _, opt := range opts {
		opt(client)
	}

	return client
}

func (c *Client) doRequest(ctx context.Context, method, endpoint string, body interface{}) (*http.Response, error) {
	var reqBody io.Reader
	if body != nil {
		jsonData, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewReader(jsonData)
	}

	fullURL := c.baseURL + endpoint
	req, err := http.NewRequestWithContext(ctx, method, fullURL, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	return c.httpClient.Do(req)
}

func (c *Client) parseResponse(resp *http.Response, v interface{}) error {
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &APIError{
			StatusCode: resp.StatusCode,
			Message:    string(body),
		}
	}

	if v != nil {
		if err := json.Unmarshal(body, v); err != nil {
			return fmt.Errorf("failed to unmarshal response: %w", err)
		}
	}

	return nil
}

func (c *Client) HealthCheck(ctx context.Context) (*HealthCheckResponse, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, "/api/health", nil)
	if err != nil {
		return nil, err
	}

	var result HealthCheckResponse
	if err := c.parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (c *Client) CreateCollection(ctx context.Context, req *CreateCollectionRequest) (*Collection, error) {
	resp, err := c.doRequest(ctx, http.MethodPost, "/api/collections", req)
	if err != nil {
		return nil, err
	}

	var result Collection
	if err := c.parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (c *Client) ListCollections(ctx context.Context) ([]string, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, "/api/collections", nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		Collections []string `json:"collections"`
	}
	if err := c.parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return result.Collections, nil
}

func (c *Client) GetCollection(ctx context.Context, name string) (*Collection, error) {
	endpoint := fmt.Sprintf("/api/collections/%s", url.PathEscape(name))
	resp, err := c.doRequest(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}

	var result Collection
	if err := c.parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (c *Client) DeleteCollection(ctx context.Context, name string) error {
	endpoint := fmt.Sprintf("/api/collections/%s", url.PathEscape(name))
	resp, err := c.doRequest(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return err
	}

	return c.parseResponse(resp, nil)
}

func (c *Client) InsertDocuments(ctx context.Context, collection string, req *InsertDocumentsRequest) (*InsertResponse, error) {
	endpoint := fmt.Sprintf("/api/collections/%s/documents", url.PathEscape(collection))
	resp, err := c.doRequest(ctx, http.MethodPost, endpoint, req)
	if err != nil {
		return nil, err
	}

	var result InsertResponse
	if err := c.parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (c *Client) GetDocument(ctx context.Context, collection, id string) (*Document, error) {
	endpoint := fmt.Sprintf("/api/collections/%s/documents/%s", url.PathEscape(collection), url.PathEscape(id))
	resp, err := c.doRequest(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}

	var result Document
	if err := c.parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (c *Client) DeleteDocument(ctx context.Context, collection, id string) error {
	endpoint := fmt.Sprintf("/api/collections/%s/documents/%s", url.PathEscape(collection), url.PathEscape(id))
	resp, err := c.doRequest(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return err
	}

	return c.parseResponse(resp, nil)
}

func (c *Client) VectorSearch(ctx context.Context, collection string, req *VectorSearchRequest) (*SearchResponse, error) {
	endpoint := fmt.Sprintf("/api/collections/%s/search", url.PathEscape(collection))
	resp, err := c.doRequest(ctx, http.MethodPost, endpoint, req)
	if err != nil {
		return nil, err
	}

	var result SearchResponse
	if err := c.parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (c *Client) CreateIndex(ctx context.Context, collection string, req *CreateIndexRequest) error {
	endpoint := fmt.Sprintf("/api/collections/%s/indexes", url.PathEscape(collection))
	resp, err := c.doRequest(ctx, http.MethodPost, endpoint, req)
	if err != nil {
		return err
	}

	return c.parseResponse(resp, nil)
}

func (c *Client) ListIndexes(ctx context.Context, collection string) ([]IndexInfo, error) {
	endpoint := fmt.Sprintf("/api/collections/%s/indexes", url.PathEscape(collection))
	resp, err := c.doRequest(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		Indexes []IndexInfo `json:"indexes"`
	}
	if err := c.parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return result.Indexes, nil
}

func (c *Client) SystemStats(ctx context.Context) (*SystemStats, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, "/api/system/stats", nil)
	if err != nil {
		return nil, err
	}

	var result SystemStats
	if err := c.parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return &result, nil
}
