package coretexdb

import (
	"context"
	"sync"
)

type AsyncClient struct {
	client  *Client
	workers int
}

func NewAsyncClient(host string, port int, workers int, opts ...ClientOption) *AsyncClient {
	client := NewClient(host, port, opts...)
	if workers <= 0 {
		workers = 10
	}
	return &AsyncClient{
		client:  client,
		workers: workers,
	}
}

func (c *AsyncClient) HealthCheck(ctx context.Context) <-chan *Result[*HealthCheckResponse] {
	return c.executeAsync(ctx, func() (*HealthCheckResponse, error) {
		return c.client.HealthCheck(ctx)
	})
}

func (c *AsyncClient) CreateCollection(ctx context.Context, req *CreateCollectionRequest) <-chan *Result[*Collection] {
	return c.executeAsync(ctx, func() (*Collection, error) {
		return c.client.CreateCollection(ctx, req)
	})
}

func (c *AsyncClient) ListCollections(ctx context.Context) <-chan *Result[[]string] {
	return c.executeAsync(ctx, func() ([]string, error) {
		return c.client.ListCollections(ctx)
	})
}

func (c *AsyncClient) GetCollection(ctx context.Context, name string) <-chan *Result[*Collection] {
	return c.executeAsync(ctx, func() (*Collection, error) {
		return c.client.GetCollection(ctx, name)
	})
}

func (c *AsyncClient) DeleteCollection(ctx context.Context, name string) <-chan *Result[struct{}] {
	return c.executeAsync(ctx, func() (struct{}, error) {
		return struct{}{}, c.client.DeleteCollection(ctx, name)
	})
}

func (c *AsyncClient) InsertDocuments(ctx context.Context, collection string, req *InsertDocumentsRequest) <-chan *Result[*InsertResponse] {
	return c.executeAsync(ctx, func() (*InsertResponse, error) {
		return c.client.InsertDocuments(ctx, collection, req)
	})
}

func (c *AsyncClient) GetDocument(ctx context.Context, collection, id string) <-chan *Result[*Document] {
	return c.executeAsync(ctx, func() (*Document, error) {
		return c.client.GetDocument(ctx, collection, id)
	})
}

func (c *AsyncClient) DeleteDocument(ctx context.Context, collection, id string) <-chan *Result[struct{}] {
	return c.executeAsync(ctx, func() (struct{}, error) {
		return struct{}{}, c.client.DeleteDocument(ctx, collection, id)
	})
}

func (c *AsyncClient) VectorSearch(ctx context.Context, collection string, req *VectorSearchRequest) <-chan *Result[*SearchResponse] {
	return c.executeAsync(ctx, func() (*SearchResponse, error) {
		return c.client.VectorSearch(ctx, collection, req)
	})
}

func (c *AsyncClient) CreateIndex(ctx context.Context, collection string, req *CreateIndexRequest) <-chan *Result[struct{}] {
	return c.executeAsync(ctx, func() (struct{}, error) {
		return struct{}{}, c.client.CreateIndex(ctx, collection, req)
	})
}

func (c *AsyncClient) ListIndexes(ctx context.Context, collection string) <-chan *Result[[]IndexInfo] {
	return c.executeAsync(ctx, func() ([]IndexInfo, error) {
		return c.client.ListIndexes(ctx, collection)
	})
}

func (c *AsyncClient) SystemStats(ctx context.Context) <-chan *Result[*SystemStats] {
	return c.executeAsync(ctx, func() (*SystemStats, error) {
		return c.client.SystemStats(ctx)
	})
}

func (c *AsyncClient) BatchVectorSearch(ctx context.Context, collection string, requests []*VectorSearchRequest) []<-chan *Result[*SearchResponse] {
	channels := make([]<-chan *Result[*SearchResponse], len(requests))
	for i, req := range requests {
		channels[i] = c.VectorSearch(ctx, collection, req)
	}
	return channels
}

type Result[T any] struct {
	Value T
	Error error
}

func (c *AsyncClient) executeAsync[T any](ctx context.Context, fn func() (T, error)) <-chan *Result[T] {
	resultCh := make(chan *Result[T], 1)

	go func() {
		defer close(resultCh)

		select {
		case <-ctx.Done():
			resultCh <- &Result[T]{Error: ctx.Err()}
		default:
			value, err := fn()
			resultCh <- &Result[T]{Value: value, Error: err}
		}
	}()

	return resultCh
}
