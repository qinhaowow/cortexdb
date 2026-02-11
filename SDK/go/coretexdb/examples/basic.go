package main

import (
	"context"
	"fmt"
	"log"

	"github.com/coretexdb/coretexdb-go/client"
)

func main() {
	ctx := context.Background()

	c := coretexdb.NewClient("localhost", 8080)

	health, err := c.HealthCheck(ctx)
	if err != nil {
		log.Fatalf("Health check failed: %v", err)
	}
	fmt.Printf("Health check: %s\n", health.Status)

	collections, err := c.ListCollections(ctx)
	if err != nil {
		log.Fatalf("List collections failed: %v", err)
	}
	fmt.Printf("Collections: %v\n", collections)

	collection, err := c.CreateCollection(ctx, &coretexdb.CreateCollectionRequest{
		Name:        "test_collection",
		Description: "Test collection created via Go SDK",
	})
	if err != nil {
		log.Fatalf("Create collection failed: %v", err)
	}
	fmt.Printf("Created collection: %s\n", collection.Name)

	searchResult, err := c.VectorSearch(ctx, "test_collection", &coretexdb.VectorSearchRequest{
		Query: []float32{1.0, 2.0, 3.0},
		Limit: 10,
	})
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("Search results: %d\n", len(searchResult.Results))

	err = c.DeleteCollection(ctx, "test_collection")
	if err != nil {
		log.Fatalf("Delete collection failed: %v", err)
	}
	fmt.Println("Deleted test_collection")
}
