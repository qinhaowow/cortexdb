package coretexdb

import (
	"time"
)

type MetricType string

const (
	MetricCosine    MetricType = "cosine"
	MetricEuclidean MetricType = "euclidean"
	MetricDotProduct MetricType = "dotproduct"
)

type IndexType string

const (
	IndexTypeHNSW     IndexType = "hnsw"
	IndexTypeIVF      IndexType = "ivf"
	IndexTypeScalar   IndexType = "scalar"
)

type CollectionSchema struct {
	Fields []FieldSchema `json:"fields"`
}

type FieldSchema struct {
	Name     string      `json:"name"`
	DataType string      `json:"data_type"`
	Required bool        `json:"required"`
	Index    bool        `json:"index"`
	Options  interface{} `json:"options,omitempty"`
}

type Collection struct {
	Name        string                 `json:"name"`
	Schema      *CollectionSchema       `json:"schema,omitempty"`
	Description string                  `json:"description,omitempty"`
	Settings    map[string]interface{}  `json:"settings,omitempty"`
	Count       int                    `json:"count,omitempty"`
	CreatedAt   time.Time              `json:"created_at,omitempty"`
	UpdatedAt   time.Time              `json:"updated_at,omitempty"`
}

type CreateCollectionRequest struct {
	Name        string                 `json:"name"`
	Schema      *CollectionSchema      `json:"schema,omitempty"`
	Description string                 `json:"description,omitempty"`
	Settings    map[string]interface{} `json:"settings,omitempty"`
}

type Document struct {
	ID        string                 `json:"id"`
	Vector    []float32              `json:"vector,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	Score     float32                `json:"score,omitempty"`
	CreatedAt time.Time             `json:"created_at,omitempty"`
	UpdatedAt time.Time             `json:"updated_at,omitempty"`
}

type InsertDocumentsRequest struct {
	Documents []Document              `json:"documents"`
	Overwrite bool                    `json:"overwrite,omitempty"`
}

type InsertResponse struct {
	Inserted int       `json:"inserted"`
	Ids      []string  `json:"ids,omitempty"`
	TimeMs   float64   `json:"time_ms,omitempty"`
}

type VectorSearchRequest struct {
	Query              []float32                `json:"query"`
	Limit              int                      `json:"limit,omitempty"`
	Threshold          float32                  `json:"threshold,omitempty"`
	IncludeDocuments   bool                     `json:"include_documents,omitempty"`
	Filter             map[string]interface{}   `json:"filter,omitempty"`
	Params             map[string]interface{}   `json:"params,omitempty"`
}

type SearchResult struct {
	ID               string                 `json:"id"`
	Score            float32                `json:"score"`
	Distance         float32                `json:"distance,omitempty"`
	Vector           []float32              `json:"vector,omitempty"`
	Metadata         map[string]interface{} `json:"metadata,omitempty"`
}

type SearchResponse struct {
	Results         []SearchResult         `json:"results"`
	Count           int                    `json:"count"`
	ExecutionTimeMs float64                `json:"execution_time_ms"`
	TimeMs          float64                `json:"time_ms,omitempty"`
}

type ScalarQueryRequest struct {
	Filter            map[string]interface{}  `json:"filter"`
	Limit             int                     `json:"limit,omitempty"`
	Offset            int                     `json:"offset,omitempty"`
	Sort              string                  `json:"sort,omitempty"`
	SortOrder         string                  `json:"sort_order,omitempty"`
	IncludeDocuments  bool                    `json:"include_documents,omitempty"`
}

type HybridSearchRequest struct {
	VectorQuery       []float32               `json:"vector_query"`
	ScalarFilter      map[string]interface{}  `json:"scalar_filter"`
	Limit             int                     `json:"limit,omitempty"`
	Weight            float32                 `json:"weight,omitempty"`
}

type IndexConfig struct {
	Type     IndexType            `json:"type"`
	Metric   MetricType           `json:"metric"`
	Params   map[string]interface{} `json:"params,omitempty"`
}

type VectorIndexConfig struct {
	Type      IndexType            `json:"type"`
	Dimension int                  `json:"dimension"`
	Metric    MetricType           `json:"metric"`
	Params    HNSWParams           `json:"params,omitempty"`
}

type HNSWParams struct {
	M              int     `json:"m,omitempty"`
	EFConstruction int     `json:"ef_construction,omitempty"`
	EF             int     `json:"ef,omitempty"`
}

type ScalarIndexConfig struct {
	Field string `json:"field"`
	Type  string `json:"type"`
}

type CreateIndexRequest struct {
	Name   string                 `json:"name"`
	Type   string                 `json:"type"`
	Config map[string]interface{} `json:"config"`
}

type IndexInfo struct {
	Name      string                 `json:"name"`
	Type      IndexType              `json:"type"`
	Config    map[string]interface{} `json:"config,omitempty"`
	Status    string                 `json:"status,omitempty"`
	DocCount  int                    `json:"doc_count,omitempty"`
	CreatedAt time.Time             `json:"created_at,omitempty"`
}

type HealthCheckResponse struct {
	Status    string `json:"status"`
	Version   string `json:"version,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
}

type SystemStats struct {
	Storage    StorageStats       `json:"storage"`
	Indexes    IndexStats         `json:"indexes"`
	System     SystemInfo         `json:"system"`
}

type StorageStats struct {
	Type         string `json:"type"`
	Collections  int    `json:"collections"`
	Documents    int    `json:"documents"`
	UsedBytes    int64  `json:"used_bytes,omitempty"`
}

type IndexStats struct {
	Count          int `json:"count"`
	VectorIndexes  int `json:"vector_indexes"`
	ScalarIndexes  int `json:"scalar_indexes"`
}

type SystemInfo struct {
	RustVersion string `json:"rust_version,omitempty"`
	Version     string `json:"version"`
	Timestamp   string `json:"timestamp"`
}

type BatchSearchQuery struct {
	Queries           [][]float32             `json:"queries"`
	K                 int                     `json:"k"`
	Filter            map[string]interface{}  `json:"filter,omitempty"`
	IncludeMetadata   bool                    `json:"include_metadata,omitempty"`
}

type BatchSearchResult struct {
	Results          [][]SearchResult `json:"results"`
	ExecutionTimeMs  float64          `json:"execution_time_ms"`
}

type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	return e.Message
}
