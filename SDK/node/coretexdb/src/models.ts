export enum MetricType {
  COSINE = 'cosine',
  EUCLIDEAN = 'euclidean',
  DOT_PRODUCT = 'dotproduct'
}

export enum IndexType {
  HNSW = 'hnsw',
  IVF = 'ivf',
  SCALAR = 'scalar'
}

export interface CollectionSchema {
  fields: FieldSchema[];
}

export interface FieldSchema {
  name: string;
  data_type: string;
  required: boolean;
  index: boolean;
  options?: Record<string, unknown>;
}

export interface Collection {
  name: string;
  schema?: CollectionSchema;
  description?: string;
  settings?: Record<string, unknown>;
  count?: number;
  created_at?: string;
  updated_at?: string;
}

export interface CreateCollectionRequest {
  name: string;
  schema?: CollectionSchema;
  description?: string;
  settings?: Record<string, unknown>;
}

export interface Document {
  id: string;
  vector?: number[];
  metadata?: Record<string, unknown>;
  score?: number;
  created_at?: string;
  updated_at?: string;
}

export interface InsertDocumentsRequest {
  documents: Document[];
  overwrite?: boolean;
}

export interface InsertResponse {
  inserted: number;
  ids?: string[];
  time_ms?: number;
}

export interface VectorSearchRequest {
  query: number[];
  limit?: number;
  threshold?: number;
  include_documents?: boolean;
  filter?: Record<string, unknown>;
  params?: Record<string, unknown>;
}

export interface SearchResult {
  id: string;
  score: number;
  distance?: number;
  vector?: number[];
  metadata?: Record<string, unknown>;
}

export interface SearchResponse {
  results: SearchResult[];
  count: number;
  execution_time_ms?: number;
  time_ms?: number;
}

export interface ScalarQueryRequest {
  filter: Record<string, unknown>;
  limit?: number;
  offset?: number;
  sort?: string;
  sort_order?: string;
  include_documents?: boolean;
}

export interface HybridSearchRequest {
  vector_query: number[];
  scalar_filter: Record<string, unknown>;
  limit?: number;
  weight?: number;
}

export interface IndexConfig {
  type: IndexType;
  metric: MetricType;
  params?: Record<string, unknown>;
}

export interface VectorIndexConfig {
  type: IndexType;
  dimension: number;
  metric: MetricType;
  params?: HNSWParams;
}

export interface HNSWParams {
  m?: number;
  ef_construction?: number;
  ef?: number;
}

export interface ScalarIndexConfig {
  field: string;
  type: string;
}

export interface CreateIndexRequest {
  name: string;
  type: string;
  config: Record<string, unknown>;
}

export interface IndexInfo {
  name: string;
  type: IndexType;
  config?: Record<string, unknown>;
  status?: string;
  doc_count?: number;
  created_at?: string;
}

export interface HealthCheckResponse {
  status: string;
  version?: string;
  timestamp?: string;
}

export interface SystemStats {
  storage: StorageStats;
  indexes: IndexStats;
  system: SystemInfo;
}

export interface StorageStats {
  type: string;
  collections: number;
  documents: number;
  used_bytes?: number;
}

export interface IndexStats {
  count: number;
  vector_indexes: number;
  scalar_indexes: number;
}

export interface SystemInfo {
  rust_version?: string;
  version: string;
  timestamp: string;
}

export interface BatchSearchQuery {
  queries: number[][];
  k: number;
  filter?: Record<string, unknown>;
  include_metadata?: boolean;
}

export interface BatchSearchResult {
  results: SearchResult[][];
  execution_time_ms: number;
}

export interface APIError extends Error {
  statusCode: number;
  message: string;
}

export interface ClientOptions {
  host?: string;
  port?: number;
  apiKey?: string;
  timeout?: number;
}
