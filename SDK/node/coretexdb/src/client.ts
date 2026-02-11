import axios, { AxiosInstance, AxiosError } from 'axios';
import {
  Collection,
  CreateCollectionRequest,
  Document,
  InsertDocumentsRequest,
  InsertResponse,
  VectorSearchRequest,
  SearchResponse,
  CreateIndexRequest,
  IndexInfo,
  HealthCheckResponse,
  SystemStats,
  ClientOptions,
  APIError,
  ScalarQueryRequest,
  HybridSearchRequest,
  SearchResult,
  BatchSearchQuery,
  BatchSearchResult
} from './models';

export class CoretexDBClient {
  private client: AxiosInstance;
  private host: string;
  private port: number;
  private apiKey?: string;

  constructor(options: ClientOptions = {}) {
    this.host = options.host || 'localhost';
    this.port = options.port || 8080;
    this.apiKey = options.apiKey || process.env.CORTEXDB_API_KEY;
    const timeout = options.timeout || 30;

    this.client = axios.create({
      baseURL: `http://${this.host}:${this.port}`,
      timeout: timeout * 1000,
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (this.apiKey) {
      this.client.defaults.headers.common['Authorization'] = `Bearer ${this.apiKey}`;
    }

    this.client.interceptors.response.use(
      (response) => response,
      (error: AxiosError) => {
        const apiError: APIError = new Error(
          (error.response?.data as string) || error.message
        ) as APIError;
        apiError.statusCode = error.response?.status || 500;
        apiError.message = (error.response?.data as string) || error.message;
        return Promise.reject(apiError);
      }
    );
  }

  private async request<T>(
    method: 'get' | 'post' | 'put' | 'delete',
    endpoint: string,
    data?: unknown
  ): Promise<T> {
    const response = await this.client.request<T>({
      method,
      url: endpoint,
      data,
    });
    return response.data;
  }

  async healthCheck(): Promise<HealthCheckResponse> {
    return this.request<HealthCheckResponse>('get', '/api/health');
  }

  async createCollection(request: CreateCollectionRequest): Promise<Collection> {
    return this.request<Collection>('post', '/api/collections', request);
  }

  async createCollection(name: string): Promise<Collection> {
    return this.createCollection({ name });
  }

  async listCollections(): Promise<string[]> {
    interface ListCollectionsResponse {
      collections: string[];
    }
    const response = await this.request<ListCollectionsResponse>('get', '/api/collections');
    return response.collections;
  }

  async getCollection(name: string): Promise<Collection> {
    return this.request<Collection>('get', `/api/collections/${encodeURIComponent(name)}`);
  }

  async deleteCollection(name: string): Promise<void> {
    await this.request('delete', `/api/collections/${encodeURIComponent(name)}`);
  }

  async insertDocuments(
    collection: string,
    documents: Document[],
    overwrite = false
  ): Promise<InsertResponse> {
    const request: InsertDocumentsRequest = {
      documents,
      overwrite,
    };
    return this.request<InsertResponse>(
      'post',
      `/api/collections/${encodeURIComponent(collection)}/documents`,
      request
    );
  }

  async getDocument(collection: string, id: string): Promise<Document> {
    return this.request<Document>(
      'get',
      `/api/collections/${encodeURIComponent(collection)}/documents/${encodeURIComponent(id)}`
    );
  }

  async deleteDocument(collection: string, id: string): Promise<void> {
    await this.request(
      'delete',
      `/api/collections/${encodeURIComponent(collection)}/documents/${encodeURIComponent(id)}`
    );
  }

  async vectorSearch(
    collection: string,
    request: VectorSearchRequest
  ): Promise<SearchResponse> {
    return this.request<SearchResponse>(
      'post',
      `/api/collections/${encodeURIComponent(collection)}/search`,
      request
    );
  }

  async vectorSearch(
    collection: string,
    query: number[],
    limit = 10
  ): Promise<SearchResponse> {
    return this.vectorSearch(collection, {
      query,
      limit,
      include_documents: true,
    });
  }

  async scalarQuery(
    collection: string,
    request: ScalarQueryRequest
  ): Promise<SearchResponse> {
    return this.request<SearchResponse>(
      'post',
      `/api/collections/${encodeURIComponent(collection)}/query`,
      request
    );
  }

  async hybridSearch(
    collection: string,
    request: HybridSearchRequest
  ): Promise<SearchResponse> {
    return this.request<SearchResponse>(
      'post',
      `/api/collections/${encodeURIComponent(collection)}/hybrid`,
      request
    );
  }

  async createIndex(
    collection: string,
    request: CreateIndexRequest
  ): Promise<void> {
    await this.request(
      'post',
      `/api/collections/${encodeURIComponent(collection)}/indexes`,
      request
    );
  }

  async createIndex(
    collection: string,
    name: string,
    type: string,
    config: Record<string, unknown>
  ): Promise<void> {
    return this.createIndex(collection, { name, type, config });
  }

  async listIndexes(collection: string): Promise<IndexInfo[]> {
    interface ListIndexesResponse {
      indexes: IndexInfo[];
    }
    const response = await this.request<ListIndexesResponse>(
      'get',
      `/api/collections/${encodeURIComponent(collection)}/indexes`
    );
    return response.indexes;
  }

  async deleteIndex(collection: string, name: string): Promise<void> {
    await this.request(
      'delete',
      `/api/collections/${encodeURIComponent(collection)}/indexes/${encodeURIComponent(name)}`
    );
  }

  async systemStats(): Promise<SystemStats> {
    return this.request<SystemStats>('get', '/api/system/stats');
  }

  async batchVectorSearch(
    collection: string,
    queries: number[][],
    options: { k?: number; filter?: Record<string, unknown> } = {}
  ): Promise<BatchSearchResult> {
    const request: BatchSearchQuery = {
      queries,
      k: options.k || 10,
      filter: options.filter,
      include_metadata: true,
    };
    return this.request<BatchSearchResult>(
      'post',
      `/api/collections/${encodeURIComponent(collection)}/batch-search`,
      request
    );
  }

  getBaseUrl(): string {
    return `http://${this.host}:${this.port}`;
  }
}

export { SearchResult, Document, Collection, VectorSearchRequest };
