#pragma once

#include <string>
#include <vector>
#include <map>
#include <optional>
#include <memory>
#include <functional>

namespace coretexdb {

enum class MetricType {
    COSINE,
    EUCLIDEAN,
    DOT_PRODUCT
};

enum class IndexType {
    HNSW,
    IVF,
    SCALAR
};

struct FieldSchema {
    std::string name;
    std::string data_type;
    bool required = false;
    bool index = false;
    std::map<std::string, std::optional<nlohmann::json>> options;
};

struct CollectionSchema {
    std::vector<FieldSchema> fields;
};

struct Collection {
    std::string name;
    std::optional<CollectionSchema> schema;
    std::optional<std::string> description;
    std::map<std::string, nlohmann::json> settings;
    int count = 0;
    std::optional<std::string> created_at;
    std::optional<std::string> updated_at;
};

struct CreateCollectionRequest {
    std::string name;
    std::optional<CollectionSchema> schema;
    std::optional<std::string> description;
    std::map<std::string, nlohmann::json> settings;
};

struct Document {
    std::string id;
    std::optional<std::vector<float>> vector;
    std::map<std::string, nlohmann::json> metadata;
    float score = 0.0f;
    std::optional<std::string> created_at;
    std::optional<std::string> updated_at;
};

struct InsertDocumentsRequest {
    std::vector<Document> documents;
    bool overwrite = false;
};

struct InsertResponse {
    int inserted = 0;
    std::optional<std::vector<std::string>> ids;
    double time_ms = 0.0;
};

struct VectorSearchRequest {
    std::vector<float> query;
    int limit = 10;
    float threshold = 0.0f;
    bool include_documents = true;
    std::map<std::string, nlohmann::json> filter;
    std::map<std::string, nlohmann::json> params;
};

struct SearchResult {
    std::string id;
    float score = 0.0f;
    float distance = 0.0f;
    std::optional<std::vector<float>> vector;
    std::map<std::string, nlohmann::json> metadata;
};

struct SearchResponse {
    std::vector<SearchResult> results;
    int count = 0;
    double execution_time_ms = 0.0;
    double time_ms = 0.0;
};

struct ScalarQueryRequest {
    std::map<std::string, nlohmann::json> filter;
    int limit = 10;
    int offset = 0;
    std::optional<std::string> sort;
    std::optional<std::string> sort_order;
    bool include_documents = true;
};

struct HybridSearchRequest {
    std::vector<float> vector_query;
    std::map<std::string, nlohmann::json> scalar_filter;
    int limit = 10;
    float weight = 0.5f;
};

struct HNSWParams {
    int m = 16;
    int ef_construction = 200;
    int ef = 10;
};

struct VectorIndexConfig {
    IndexType type = IndexType::HNSW;
    int dimension = 0;
    MetricType metric = MetricType::COSINE;
    HNSWParams params;
};

struct ScalarIndexConfig {
    std::string field;
    std::string type;
};

struct CreateIndexRequest {
    std::string name;
    std::string type;
    std::map<std::string, nlohmann::json> config;
};

struct IndexInfo {
    std::string name;
    IndexType type;
    std::map<std::string, nlohmann::json> config;
    std::optional<std::string> status;
    int doc_count = 0;
    std::optional<std::string> created_at;
};

struct HealthCheckResponse {
    std::string status;
    std::optional<std::string> version;
    std::optional<std::string> timestamp;
};

struct StorageStats {
    std::string type;
    int collections = 0;
    int documents = 0;
    long long used_bytes = 0;
};

struct IndexStats {
    int count = 0;
    int vector_indexes = 0;
    int scalar_indexes = 0;
};

struct SystemInfo {
    std::optional<std::string> rust_version;
    std::string version;
    std::string timestamp;
};

struct SystemStats {
    StorageStats storage;
    IndexStats indexes;
    SystemInfo system;
};

struct BatchSearchQuery {
    std::vector<std::vector<float>> queries;
    int k = 10;
    std::map<std::string, nlohmann::json> filter;
    bool include_metadata = true;
};

struct BatchSearchResult {
    std::vector<std::vector<SearchResult>> results;
    double execution_time_ms = 0.0;
};

struct APIError : public std::runtime_error {
    int status_code;
    std::string message;
    
    APIError(int code, const std::string& msg) 
        : std::runtime_error(msg), status_code(code), message(msg) {}
};

} // namespace coretexdb
