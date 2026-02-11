#include "coretexdb/models.hpp"

namespace coretexdb {

void from_json(const nlohmann::json& j, FieldSchema& field) {
    if (j.contains("name")) j.at("name").get_to(field.name);
    if (j.contains("data_type")) j.at("data_type").get_to(field.data_type);
    if (j.contains("required")) j.at("required").get_to(field.required);
    if (j.contains("index")) j.at("index").get_to(field.index);
    if (j.contains("options")) field.options = j.at("options").get<std::map<std::string, std::optional<nlohmann::json>>>();
}

void to_json(nlohmann::json& j, const FieldSchema& field) {
    j = nlohmann::json::object();
    j["name"] = field.name;
    j["data_type"] = field.data_type;
    j["required"] = field.required;
    j["index"] = field.index;
    if (!field.options.empty()) {
        j["options"] = field.options;
    }
}

void from_json(const nlohmann::json& j, CollectionSchema& schema) {
    if (j.contains("fields")) j.at("fields").get_to(schema.fields);
}

void to_json(nlohmann::json& j, const CollectionSchema& schema) {
    j = nlohmann::json::object();
    j["fields"] = schema.fields;
}

void from_json(const nlohmann::json& j, Collection& collection) {
    if (j.contains("name")) j.at("name").get_to(collection.name);
    if (j.contains("schema")) collection.schema = j.at("schema").get<CollectionSchema>();
    if (j.contains("description")) collection.description = j.at("description").get<std::string>();
    if (j.contains("settings")) collection.settings = j.at("settings").get<std::map<std::string, nlohmann::json>>();
    if (j.contains("count")) j.at("count").get_to(collection.count);
    if (j.contains("created_at")) collection.created_at = j.at("created_at").get<std::string>();
    if (j.contains("updated_at")) collection.updated_at = j.at("updated_at").get<std::string>();
}

void to_json(nlohmann::json& j, const Collection& collection) {
    j = nlohmann::json::object();
    j["name"] = collection.name;
    if (collection.schema) j["schema"] = *collection.schema;
    if (collection.description) j["description"] = *collection.description;
    if (!collection.settings.empty()) j["settings"] = collection.settings;
    j["count"] = collection.count;
    if (collection.created_at) j["created_at"] = *collection.created_at;
    if (collection.updated_at) j["updated_at"] = *collection.updated_at;
}

void from_json(const nlohmann::json& j, Document& doc) {
    if (j.contains("id")) j.at("id").get_to(doc.id);
    if (j.contains("vector")) doc.vector = j.at("vector").get<std::vector<float>>();
    if (j.contains("metadata")) doc.metadata = j.at("metadata").get<std::map<std::string, nlohmann::json>>();
    if (j.contains("score")) j.at("score").get_to(doc.score);
    if (j.contains("created_at")) doc.created_at = j.at("created_at").get<std::string>();
    if (j.contains("updated_at")) doc.updated_at = j.at("updated_at").get<std::string>();
}

void to_json(nlohmann::json& j, const Document& doc) {
    j = nlohmann::json::object();
    j["id"] = doc.id;
    if (doc.vector) j["vector"] = *doc.vector;
    if (!doc.metadata.empty()) j["metadata"] = doc.metadata;
    j["score"] = doc.score;
    if (doc.created_at) j["created_at"] = *doc.created_at;
    if (doc.updated_at) j["updated_at"] = *doc.updated_at;
}

void from_json(const nlohmann::json& j, InsertResponse& response) {
    if (j.contains("inserted")) j.at("inserted").get_to(response.inserted);
    if (j.contains("ids")) response.ids = j.at("ids").get<std::vector<std::string>>();
    if (j.contains("time_ms")) j.at("time_ms").get_to(response.time_ms);
}

void to_json(nlohmann::json& j, const InsertResponse& response) {
    j = nlohmann::json::object();
    j["inserted"] = response.inserted;
    if (response.ids) j["ids"] = *response.ids;
    j["time_ms"] = response.time_ms;
}

void from_json(const nlohmann::json& j, SearchResult& result) {
    if (j.contains("id")) j.at("id").get_to(result.id);
    if (j.contains("score")) j.at("score").get_to(result.score);
    if (j.contains("distance")) j.at("distance").get_to(result.distance);
    if (j.contains("vector")) result.vector = j.at("vector").get<std::vector<float>>();
    if (j.contains("metadata")) result.metadata = j.at("metadata").get<std::map<std::string, nlohmann::json>>();
}

void to_json(nlohmann::json& j, const SearchResult& result) {
    j = nlohmann::json::object();
    j["id"] = result.id;
    j["score"] = result.score;
    j["distance"] = result.distance;
    if (result.vector) j["vector"] = *result.vector;
    if (!result.metadata.empty()) j["metadata"] = result.metadata;
}

void from_json(const nlohmann::json& j, SearchResponse& response) {
    if (j.contains("results")) j.at("results").get_to(response.results);
    if (j.contains("count")) j.at("count").get_to(response.count);
    if (j.contains("execution_time_ms")) j.at("execution_time_ms").get_to(response.execution_time_ms);
    if (j.contains("time_ms")) j.at("time_ms").get_to(response.time_ms);
}

void to_json(nlohmann::json& j, const SearchResponse& response) {
    j = nlohmann::json::object();
    j["results"] = response.results;
    j["count"] = response.count;
    j["execution_time_ms"] = response.execution_time_ms;
    j["time_ms"] = response.time_ms;
}

void from_json(const nlohmann::json& j, IndexInfo& info) {
    if (j.contains("name")) j.at("name").get_to(info.name);
    if (j.contains("type")) info.type = j.at("type").get<IndexType>();
    if (j.contains("config")) info.config = j.at("config").get<std::map<std::string, nlohmann::json>>();
    if (j.contains("status")) info.status = j.at("status").get<std::string>();
    if (j.contains("doc_count")) j.at("doc_count").get_to(info.doc_count);
    if (j.contains("created_at")) info.created_at = j.at("created_at").get<std::string>();
}

void to_json(nlohmann::json& j, const IndexInfo& info) {
    j = nlohmann::json::object();
    j["name"] = info.name;
    j["type"] = info.type;
    if (!info.config.empty()) j["config"] = info.config;
    if (info.status) j["status"] = *info.status;
    j["doc_count"] = info.doc_count;
    if (info.created_at) j["created_at"] = *info.created_at;
}

void from_json(const nlohmann::json& j, HealthCheckResponse& response) {
    if (j.contains("status")) j.at("status").get_to(response.status);
    if (j.contains("version")) response.version = j.at("version").get<std::string>();
    if (j.contains("timestamp")) response.timestamp = j.at("timestamp").get<std::string>();
}

void to_json(nlohmann::json& j, const HealthCheckResponse& response) {
    j = nlohmann::json::object();
    j["status"] = response.status;
    if (response.version) j["version"] = *response.version;
    if (response.timestamp) j["timestamp"] = *response.timestamp;
}

void from_json(const nlohmann::json& j, StorageStats& stats) {
    if (j.contains("type")) j.at("type").get_to(stats.type);
    if (j.contains("collections")) j.at("collections").get_to(stats.collections);
    if (j.contains("documents")) j.at("documents").get_to(stats.documents);
    if (j.contains("used_bytes")) j.at("used_bytes").get_to(stats.used_bytes);
}

void from_json(const nlohmann::json& j, IndexStats& stats) {
    if (j.contains("count")) j.at("count").get_to(stats.count);
    if (j.contains("vector_indexes")) j.at("vector_indexes").get_to(stats.vector_indexes);
    if (j.contains("scalar_indexes")) j.at("scalar_indexes").get_to(stats.scalar_indexes);
}

void from_json(const nlohmann::json& j, SystemInfo& info) {
    if (j.contains("rust_version")) info.rust_version = j.at("rust_version").get<std::string>();
    if (j.contains("version")) j.at("version").get_to(info.version);
    if (j.contains("timestamp")) j.at("timestamp").get_to(info.timestamp);
}

void from_json(const nlohmann::json& j, SystemStats& stats) {
    if (j.contains("storage")) j.at("storage").get_to(stats.storage);
    if (j.contains("indexes")) j.at("indexes").get_to(stats.indexes);
    if (j.contains("system")) j.at("system").get_to(stats.system);
}

} // namespace coretexdb
