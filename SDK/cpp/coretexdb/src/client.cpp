#include "coretexdb/client.hpp"
#include "coretexdb/models.hpp"
#include <iostream>
#include <sstream>
#include <chrono>

namespace coretexdb {

Client::Client(const std::string& host, int port)
    : Client(host, port, "", 30) {}

Client::Client(const std::string& host, int port, const std::string& apiKey, int timeout)
    : host_(host), port_(port), apiKey_(apiKey), timeout_(timeout), curl_(nullptr) {
    initializeCurl();
}

Client::~Client() {
    cleanupCurl();
}

void Client::initializeCurl() {
    curl_global_init(CURL_GLOBAL_ALL);
    curl_ = curl_easy_init();
}

void Client::cleanupCurl() {
    if (curl_) {
        curl_easy_cleanup(curl_);
    }
    curl_global_cleanup();
}

std::string Client::getBaseUrl() const {
    std::ostringstream oss;
    oss << "http://" << host_ << ":" << port_;
    return oss.str();
}

std::string Client::encodePathParam(const std::string& param) const {
    std::string encoded;
    for (char c : param) {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            encoded += c;
        } else {
            std::ostringstream oss;
            oss << "%" << std::uppercase << std::hex << static_cast<int>(static_cast<unsigned char>(c));
            encoded += oss.str();
        }
    }
    return encoded;
}

void Client::addHeaders(struct curl_slist* headers) const {
    if (apiKey_.empty()) return;
    
    std::string authHeader = "Authorization: Bearer " + apiKey_;
    curl_slist_append(headers, authHeader.c_str());
}

template<typename T>
T Client::request(const std::string& method, 
                 const std::string& endpoint, 
                 const nlohmann::json* body) {
    std::string url = getBaseUrl() + endpoint;
    
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    addHeaders(headers);

    std::string response_string;
    std::string header_string;

    curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl_, CURLOPT_CUSTOMREQUEST, method.c_str());
    curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, [](char* ptr, size_t size, size_t nmemb, void* userdata) {
        std::string* response = static_cast<std::string*>(userdata);
        response->append(ptr, size * nmemb);
        return size * nmemb;
    });
    curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &response_string);
    curl_easy_setopt(curl_, CURLOPT_HEADERDATA, &header_string);

    if (body) {
        std::string body_str = body->dump();
        curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, body_str.c_str());
    }

    long response_code;
    CURLcode res = curl_easy_perform(curl_);
    
    curl_slist_free_all(headers);

    if (res != CURLE_OK) {
        throw APIError(0, curl_easy_strerror(res));
    }

    curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &response_code);

    if (response_code < 200 || response_code >= 300) {
        throw APIError(static_cast<int>(response_code), response_string);
    }

    if constexpr (std::is_same_v<T, void>) {
        return;
    } else {
        if (response_string.empty()) {
            return T();
        }
        return nlohmann::json::parse(response_string).get<T>();
    }
}

template<typename T>
T Client::parseResponse(CURL* res, const std::string& url, long* response_code) {
    return T();
}

HealthCheckResponse Client::healthCheck() {
    return request<HealthCheckResponse>("GET", "/api/health");
}

Collection Client::createCollection(const CreateCollectionRequest& request) {
    nlohmann::json body;
    body["name"] = request.name;
    if (request.schema) {
        body["schema"] = *request.schema;
    }
    if (request.description) {
        body["description"] = *request.description;
    }
    if (!request.settings.empty()) {
        body["settings"] = request.settings;
    }
    return request<Collection>("POST", "/api/collections", &body);
}

Collection Client::createCollection(const std::string& name) {
    return createCollection(CreateCollectionRequest{.name = name});
}

std::vector<std::string> Client::listCollections() {
    struct ListCollectionsResponse {
        std::vector<std::string> collections;
    };
    auto response = request<ListCollectionsResponse>("GET", "/api/collections");
    return response.collections;
}

Collection Client::getCollection(const std::string& name) {
    std::string endpoint = "/api/collections/" + encodePathParam(name);
    return request<Collection>("GET", endpoint);
}

void Client::deleteCollection(const std::string& name) {
    std::string endpoint = "/api/collections/" + encodePathParam(name);
    request<void>("DELETE", endpoint);
}

InsertResponse Client::insertDocuments(const std::string& collection, 
                                       const std::vector<Document>& documents,
                                       bool overwrite) {
    std::string endpoint = "/api/collections/" + encodePathParam(collection) + "/documents";
    
    nlohmann::json body;
    for (const auto& doc : documents) {
        nlohmann::json docJson;
        docJson["id"] = doc.id;
        if (doc.vector) {
            docJson["vector"] = *doc.vector;
        }
        if (!doc.metadata.empty()) {
            docJson["metadata"] = doc.metadata;
        }
        body["documents"].push_back(docJson);
    }
    body["overwrite"] = overwrite;
    
    return request<InsertResponse>("POST", endpoint, &body);
}

Document Client::getDocument(const std::string& collection, const std::string& id) {
    std::string endpoint = "/api/collections/" + encodePathParam(collection) + 
                          "/documents/" + encodePathParam(id);
    return request<Document>("GET", endpoint);
}

void Client::deleteDocument(const std::string& collection, const std::string& id) {
    std::string endpoint = "/api/collections/" + encodePathParam(collection) + 
                          "/documents/" + encodePathParam(id);
    request<void>("DELETE", endpoint);
}

SearchResponse Client::vectorSearch(const std::string& collection, const VectorSearchRequest& request) {
    std::string endpoint = "/api/collections/" + encodePathParam(collection) + "/search";
    
    nlohmann::json body;
    body["query"] = request.query;
    body["limit"] = request.limit;
    body["threshold"] = request.threshold;
    body["include_documents"] = request.include_documents;
    if (!request.filter.empty()) {
        body["filter"] = request.filter;
    }
    if (!request.params.empty()) {
        body["params"] = request.params;
    }
    
    return request<SearchResponse>("POST", endpoint, &body);
}

SearchResponse Client::vectorSearch(const std::string& collection, 
                                    const std::vector<float>& query, 
                                    int limit) {
    VectorSearchRequest request;
    request.query = query;
    request.limit = limit;
    request.include_documents = true;
    return vectorSearch(collection, request);
}

SearchResponse Client::scalarQuery(const std::string& collection, const ScalarQueryRequest& request) {
    std::string endpoint = "/api/collections/" + encodePathParam(collection) + "/query";
    
    nlohmann::json body;
    body["filter"] = request.filter;
    body["limit"] = request.limit;
    body["offset"] = request.offset;
    body["include_documents"] = request.include_documents;
    if (request.sort) {
        body["sort"] = *request.sort;
    }
    if (request.sort_order) {
        body["sort_order"] = *request.sort_order;
    }
    
    return request<SearchResponse>("POST", endpoint, &body);
}

SearchResponse Client::hybridSearch(const std::string& collection, const HybridSearchRequest& request) {
    std::string endpoint = "/api/collections/" + encodePathParam(collection) + "/hybrid";
    
    nlohmann::json body;
    body["vector_query"] = request.vector_query;
    body["scalar_filter"] = request.scalar_filter;
    body["limit"] = request.limit;
    body["weight"] = request.weight;
    
    return request<SearchResponse>("POST", endpoint, &body);
}

void Client::createIndex(const std::string& collection, const CreateIndexRequest& request) {
    std::string endpoint = "/api/collections/" + encodePathParam(collection) + "/indexes";
    
    nlohmann::json body;
    body["name"] = request.name;
    body["type"] = request.type;
    body["config"] = request.config;
    
    request<void>("POST", endpoint, &body);
}

void Client::createIndex(const std::string& collection, 
                         const std::string& name, 
                         const std::string& type,
                         const std::map<std::string, nlohmann::json>& config) {
    CreateIndexRequest request;
    request.name = name;
    request.type = type;
    request.config = config;
    createIndex(collection, request);
}

std::vector<IndexInfo> Client::listIndexes(const std::string& collection) {
    std::string endpoint = "/api/collections/" + encodePathParam(collection) + "/indexes";
    
    struct ListIndexesResponse {
        std::vector<IndexInfo> indexes;
    };
    auto response = request<ListIndexesResponse>("GET", endpoint);
    return response.indexes;
}

void Client::deleteIndex(const std::string& collection, const std::string& name) {
    std::string endpoint = "/api/collections/" + encodePathParam(collection) + 
                          "/indexes/" + encodePathParam(name);
    request<void>("DELETE", endpoint);
}

SystemStats Client::systemStats() {
    return request<SystemStats>("GET", "/api/system/stats");
}

BatchSearchResult Client::batchVectorSearch(const std::string& collection, 
                                            const BatchSearchQuery& request) {
    std::string endpoint = "/api/collections/" + encodePathParam(collection) + "/batch-search";
    
    nlohmann::json body;
    body["queries"] = request.queries;
    body["k"] = request.k;
    body["include_metadata"] = request.include_metadata;
    if (!request.filter.empty()) {
        body["filter"] = request.filter;
    }
    
    return request<BatchSearchResult>("POST", endpoint, &body);
}

} // namespace coretexdb
