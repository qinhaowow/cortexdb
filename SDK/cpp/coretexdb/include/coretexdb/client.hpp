#pragma once

#include <string>
#include <memory>
#include <curl/curl.h>

namespace coretexdb {

class Client {
public:
    explicit Client(const std::string& host, int port);
    Client(const std::string& host, int port, const std::string& apiKey, int timeout = 30);
    ~Client();

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;
    Client(Client&&) = delete;
    Client& operator=(Client&&) = delete;

    HealthCheckResponse healthCheck();
    Collection createCollection(const CreateCollectionRequest& request);
    Collection createCollection(const std::string& name);
    std::vector<std::string> listCollections();
    Collection getCollection(const std::string& name);
    void deleteCollection(const std::string& name);

    InsertResponse insertDocuments(const std::string& collection, 
                                   const std::vector<Document>& documents,
                                   bool overwrite = false);
    Document getDocument(const std::string& collection, const std::string& id);
    void deleteDocument(const std::string& collection, const std::string& id);

    SearchResponse vectorSearch(const std::string& collection, const VectorSearchRequest& request);
    SearchResponse vectorSearch(const std::string& collection, 
                                const std::vector<float>& query, 
                                int limit = 10);
    SearchResponse scalarQuery(const std::string& collection, const ScalarQueryRequest& request);
    SearchResponse hybridSearch(const std::string& collection, const HybridSearchRequest& request);

    void createIndex(const std::string& collection, const CreateIndexRequest& request);
    void createIndex(const std::string& collection, 
                     const std::string& name, 
                     const std::string& type,
                     const std::map<std::string, nlohmann::json>& config);
    std::vector<IndexInfo> listIndexes(const std::string& collection);
    void deleteIndex(const std::string& collection, const std::string& name);

    SystemStats systemStats();

    BatchSearchResult batchVectorSearch(const std::string& collection, 
                                        const BatchSearchQuery& request);

    std::string getBaseUrl() const;

private:
    std::string host_;
    int port_;
    std::string apiKey_;
    int timeout_;
    CURL* curl_;

    void initializeCurl();
    void cleanupCurl();

    template<typename T>
    T request(const std::string& method, 
             const std::string& endpoint, 
             const nlohmann::json* body = nullptr);

    std::string encodePathParam(const std::string& param) const;
    void addHeaders(struct curl_slist* headers) const;
    
    template<typename T>
    T parseResponse(CURL* res, const std::string& url, long* response_code);
};

} // namespace coretexdb
