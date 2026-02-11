#include "coretexdb/client.hpp"
#include <iostream>
#include <vector>
#include <map>

int main() {
    try {
        coretexdb::Client client("localhost", 8080);

        auto health = client.healthCheck();
        std::cout << "Health check: " << health.status << std::endl;

        auto collections = client.listCollections();
        std::cout << "Collections: ";
        for (const auto& c : collections) {
            std::cout << c << " ";
        }
        std::cout << std::endl;

        coretexdb::CreateCollectionRequest createRequest;
        createRequest.name = "test_collection";
        createRequest.description = "Test collection created via C++ SDK";
        auto collection = client.createCollection(createRequest);
        std::cout << "Created collection: " << collection.name << std::endl;

        std::vector<coretexdb::Document> documents;
        coretexdb::Document doc1;
        doc1.id = "doc1";
        doc1.vector = {1.0f, 2.0f, 3.0f};
        doc1.metadata["text"] = "Hello world";
        documents.push_back(doc1);

        coretexdb::Document doc2;
        doc2.id = "doc2";
        doc2.vector = {4.0f, 5.0f, 6.0f};
        doc2.metadata["text"] = "Goodbye world";
        documents.push_back(doc2);

        auto insertResponse = client.insertDocuments("test_collection", documents);
        std::cout << "Inserted: " << insertResponse.inserted << std::endl;

        auto searchResponse = client.vectorSearch("test_collection", {1.0f, 2.0f, 3.0f}, 10);
        std::cout << "Search results: " << searchResponse.results.size() << std::endl;

        for (const auto& result : searchResponse.results) {
            std::cout << "  - ID: " << result.id << ", Score: " << result.score << std::endl;
        }

        client.deleteCollection("test_collection");
        std::cout << "Deleted test_collection" << std::endl;

    } catch (const coretexdb::APIError& e) {
        std::cerr << "API Error: " << e.what() << " (Status: " << e.status_code << ")" << std::endl;
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
