package com.coretexdb.examples;

import com.coretexdb.CoretexDBClient;
import com.coretexdb.models.*;

import java.util.*;

public class BasicExample {
    public static void main(String[] args) {
        try {
            CoretexDBClient client = new CoretexDBClient("localhost", 8080);

            HealthCheckResponse health = client.healthCheck();
            System.out.println("Health check: " + health.getStatus());

            List<String> collections = client.listCollections();
            System.out.println("Collections: " + collections);

            Collection collection = client.createCollection("test_collection");
            System.out.println("Created collection: " + collection.getName());

            List<Document> documents = new ArrayList<>();
            Document doc1 = new Document();
            doc1.setId("doc1");
            doc1.setVector(Arrays.asList(1.0f, 2.0f, 3.0f));
            Map<String, Object> metadata1 = new HashMap<>();
            metadata1.put("text", "Hello world");
            doc1.setMetadata(metadata1);
            documents.add(doc1);

            Document doc2 = new Document();
            doc2.setId("doc2");
            doc2.setVector(Arrays.asList(4.0f, 5.0f, 6.0f));
            Map<String, Object> metadata2 = new HashMap<>();
            metadata2.put("text", "Goodbye world");
            doc2.setMetadata(metadata2);
            documents.add(doc2);

            InsertResponse insertResponse = client.insertDocuments("test_collection", documents);
            System.out.println("Inserted: " + insertResponse.getInserted());

            SearchResponse searchResponse = client.vectorSearch("test_collection", Arrays.asList(1.0f, 2.0f, 3.0f), 10);
            System.out.println("Search results: " + searchResponse.getResults().size());

            client.deleteCollection("test_collection");
            System.out.println("Deleted test_collection");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
