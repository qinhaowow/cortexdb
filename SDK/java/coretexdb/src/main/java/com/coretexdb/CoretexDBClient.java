package com.coretexdb;

import com.coretexdb.models.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CoretexDBClient {
    private final String host;
    private final int port;
    private final String apiKey;
    private final int timeout;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public CoretexDBClient(String host, int port, String apiKey, int timeout) {
        this.host = host;
        this.port = port;
        this.apiKey = apiKey;
        this.timeout = timeout;
        this.httpClient = HttpClientBuilder.create()
                .setConnectionRequestTimeout(timeout * 1000)
                .setSocketTimeout(timeout * 1000)
                .build();
        this.objectMapper = new ObjectMapper();
    }

    public CoretexDBClient(String host, int port, String apiKey) {
        this(host, port, apiKey, 30);
    }

    public CoretexDBClient(String host, int port) {
        this(host, port, null, 30);
    }

    private String getBaseUrl() {
        return String.format("http://%s:%d", host, port);
    }

    private Map<String, String> getHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        if (apiKey != null && !apiKey.isEmpty()) {
            headers.put("Authorization", "Bearer " + apiKey);
        }
        return headers;
    }

    private <T> T executeRequest(HttpGet request, Class<T> responseClass) throws IOException {
        addHeaders(request);
        HttpResponse response = httpClient.execute(request);
        return parseResponse(response, responseClass);
    }

    private <T> T executeRequest(HttpPost request, Class<T> responseClass) throws IOException {
        addHeaders(request);
        HttpResponse response = httpClient.execute(request);
        return parseResponse(response, responseClass);
    }

    private <T> T executeRequest(HttpPut request, Class<T> responseClass) throws IOException {
        addHeaders(request);
        HttpResponse response = httpClient.execute(request);
        return parseResponse(response, responseClass);
    }

    private void executeRequest(HttpDelete request) throws IOException {
        addHeaders(request);
        httpClient.execute(request);
    }

    private void addHeaders(HttpGet request) {
        getHeaders().forEach(request::addHeader);
    }

    private void addHeaders(HttpPost request) {
        getHeaders().forEach(request::addHeader);
    }

    private void addHeaders(HttpPut request) {
        getHeaders().forEach(request::addHeader);
    }

    private void addHeaders(HttpDelete request) {
        getHeaders().forEach(request::addHeader);
    }

    private <T> T parseResponse(HttpResponse response, Class<T> responseClass) throws IOException {
        int statusCode = response.getStatusLine().getStatusCode();
        String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);

        if (statusCode < 200 || statusCode >= 300) {
            throw new APIError(statusCode, body);
        }

        if (responseClass == null || body == null || body.isEmpty()) {
            return null;
        }

        return objectMapper.readValue(body, responseClass);
    }

    public HealthCheckResponse healthCheck() throws IOException {
        String url = getBaseUrl() + "/api/health";
        HttpGet request = new HttpGet(url);
        return executeRequest(request, HealthCheckResponse.class);
    }

    public Collection createCollection(CreateCollectionRequest request) throws IOException {
        String url = getBaseUrl() + "/api/collections";
        HttpPost httpPost = new HttpPost(url);
        String json = objectMapper.writeValueAsString(request);
        httpPost.setEntity(new StringEntity(json, StandardCharsets.UTF_8));
        return executeRequest(httpPost, Collection.class);
    }

    public Collection createCollection(String name) throws IOException {
        CreateCollectionRequest request = new CreateCollectionRequest();
        request.setName(name);
        return createCollection(request);
    }

    public List<String> listCollections() throws IOException {
        String url = getBaseUrl() + "/api/collections";
        HttpGet request = new HttpGet(url);

        class ListCollectionsResponse {
            public List<String> collections;
        }

        ListCollectionsResponse response = executeRequest(request, ListCollectionsResponse.class);
        return response.collections;
    }

    public Collection getCollection(String name) throws IOException {
        String url = String.format("%s/api/collections/%s", getBaseUrl(), encodePathParam(name));
        HttpGet request = new HttpGet(url);
        return executeRequest(request, Collection.class);
    }

    public void deleteCollection(String name) throws IOException {
        String url = String.format("%s/api/collections/%s", getBaseUrl(), encodePathParam(name));
        HttpDelete request = new HttpDelete(url);
        executeRequest(request);
    }

    public InsertResponse insertDocuments(String collection, List<Document> documents) throws IOException {
        return insertDocuments(collection, documents, false);
    }

    public InsertResponse insertDocuments(String collection, List<Document> documents, boolean overwrite) throws IOException {
        String url = String.format("%s/api/collections/%s/documents", getBaseUrl(), encodePathParam(collection));
        HttpPost request = new HttpPost(url);

        InsertDocumentsRequest insertRequest = new InsertDocumentsRequest();
        insertRequest.setDocuments(documents);
        insertRequest.setOverwrite(overwrite);

        String json = objectMapper.writeValueAsString(insertRequest);
        request.setEntity(new StringEntity(json, StandardCharsets.UTF_8));
        return executeRequest(request, InsertResponse.class);
    }

    public Document getDocument(String collection, String id) throws IOException {
        String url = String.format("%s/api/collections/%s/documents/%s",
                getBaseUrl(), encodePathParam(collection), encodePathParam(id));
        HttpGet request = new HttpGet(url);
        return executeRequest(request, Document.class);
    }

    public void deleteDocument(String collection, String id) throws IOException {
        String url = String.format("%s/api/collections/%s/documents/%s",
                getBaseUrl(), encodePathParam(collection), encodePathParam(id));
        HttpDelete request = new HttpDelete(url);
        executeRequest(request);
    }

    public SearchResponse vectorSearch(String collection, VectorSearchRequest request) throws IOException {
        String url = String.format("%s/api/collections/%s/search", getBaseUrl(), encodePathParam(collection));
        HttpPost httpPost = new HttpPost(url);

        String json = objectMapper.writeValueAsString(request);
        httpPost.setEntity(new StringEntity(json, StandardCharsets.UTF_8));
        return executeRequest(httpPost, SearchResponse.class);
    }

    public SearchResponse vectorSearch(String collection, List<Float> query) throws IOException {
        return vectorSearch(collection, query, 10);
    }

    public SearchResponse vectorSearch(String collection, List<Float> query, int limit) throws IOException {
        VectorSearchRequest request = new VectorSearchRequest();
        request.setQuery(query);
        request.setLimit(limit);
        request.setIncludeDocuments(true);
        return vectorSearch(collection, request);
    }

    public void createIndex(String collection, String indexName, String indexType, Map<String, Object> config) throws IOException {
        String url = String.format("%s/api/collections/%s/indexes", getBaseUrl(), encodePathParam(collection));
        HttpPost request = new HttpPost(url);

        CreateIndexRequest indexRequest = new CreateIndexRequest();
        indexRequest.setName(indexName);
        indexRequest.setType(indexType);
        indexRequest.setConfig(config);

        String json = objectMapper.writeValueAsString(indexRequest);
        request.setEntity(new StringEntity(json, StandardCharsets.UTF_8));
        executeRequest(request);
    }

    public List<IndexInfo> listIndexes(String collection) throws IOException {
        String url = String.format("%s/api/collections/%s/indexes", getBaseUrl(), encodePathParam(collection));
        HttpGet request = new HttpGet(url);

        class ListIndexesResponse {
            public List<IndexInfo> indexes;
        }

        ListIndexesResponse response = executeRequest(request, ListIndexesResponse.class);
        return response.indexes;
    }

    public SystemStats systemStats() throws IOException {
        String url = getBaseUrl() + "/api/system/stats";
        HttpGet request = new HttpGet(url);
        return executeRequest(request, SystemStats.class);
    }

    private String encodePathParam(String param) {
        try {
            return java.net.URLEncoder.encode(param, StandardCharsets.UTF_8.toString());
        } catch (java.io.UnsupportedEncodingException e) {
            return param;
        }
    }
}
