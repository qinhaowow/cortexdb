package com.coretexdb.models;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public class MetricType {
    public static final String COSINE = "cosine";
    public static final String EUCLIDEAN = "euclidean";
    public static final String DOT_PRODUCT = "dotproduct";
}

public class IndexType {
    public static final String HNSW = "hnsw";
    public static final String IVF = "ivf";
    public static final String SCALAR = "scalar";
}

public class CollectionSchema {
    private List<FieldSchema> fields;

    public List<FieldSchema> getFields() {
        return fields;
    }

    public void setFields(List<FieldSchema> fields) {
        this.fields = fields;
    }
}

public class FieldSchema {
    private String name;
    private String dataType;
    private boolean required;
    private boolean index;
    private Map<String, Object> options;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }

    public boolean isIndex() {
        return index;
    }

    public void setIndex(boolean index) {
        this.index = index;
    }

    public Map<String, Object> getOptions() {
        return options;
    }

    public void setOptions(Map<String, Object> options) {
        this.options = options;
    }
}

public class Collection {
    private String name;
    private CollectionSchema schema;
    private String description;
    private Map<String, Object> settings;
    private int count;
    private Instant createdAt;
    private Instant updatedAt;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public CollectionSchema getSchema() {
        return schema;
    }

    public void setSchema(CollectionSchema schema) {
        this.schema = schema;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String, Object> getSettings() {
        return settings;
    }

    public void setSettings(Map<String, Object> settings) {
        this.settings = settings;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}

public class CreateCollectionRequest {
    private String name;
    private CollectionSchema schema;
    private String description;
    private Map<String, Object> settings;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public CollectionSchema getSchema() {
        return schema;
    }

    public void setSchema(CollectionSchema schema) {
        this.schema = schema;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String, Object> getSettings() {
        return settings;
    }

    public void setSettings(Map<String, Object> settings) {
        this.settings = settings;
    }
}

public class Document {
    private String id;
    private List<Float> vector;
    private Map<String, Object> metadata;
    private float score;
    private Instant createdAt;
    private Instant updatedAt;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<Float> getVector() {
        return vector;
    }

    public void setVector(List<Float> vector) {
        this.vector = vector;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    public float getScore() {
        return score;
    }

    public void setScore(float score) {
        this.score = score;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}

public class InsertDocumentsRequest {
    private List<Document> documents;
    private boolean overwrite;

    public List<Document> getDocuments() {
        return documents;
    }

    public void setDocuments(List<Document> documents) {
        this.documents = documents;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }
}

public class InsertResponse {
    private int inserted;
    private List<String> ids;
    private double timeMs;

    public int getInserted() {
        return inserted;
    }

    public void setInserted(int inserted) {
        this.inserted = inserted;
    }

    public List<String> getIds() {
        return ids;
    }

    public void setIds(List<String> ids) {
        this.ids = ids;
    }

    public double getTimeMs() {
        return timeMs;
    }

    public void setTimeMs(double timeMs) {
        this.timeMs = timeMs;
    }
}

public class VectorSearchRequest {
    private List<Float> query;
    private int limit;
    private float threshold;
    private boolean includeDocuments;
    private Map<String, Object> filter;
    private Map<String, Object> params;

    public List<Float> getQuery() {
        return query;
    }

    public void setQuery(List<Float> query) {
        this.query = query;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public float getThreshold() {
        return threshold;
    }

    public void setThreshold(float threshold) {
        this.threshold = threshold;
    }

    public boolean isIncludeDocuments() {
        return includeDocuments;
    }

    public void setIncludeDocuments(boolean includeDocuments) {
        this.includeDocuments = includeDocuments;
    }

    public Map<String, Object> getFilter() {
        return filter;
    }

    public void setFilter(Map<String, Object> filter) {
        this.filter = filter;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }
}

public class SearchResult {
    private String id;
    private float score;
    private float distance;
    private List<Float> vector;
    private Map<String, Object> metadata;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public float getScore() {
        return score;
    }

    public void setScore(float score) {
        this.score = score;
    }

    public float getDistance() {
        return distance;
    }

    public void setDistance(float distance) {
        this.distance = distance;
    }

    public List<Float> getVector() {
        return vector;
    }

    public void setVector(List<Float> vector) {
        this.vector = vector;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }
}

public class SearchResponse {
    private List<SearchResult> results;
    private int count;
    private double executionTimeMs;
    private double timeMs;

    public List<SearchResult> getResults() {
        return results;
    }

    public void setResults(List<SearchResult> results) {
        this.results = results;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public double getExecutionTimeMs() {
        return executionTimeMs;
    }

    public void setExecutionTimeMs(double executionTimeMs) {
        this.executionTimeMs = executionTimeMs;
    }

    public double getTimeMs() {
        return timeMs;
    }

    public void setTimeMs(double timeMs) {
        this.timeMs = timeMs;
    }
}

public class CreateIndexRequest {
    private String name;
    private String type;
    private Map<String, Object> config;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }
}

public class IndexInfo {
    private String name;
    private String type;
    private Map<String, Object> config;
    private String status;
    private int docCount;
    private Instant createdAt;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public int getDocCount() {
        return docCount;
    }

    public void setDocCount(int docCount) {
        this.docCount = docCount;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }
}

public class HealthCheckResponse {
    private String status;
    private String version;
    private String timestamp;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}

public class SystemStats {
    private StorageStats storage;
    private IndexStats indexes;
    private SystemInfo system;

    public StorageStats getStorage() {
        return storage;
    }

    public void setStorage(StorageStats storage) {
        this.storage = storage;
    }

    public IndexStats getIndexes() {
        return indexes;
    }

    public void setIndexes(IndexStats indexes) {
        this.indexes = indexes;
    }

    public SystemInfo getSystem() {
        return system;
    }

    public void setSystem(SystemInfo system) {
        this.system = system;
    }
}

public class StorageStats {
    private String type;
    private int collections;
    private int documents;
    private long usedBytes;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getCollections() {
        return collections;
    }

    public void setCollections(int collections) {
        this.collections = collections;
    }

    public int getDocuments() {
        return documents;
    }

    public void setDocuments(int documents) {
        this.documents = documents;
    }

    public long getUsedBytes() {
        return usedBytes;
    }

    public void setUsedBytes(long usedBytes) {
        this.usedBytes = usedBytes;
    }
}

public class IndexStats {
    private int count;
    private int vectorIndexes;
    private int scalarIndexes;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getVectorIndexes() {
        return vectorIndexes;
    }

    public void setVectorIndexes(int vectorIndexes) {
        this.vectorIndexes = vectorIndexes;
    }

    public int getScalarIndexes() {
        return scalarIndexes;
    }

    public void setScalarIndexes(int scalarIndexes) {
        this.scalarIndexes = scalarIndexes;
    }
}

public class SystemInfo {
    private String rustVersion;
    private String version;
    private String timestamp;

    public String getRustVersion() {
        return rustVersion;
    }

    public void setRustVersion(String rustVersion) {
        this.rustVersion = rustVersion;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}

public class APIError extends RuntimeException {
    private int statusCode;
    private String message;

    public APIError(int statusCode, String message) {
        super(message);
        this.statusCode = statusCode;
        this.message = message;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
