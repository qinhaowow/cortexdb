# CortexDB Community Edition

CortexDB Community Edition is an open-source, high-performance vector database designed for AI applications. This edition provides core vector search functionality with a focus on simplicity, performance, and ease of use.

## Features

### Core Vector Database
- **Vector Storage**: Efficient storage and management of high-dimensional vectors
- **Similarity Search**: Fast nearest neighbor search with multiple distance metrics (cosine, euclidean, dot product)
- **Scalar Indexing**: Hybrid search combining vector and metadata filtering
- **HNSW Index**: Hierarchical Navigable Small World index for high-performance approximate nearest neighbor search
- **Incremental Indexing**: Support for dynamic vector insertions without full index rebuilding

### AI-Powered Features
- **Auto-Tuning**: Automatic index parameter optimization based on data characteristics
- **Embedding Management**: Built-in embedding generation and management
- **Expression Evaluation**: Support for complex scalar expressions in queries
- **Performance Optimization**: JIT compilation for query optimization
- **Model Repository**: Management and versioning of ML models
- **Intelligent Features**: Smart caching and query optimization

### API Support
- **REST API**: Full-featured RESTful API for all database operations
- **gRPC Protocol**: High-performance gRPC interface (requires `grpc` feature)
- **PostgreSQL Protocol**: Wire-compatible PostgreSQL protocol support (requires `postgres` feature)

### Storage & Reliability
- **Memory Storage**: In-memory storage for maximum performance
- **Persistent Storage**: Persistent storage with RocksDB backend
- **Data Compression**: Support for LZ4, Snappy, and Zstandard compression
- **Transaction Support**: MVCC-based transaction support with WAL logging

### Developer Experience
- **Multiple SDKs**: Official SDKs for Python, Go, Node.js, C++, and Java
- **LangChain Integration**: Seamless integration with LangChain for building AI applications
- **HuggingFace Integration**: Support for HuggingFace models and datasets
- **OpenAI Integration**: Easy integration with OpenAI embeddings

## Quick Start

### Building from Source

```bash
# Clone the repository
git clone https://github.com/cortexdb/cortexdb.git
cd cortexdb

# Build the project
cargo build --release

# Run the server
./target/release/cortex --help
```

### Using Docker

```bash
# Pull and run the latest community edition
docker pull coretexdb/cortexdb:latest
docker run -p 8080:8080 coretexdb/cortexdb:latest
```

### Basic Usage

```rust
use cortexdb::{CortexDB, Vector, DistanceMetric};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = CortexDB::new().await?;

    // Create a collection
    let collection = db.create_collection("documents", 768, DistanceMetric::Cosine).await?;

    // Insert vectors
    let vectors: Vec<Vector> = vec![
        Vector::new(vec![0.1; 768]),
        Vector::new(vec![0.2; 768]),
        Vector::new(vec![0.3; 768]),
    ];

    for (i, vector) in vectors.iter().enumerate() {
        db.store(&format!("doc_{}", i), vector, None).await?;
    }

    // Search for similar vectors
    let query = Vector::new(vec![0.15; 768]);
    let results = db.search(&query, 3).await?;

    for result in results {
        println!("ID: {}, Score: {}", result.id, result.score);
    }

    Ok(())
}
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `CORTEXDB_HOST` | Server host | `0.0.0.0` |
| `CORTEXDB_PORT` | Server port | `8080` |
| `CORTEXDB_STORAGE` | Storage engine (`memory`, `persistent`) | `memory` |
| `CORTEXDB_DATA_DIR` | Data directory for persistent storage | `./data` |

### Configuration File

Create a `cortexdb.toml` configuration file:

```toml
[storage]
engine = "persistent"
directory = "./data"
enable_compression = true
compression_algorithm = "lz4"

[api]
host = "0.0.0.0"
port = 8080
workers = 4
cors_enabled = true

[security]
authentication_enabled = false
authorization_enabled = false
```

## SDKs and Integrations

### Python SDK

```bash
pip install cortexdb
```

```python
import cortexdb
import numpy as np

# Initialize client
client = cortexdb.CortexDBClient(host="localhost", port=8080)

# Create collection
client.create_collection("documents", dimension=768, metric="cosine")

# Insert vectors
vectors = np.random.rand(100, 768).astype(np.float32)
client.insert("documents", vectors)

# Search
query = np.random.rand(768).astype(np.float32)
results = client.search("documents", query, k=5)
```

### LangChain Integration

```python
from langchain.vectorstores import CortexDB
from langchain.embeddings import OpenAIEmbeddings

vectorstore = CortexDB(
    embedding_function=OpenAIEmbeddings(),
    collection_name="documents",
    host="localhost",
    port=8080
)
```

## API Reference

### REST API Endpoints

#### Collections
- `POST /collections` - Create a new collection
- `GET /collections` - List all collections
- `GET /collections/:name` - Get collection details
- `DELETE /collections/:name` - Delete a collection

#### Vectors
- `POST /collections/:name/insert` - Insert vectors
- `POST /collections/:name/search` - Search vectors
- `POST /collections/:name/query` - Scalar query with filtering
- `DELETE /collections/:name/vectors` - Delete vectors

### gRPC API

Full gRPC API documentation available in the [protocol buffer definitions](src/coretex_api/grpc/proto/coretexdb.proto).

## Architecture

```
CortexDB Community Edition
├── Core Engine
│   ├── Vector Index (HNSW, Brute Force)
│   ├── Scalar Index (BTree, Hash)
│   ├── Storage Layer (Memory, Persistent)
│   └── Transaction Manager (MVCC, WAL)
├── API Layer
│   ├── REST API (Axum)
│   ├── gRPC (Tonic)
│   └── PostgreSQL Protocol
├── AI Features
│   ├── Auto-Tuning
│   ├── Embedding Generation
│   └── Query Optimization
└── Utilities
    ├── CLI Tools
    ├── Monitoring
    └── Backup/Restore
```

## Feature Comparison

| Feature | Community | Enterprise |
|---------|-----------|-------------|
| Vector Search | ✅ | ✅ |
| HNSW Index | ✅ | ✅ |
| Scalar Index | ✅ | ✅ |
| REST API | ✅ | ✅ |
| Python SDK | ✅ | ✅ |
| gRPC API | ✅ | ✅ |
| **Hybrid Search** | ❌ | ✅ |
| **Privacy Computing** | ❌ | ✅ |
| **Edge Computing** | ❌ | ✅ |
| **Federated Learning** | ❌ | ✅ |
| **Distributed Cluster** | Limited | ✅ |
| **Enterprise Support** | ❌ | ✅ |
| **SLA Guarantee** | ❌ | ✅ |

## Performance

CortexDB Community Edition is optimized for single-node performance:

- **Insertion**: ~100K vectors/second
- **Search**: ~10K queries/second (1M vectors, HNSW ef=100)
- **Memory Usage**: ~1.2x vector data size
- **Latency**: <1ms p99 for small queries

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details.

## License

Apache License 2.0 - see [LICENSE](LICENSE) file for details.

## Community

- **GitHub Issues**: Report bugs and request features
- **Discussions**: Ask questions and share ideas
- **Wiki**: Check for tutorials and guides

## Resources

- [Documentation](https://docs.cortexdb.io)
- [API Reference](https://docs.cortexdb.io/api)
- [Examples](examples/)
- [Performance Benchmarks](benchmarks/)
