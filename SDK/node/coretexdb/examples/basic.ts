import { CoretexDBClient } from './src/index';

async function main() {
  const client = new CoretexDBClient({
    host: 'localhost',
    port: 8080,
  });

  try {
    const health = await client.healthCheck();
    console.log('Health check:', health.status);

    const collections = await client.listCollections();
    console.log('Collections:', collections);

    const collection = await client.createCollection('test_collection');
    console.log('Created collection:', collection.name);

    const documents = [
      {
        id: 'doc1',
        vector: [1.0, 2.0, 3.0],
        metadata: { text: 'Hello world' },
      },
      {
        id: 'doc2',
        vector: [4.0, 5.0, 6.0],
        metadata: { text: 'Goodbye world' },
      },
    ];

    const insertResponse = await client.insertDocuments('test_collection', documents);
    console.log('Inserted:', insertResponse.inserted);

    const searchResponse = await client.vectorSearch('test_collection', [1.0, 2.0, 3.0], 10);
    console.log('Search results:', searchResponse.results.length);

    await client.deleteCollection('test_collection');
    console.log('Deleted test_collection');
  } catch (error) {
    console.error('Error:', error);
  }
}

main();
