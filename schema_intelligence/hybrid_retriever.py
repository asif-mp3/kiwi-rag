from chromadb.errors import NotFoundError
from schema_intelligence.chromadb_client import SchemaVectorStore


def retrieve_schema(query: str, top_k: int = 5):
    """
    Retrieve relevant schema blocks for a user query.
    Auto-builds schema store if missing.
    """

    store = SchemaVectorStore()

    try:
        collection = store.client.get_collection(store.collection_name)
    except NotFoundError:
        # Cold start: build schema embeddings
        store.rebuild()
        collection = store.client.get_collection(store.collection_name)

    results = collection.query(
        query_texts=[query],
        n_results=top_k
    )

    documents = results.get("documents", [[]])[0]
    metadatas = results.get("metadatas", [[]])[0]

    schema_context = []
    for doc, meta in zip(documents, metadatas):
        schema_context.append({
            "text": doc,
            "metadata": meta
        })

    return schema_context
