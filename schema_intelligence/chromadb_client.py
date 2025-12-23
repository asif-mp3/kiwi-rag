import chromadb
from chromadb.config import Settings
from schema_intelligence.embedding_builder import build_schema_documents


class SchemaVectorStore:
    def __init__(self, persist_dir="schema_store"):
        # Use PersistentClient for proper disk persistence
        self.client = chromadb.PersistentClient(path=persist_dir)
        self.collection_name = "schema"
    
    def clear_collection(self):
        """
        Clear all schema embeddings from the collection.
        Used during full reset to remove old schema references.
        """
        try:
            # Delete the collection
            self.client.delete_collection(self.collection_name)
            print(f"   Deleted ChromaDB collection: {self.collection_name}")
        except Exception as e:
            # Collection may not exist
            print(f"   ChromaDB collection doesn't exist (first run or already cleared)")

    def rebuild(self):
        """
        Rebuild schema vector store from scratch.
        Safe, deterministic, idempotent.
        """

        # Delete existing collection if present
        try:
            self.client.delete_collection(self.collection_name)
        except Exception:
            pass  # Collection may not exist yet

        # Create fresh collection
        self.collection = self.client.create_collection(
            name=self.collection_name
        )

        documents = build_schema_documents()

        # Build clean metadata (NO None values)
        metadatas = []
        for doc in documents:
            meta = {"type": doc["type"]}

            if doc.get("table") is not None:
                meta["table"] = doc["table"]

            if doc.get("metric") is not None:
                meta["metric"] = doc["metric"]

            metadatas.append(meta)

        # Add embeddings (auto-persisted by Chroma)
        self.collection.add(
            ids=[doc["id"] for doc in documents],
            documents=[doc["text"] for doc in documents],
            metadatas=metadatas
        )

    def count(self):
        collection = self.client.get_collection(self.collection_name)
        return collection.count()
