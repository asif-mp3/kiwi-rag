"""
ChromaDB client for schema-level semantic retrieval.

CRITICAL: This module uses Hugging Face embeddings ONLY.
ONNX embeddings are explicitly disabled to ensure Streamlit compatibility on Windows.

Embedding Model: sentence-transformers/all-MiniLM-L6-v2
- No API key required (uses local cached model)
- No Visual C++ dependencies
- No ONNX runtime required
- Streamlit-safe and Windows-safe
"""

import chromadb
from chromadb.config import Settings
from chromadb.api.types import EmbeddingFunction
from schema_intelligence.embedding_builder import build_schema_documents
from typing import List
import os


class CustomSentenceTransformerEmbedding(EmbeddingFunction):
    """
    Custom embedding function using sentence-transformers directly.
    This avoids ChromaDB's built-in wrapper which has PyTorch compatibility issues.
    """
    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
        try:
            from sentence_transformers import SentenceTransformer
            import torch
            
            # Set environment variable to avoid tokenizers parallelism warning
            os.environ["TOKENIZERS_PARALLELISM"] = "false"
            
            # Load model with explicit device configuration
            self.model = SentenceTransformer(model_name, device='cpu')
        except Exception as e:
            raise RuntimeError(
                f"Failed to initialize SentenceTransformer model. "
                f"Error: {e}"
            )
    
    def __call__(self, input: List[str]) -> List[List[float]]:
        """Generate embeddings for input texts."""
        embeddings = self.model.encode(input, convert_to_numpy=True)
        return embeddings.tolist()


class SchemaVectorStore:
    """
    Vector store for schema-level semantic search.
    
    IMPORTANT: This class explicitly uses Hugging Face embeddings to avoid ONNX.
    ChromaDB's default embedding function (ONNX) causes DLL errors on Windows/Streamlit.
    """
    
    def __init__(self, persist_dir="schema_store"):
        """
        Initialize ChromaDB with explicit Hugging Face embeddings.
        
        GUARDRAIL: This constructor NEVER allows ChromaDB to use default embeddings.
        If embedding initialization fails, the system will fail fast with a clear error.
        """
        # Use PersistentClient for proper disk persistence with settings
        settings = Settings(
            allow_reset=True,
            is_persistent=True
        )
        self.client = chromadb.PersistentClient(path=persist_dir, settings=settings)
        self.collection_name = "schema"
        
        # CRITICAL: Create Hugging Face embedding function explicitly
        # This prevents ChromaDB from defaulting to ONNX embeddings
        try:
            # Use our custom embedding function to avoid PyTorch meta tensor issues
            self.embedding_function = CustomSentenceTransformerEmbedding()
        except Exception as e:
            raise RuntimeError(
                f"Failed to initialize Hugging Face embeddings. "
                f"ONNX embeddings are disabled by design. "
                f"Ensure sentence-transformers is installed: pip install sentence-transformers\n"
                f"Error: {e}"
            )
        
        # Verify we're not using ONNX (runtime guardrail)
        embedding_type = type(self.embedding_function).__name__
        if "onnx" in embedding_type.lower():
            raise RuntimeError(
                f"ONNX embeddings detected ({embedding_type}). "
                f"This is not allowed. The system must use Hugging Face embeddings only."
            )
    
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
        
        IMPORTANT: Always passes explicit embedding function to prevent ONNX fallback.
        """

        # Delete existing collection if present
        try:
            self.client.delete_collection(self.collection_name)
        except Exception:
            pass  # Collection may not exist yet

        # Create fresh collection WITH EXPLICIT EMBEDDING FUNCTION
        # This is critical - never allow ChromaDB to use default embeddings
        self.collection = self.client.create_collection(
            name=self.collection_name,
            embedding_function=self.embedding_function  # EXPLICIT: No ONNX fallback
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
        # Embeddings are generated using Hugging Face model (all-MiniLM-L6-v2)
        self.collection.add(
            ids=[doc["id"] for doc in documents],
            documents=[doc["text"] for doc in documents],
            metadatas=metadatas
        )

    def count(self):
        """Get the number of documents in the collection."""
        # Get collection WITH EXPLICIT EMBEDDING FUNCTION
        collection = self.client.get_collection(
            name=self.collection_name,
            embedding_function=self.embedding_function  # EXPLICIT: No ONNX fallback
        )
        return collection.count()
