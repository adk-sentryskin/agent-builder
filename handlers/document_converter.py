"""Document converter to NDJSON format for Vertex AI Search + pgvector embeddings"""

import os
import json
import re
import base64
import logging
from typing import List, Dict, Any, Optional
from io import BytesIO
import PyPDF2
from docx import Document
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Vertex AI embedding model (lazy-loaded)
_embedding_model = None
_aiplatform_initialized = False


class DocumentConverter:
    """Convert documents to NDJSON format for Vertex AI Search"""

    def __init__(self, gcs_handler):
        """
        Initialize document converter

        Args:
            gcs_handler: GCSHandler instance
        """
        self.gcs_handler = gcs_handler

    def convert_documents(
        self,
        merchant_id: str,
        document_paths: List[str]
    ) -> Dict[str, Any]:
        """
        Convert multiple documents to NDJSON format

        Args:
            merchant_id: Merchant identifier
            document_paths: List of GCS paths to documents

        Returns:
            dict with path to generated NDJSON file and document count
        """
        try:
            all_documents = []
            skipped_files = []

            for doc_path in document_paths:
                # Validate file exists before processing
                if not self.gcs_handler.file_exists(doc_path):
                    logger.warning(f"File does not exist, skipping: {doc_path}")
                    skipped_files.append(doc_path)
                    continue
                
                try:
                    logger.info(f"Converting document: {doc_path}")
                    documents = self._convert_single_document(doc_path)
                    all_documents.extend(documents)
                except Exception as e:
                    logger.error(f"Error converting document {doc_path}: {e}")
                    skipped_files.append(doc_path)
                    # Continue with other files instead of failing completely
                    continue

            # Create NDJSON content
            ndjson_content = self._create_ndjson(all_documents)

            # Only upload if we have documents to convert
            if not all_documents:
                logger.warning("No documents were successfully converted")
                return {
                    "ndjson_path": None,
                    "document_count": 0,
                    "skipped_files": skipped_files
                }

            # Upload to GCS
            ndjson_path = f"merchants/{merchant_id}/training_files/documents.ndjson"
            self.gcs_handler.upload_file(
                ndjson_path,
                ndjson_content.encode('utf-8'),
                content_type="application/x-ndjson"
            )

            logger.info(f"Converted {len(all_documents)} documents to NDJSON: {ndjson_path}")
            if skipped_files:
                logger.warning(f"Skipped {len(skipped_files)} files: {skipped_files}")

            # Generate embeddings and store in document_chunks table for chatbot RAG
            chunks_stored = self._store_document_embeddings(merchant_id, all_documents)

            return {
                "ndjson_path": ndjson_path,
                "document_count": len(all_documents),
                "chunks_stored": chunks_stored,
                "skipped_files": skipped_files if skipped_files else None
            }

        except Exception as e:
            logger.error(f"Error converting documents: {e}")
            raise

    def _convert_single_document(self, doc_path: str) -> List[Dict[str, Any]]:
        """
        Convert a single document to Vertex AI Search format

        Args:
            doc_path: GCS path to document

        Returns:
            List of document dictionaries
        """
        # Download document
        file_content = self.gcs_handler.download_file(doc_path)
        filename = os.path.basename(doc_path)

        # Determine file type and extract text
        if doc_path.endswith('.pdf'):
            text_content = self._extract_pdf_text(file_content)
        elif doc_path.endswith('.docx'):
            text_content = self._extract_docx_text(file_content)
        elif doc_path.endswith('.txt'):
            text_content = file_content.decode('utf-8', errors='ignore')
        elif doc_path.endswith('.html') or doc_path.endswith('.htm'):
            text_content = self._extract_html_text(file_content)
        else:
            logger.warning(f"Unsupported file type: {doc_path}, treating as text")
            text_content = file_content.decode('utf-8', errors='ignore')

        # Split into chunks for RAG retrieval
        # 1000 chars ≈ 250 tokens — optimal for embedding + retrieval accuracy
        # 200 char overlap ensures context isn't lost at chunk boundaries
        max_chunk_size = 1000  # characters per chunk
        overlap = 200  # characters of overlap between chunks
        chunks = self._split_text(text_content, max_chunk_size, overlap)

        documents = []
        for i, chunk in enumerate(chunks):
            # Create title
            doc_title = filename if i == 0 else f"{filename} (Part {i + 1})"
            
            # Create document ID - sanitize to match pattern [a-zA-Z0-9-_]*
            # Remove file extension and replace invalid characters with hyphens
            base_name = os.path.splitext(filename)[0]  # Remove extension
            original_id = f"{base_name}_{i}"
            # Sanitize: replace any character not in [a-zA-Z0-9-_] with hyphen
            sanitized_id = re.sub(r'[^a-zA-Z0-9-_]', '-', original_id)
            # Replace multiple consecutive hyphens with single hyphen
            sanitized_id = re.sub(r'-+', '-', sanitized_id)
            # Remove leading/trailing hyphens
            sanitized_id = sanitized_id.strip('-')
            # Ensure ID is not empty
            if not sanitized_id:
                sanitized_id = f"doc-{i}"
            
            # Build struct_data (title should be in struct_data, not at top level)
            struct_data = {
                "title": doc_title,
                "source": doc_path,
                "filename": filename,
                "chunk_index": i,
                "total_chunks": len(chunks)
            }
            
            # Encode content as base64 (matching working script format)
            content_bytes = chunk.encode('utf-8')
            content_base64 = base64.b64encode(content_bytes).decode('utf-8')
            
            # Create Vertex AI Search document format (matching working script)
            doc = {
                "id": sanitized_id,
                "content": {
                    "mime_type": "text/plain",
                    "raw_bytes": content_base64
                },
                "struct_data": struct_data
            }
            documents.append(doc)

        return documents

    def _extract_pdf_text(self, file_content: bytes) -> str:
        """Extract text from PDF"""
        try:
            pdf_file = BytesIO(file_content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            text_parts = []

            for page in pdf_reader.pages:
                text_parts.append(page.extract_text())

            return '\n\n'.join(text_parts)
        except Exception as e:
            logger.error(f"Error extracting PDF text: {e}")
            raise

    def _extract_docx_text(self, file_content: bytes) -> str:
        """Extract text from DOCX"""
        try:
            docx_file = BytesIO(file_content)
            doc = Document(docx_file)
            text_parts = []

            for paragraph in doc.paragraphs:
                if paragraph.text.strip():
                    text_parts.append(paragraph.text)

            return '\n\n'.join(text_parts)
        except Exception as e:
            logger.error(f"Error extracting DOCX text: {e}")
            raise

    def _extract_html_text(self, file_content: bytes) -> str:
        """Extract text from HTML"""
        try:
            html_content = file_content.decode('utf-8', errors='ignore')
            soup = BeautifulSoup(html_content, 'html.parser')

            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()

            # Get text
            text = soup.get_text()

            # Clean up whitespace
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = '\n'.join(chunk for chunk in chunks if chunk)

            return text
        except Exception as e:
            logger.error(f"Error extracting HTML text: {e}")
            raise

    def _split_text(self, text: str, max_size: int, overlap: int = 0) -> List[str]:
        """
        Split text into chunks with optional overlap.

        Strategy: split on paragraph boundaries first, then sentence boundaries.
        Overlap is added by prepending the tail of the previous chunk to the next.

        Args:
            text: Text to split
            max_size: Maximum chunk size in characters
            overlap: Number of characters to overlap between consecutive chunks

        Returns:
            List of text chunks
        """
        if len(text) <= max_size:
            return [text]

        # First pass: split into segments respecting paragraph/sentence boundaries
        segments = []
        current_chunk = ""

        paragraphs = text.split('\n\n')

        for paragraph in paragraphs:
            if len(current_chunk) + len(paragraph) + 2 <= max_size:
                current_chunk += paragraph + '\n\n'
            else:
                if current_chunk:
                    segments.append(current_chunk.strip())
                # If paragraph itself is too large, split by sentences
                if len(paragraph) > max_size:
                    sentences = paragraph.split('. ')
                    current_chunk = ""
                    for sentence in sentences:
                        if len(current_chunk) + len(sentence) + 2 <= max_size:
                            current_chunk += sentence + '. '
                        else:
                            if current_chunk:
                                segments.append(current_chunk.strip())
                            current_chunk = sentence + '. '
                else:
                    current_chunk = paragraph + '\n\n'

        if current_chunk:
            segments.append(current_chunk.strip())

        # Second pass: add overlap between chunks
        if overlap <= 0 or len(segments) <= 1:
            return segments

        chunks = [segments[0]]
        for i in range(1, len(segments)):
            prev = segments[i - 1]
            # Take the last `overlap` characters from the previous segment
            overlap_text = prev[-overlap:] if len(prev) > overlap else prev
            # Trim to start at a word boundary (don't cut mid-word)
            space_idx = overlap_text.find(' ')
            if space_idx > 0:
                overlap_text = overlap_text[space_idx + 1:]
            chunk = overlap_text + " " + segments[i]
            # Ensure we don't exceed max_size after adding overlap
            if len(chunk) > max_size:
                chunk = chunk[:max_size]
            chunks.append(chunk.strip())

        return chunks

    def _get_embedding_model(self):
        """Get or create the Vertex AI text embedding model (cached)."""
        global _embedding_model, _aiplatform_initialized

        if not _aiplatform_initialized:
            from google.cloud import aiplatform
            project_id = os.getenv("GCP_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT", "shopify-473015")
            region = os.getenv("GCP_REGION", "us-central1")
            aiplatform.init(project=project_id, location=region)
            _aiplatform_initialized = True

        if _embedding_model is None:
            from vertexai.language_models import TextEmbeddingModel
            _embedding_model = TextEmbeddingModel.from_pretrained("text-embedding-004")
            logger.info("Loaded text-embedding-004 model for document embeddings")

        return _embedding_model

    def _generate_embeddings_batch(self, texts: List[str]) -> List[Optional[List[float]]]:
        """
        Generate embeddings for a batch of texts using Vertex AI.

        Args:
            texts: List of text strings to embed (max 250 per batch per API limit)

        Returns:
            List of embedding vectors (768 dimensions each), or None for failed items
        """
        try:
            from vertexai.language_models import TextEmbeddingInput
            model = self._get_embedding_model()

            # Truncate texts to 20K chars (model limit) and create inputs
            inputs = [
                TextEmbeddingInput(text=t[:20000], task_type="RETRIEVAL_DOCUMENT")
                for t in texts
            ]

            embeddings_result = model.get_embeddings(inputs)
            return [e.values for e in embeddings_result]

        except Exception as e:
            logger.error(f"Embedding generation failed for batch of {len(texts)}: {e}")
            return [None] * len(texts)

    def _store_document_embeddings(self, merchant_id: str, documents: List[Dict[str, Any]]) -> int:
        """
        Generate embeddings for document chunks and store in document_chunks table.
        Deletes existing chunks for the merchant first (full refresh).

        Args:
            merchant_id: Merchant identifier
            documents: List of Vertex AI Search formatted documents (with content.raw_bytes)

        Returns:
            Number of chunks successfully stored
        """
        from utils.db_helpers import get_connection, return_connection

        conn = None
        stored = 0

        try:
            conn = get_connection()
            cursor = conn.cursor()

            # Delete existing chunks for this merchant (full refresh)
            cursor.execute(
                "DELETE FROM public.document_chunks WHERE merchant_id = %s",
                (merchant_id,)
            )
            deleted = cursor.rowcount
            if deleted > 0:
                logger.info(f"Deleted {deleted} existing document chunks for merchant {merchant_id}")

            # Extract plain text content from each document
            chunk_data = []
            for doc in documents:
                # Decode base64 content back to text
                raw_bytes = doc.get("content", {}).get("raw_bytes", "")
                try:
                    content = base64.b64decode(raw_bytes).decode("utf-8")
                except Exception:
                    content = ""

                if not content.strip():
                    continue

                struct = doc.get("struct_data", {})
                chunk_data.append({
                    "content": content,
                    "title": struct.get("title", ""),
                    "source": struct.get("source", ""),
                    "chunk_index": struct.get("chunk_index", 0),
                    "total_chunks": struct.get("total_chunks", 1),
                })

            if not chunk_data:
                logger.warning(f"No valid chunks to embed for merchant {merchant_id}")
                conn.commit()
                return 0

            # Generate embeddings in batches of 25
            batch_size = 25
            for i in range(0, len(chunk_data), batch_size):
                batch = chunk_data[i:i + batch_size]
                texts = [c["content"] for c in batch]

                logger.info(
                    f"Generating embeddings for chunks {i+1}-{i+len(batch)} "
                    f"of {len(chunk_data)} for merchant {merchant_id}"
                )

                embeddings = self._generate_embeddings_batch(texts)

                for chunk, embedding in zip(batch, embeddings):
                    if embedding is None:
                        logger.warning(f"Skipping chunk '{chunk['title']}' — embedding failed")
                        continue

                    embedding_str = "[" + ",".join(map(str, embedding)) + "]"

                    cursor.execute(
                        """
                        INSERT INTO public.document_chunks
                            (merchant_id, content, title, source, chunk_index, total_chunks, embedding)
                        VALUES (%s, %s, %s, %s, %s, %s, %s::vector)
                        """,
                        (
                            merchant_id,
                            chunk["content"],
                            chunk["title"],
                            chunk["source"],
                            chunk["chunk_index"],
                            chunk["total_chunks"],
                            embedding_str,
                        )
                    )
                    stored += 1

            conn.commit()
            logger.info(f"Stored {stored} document chunks with embeddings for merchant {merchant_id}")
            return stored

        except Exception as e:
            logger.error(f"Error storing document embeddings for merchant {merchant_id}: {e}", exc_info=True)
            if conn:
                conn.rollback()
            return stored

        finally:
            if conn:
                return_connection(conn)

    def _create_ndjson(self, documents: List[Dict[str, Any]]) -> str:
        """
        Convert documents list to NDJSON format

        Args:
            documents: List of document dictionaries

        Returns:
            NDJSON string
        """
        lines = []
        for doc in documents:
            lines.append(json.dumps(doc, ensure_ascii=False))
        return '\n'.join(lines)

