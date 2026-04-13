# Policy Gap Analysis System

An AI-powered tool designed to systematically assess whether your application's architecture and control area descriptions comply with the organization's formal policy documents.


## How to Install and Run Locally

### Prerequisites
- Python 3.12+
- Docker for weaviate and ollama
- uv

### Step 1: Start Containers
```bash
docker compose up -d
```

### Step 2: Download the LLM Model
we use `llama3.2:3b` - approx 2GB
```bash
docker exec -it ollama ollama pull llama3.2:3b
```
Do this only once.

### Step 3: Start the Backend
Navigate to the `backend/` folder, install dependencies, and run the server
```bash
cd backend
uv sync
python main.py
```

### Step 4: Open the Application
Click below
http://localhost:8000
Upload a policy document in the Database tab.
Switch to the Analysis tab, select/create control areas, and get compliance assessment!


## Architecture

- Backend: Built 3 separate pipelines for PDF ingestion, vector storage retrieval, and LLM-based compliance assessment.
- Backend API: Python 3.12+ using FastAPI for high-performance async processing.
- Vector Store: Weaviate locally hosted via Docker. Used to store embedded policy document chunks and performing semantic similarity search.
- LLM Engine: Ollama locally hosted via Docker. We use the `llama3.2:3b`.
- Frontend: HTML, CSS, and JS(used AI). Communicates with the backend REST API.

### How PDF Data is Embedded
When a user uploads a policy PDF ( `security-control-policy.pdf`):
1. Extraction: The `pdfplumber` library extracts raw text from the document, preserving basic structure.
2. Chunking: The backend splits the raw text into manageable, overlapping chunks based on the structure of the pdf to avoid exceeding the LLM context window.
3. Embedding: Before being saved, each chunk is embedded using the local `sentence-transformers/all-MiniLM-L6-v2` embedding model (a fast, lightweight 384-dimensional vector generator).
4. Storage: The vector embeddings and raw chunk texts are ingested into Weaviate under a user-defined collection.


## Security Guardrails (Prompt Injection Defense)

Because the system relies on an LLM analyzing user input, it features a dual-layer prompt injection guardrail mechanism:

1. Input Sanitization ( Regex): 
   Before any processing begins, the user's control area description is scanned against a set of regex patterns covering known adversarial inputs (e.g., `"ignore previous instructions"`, `"system prompt"`, `"you are now"`). If caught, the pipeline safely blocks the request immediately.
2. Semantic Canary Block (Vector Injection Check): 
   The system deliberately ingests a "Canary" or "HoneyPot" chunk into the vector database. If a user tries to employ an adversarial attack using complex semantic phrasing that bypasses regex, the semantic similarity search will retrieve the Canary chunk. The backend detects this specific chunk in the retrieval array and inherently blocks the prompt from ever reaching the LLM. 


## Features:

- Create multiple collections in Weaviate for different policy documents. Switch between collections to analyze different policies without re-ingestion all from the webapp.
- Create custom control areas and get compliance assessment for each area separately. Flexibility to analyze specific control areas of the application architecture against the policy. Good User Experience.
- Detailed compliance assessment with specific references to the policy document sections that are relevant to each control area. The LLM provides a comprehensive analysis of how well the control area aligns with the policy requirements, citing specific sections and providing actionable feedback for improvement.
- Intuitive prompt injection detection and mitigation strategies to ensure the integrity of the LLM analysis.
- Accurate rerieval of relevant policy sections using semantic similarity search, ensuring the LLM has the necessary context to provide a thorough compliance assessment.
- Accurate analysis of the control area descriptions against the policy requirements, providing specific feedback on compliance gaps and areas for improvement.

## Future Improvements 

1. Support for ingestion of Multiple document formats (Word, Excel) and direct URL ingestion for web-based policies.
2. More advanced chunking strategies that preserve semantic sections.
3. Integration of a more powerful Embedding model and various othere retrieval stratedgies such as hybrid search (sparse + dense embedding), late interaction or reranking.
4. Integration of a more powerful LLM or a multimodal model that can directly analyze tables and diagrams in the policy documents.
5. Automate the extraction of application specifications from the user documents instead of taking input separately for each control area.
6. Make ingestion and retrieval asynchronous, so the main thread is not blocked.
7. Apply advanced metadata filtering and chunk prioritization strategies to ensure the most relevant policy sections are retrieved.
8. Integration of authentication and RBAC.
