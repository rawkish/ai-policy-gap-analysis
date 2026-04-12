# PolicyGuard AI — Policy Compliance Assessment System

An AI-powered tool designed to systematically assess whether your application's architecture and control area descriptions comply with your organization's formal policy documents.

---

## 🏗️ Architecture

The system is built on a modern, decoupled stack allowing for rapid local execution and secure data processing:

- **Frontend**: Vanilla HTML, CSS, and JS (no build step required). Communicates with the backend REST API.
- **Backend API**: Python 3.10+ using **FastAPI** for high-performance async processing.
- **Vector Store**: **Weaviate** locally hosted via Docker. Used to store embedded policy document chunks and perform semantic similarity search.
- **LLM Engine**: **Ollama** locally hosted via Docker. We use the `llama3.2:3b` model to ensure data never leaves your environment, maintaining strict compliance with data privacy acts.

### How PDF Data is Embedded
When a user uploads a policy PDF (e.g., `security-control-policy.pdf`):
1. **Extraction**: The `pdfplumber` library extracts raw text from the document, preserving basic structure.
2. **Chunking**: The backend splits the raw text into manageable, overlapping chunks (approx. 500-1000 characters) to avoid exceeding the LLM context window.
3. **Embedding**: Before being saved, each chunk is embedded using the local `sentence-transformers/all-MiniLM-L6-v2` embedding model (a fast, lightweight 384-dimensional vector generator).
4. **Storage**: The vector embeddings and raw chunk texts are ingested into **Weaviate** under a user-defined collection.

---

## 🛡️ Security Guardrails (Prompt Injection Defense)

Because the system relies on an LLM analyzing user input, it features a dual-layer prompt injection guardrail mechanism:

1. **Input Sanitization (Hard Regex)**: 
   Before any processing begins, the user's control area description is scanned against a set of regex patterns covering known adversarial inputs (e.g., `"ignore previous instructions"`, `"system prompt"`, `"you are now"`). If caught, the pipeline safely blocks the request immediately.
2. **Semantic Canary Block (Vector Injection Check)**: 
   The system deliberately ingests a "Canary" or "Poison" chunk into the vector database. If a user tries to employ an adversarial attack using complex semantic phrasing that bypasses regex, the semantic similarity search will retrieve the Canary chunk. The backend detects this specific chunk in the retrieval array and inherently blocks the prompt from ever reaching the LLM. 

---

## 🚀 How to Install and Run Locally

### Prerequisites
- Python 3.10+
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Required for Weaviate and Ollama)

### Step 1: Start Infrastructure
Open your terminal in the project root and spin up the Docker containers (Weaviate & Ollama):
```bash
docker compose up -d
```

### Step 2: Download the LLM Model
Because Ollama runs locally, you must pull the specific model we use (`llama3.2:3b` - approx 2GB) once:
```bash
docker exec -it ollama ollama pull llama3.2:3b
```

### Step 3: Start the Backend
Navigate to the `backend/` folder, install dependencies, and run the server:
```bash
cd backend
pip install -r requirements.txt
python main.py
```
*(Note: It is highly recommended to run this inside a Python virtual environment `python -m venv .venv`)*

### Step 4: Open the Application
The backend exposes the frontend visually. Simply open your browser and navigate to:
**http://localhost:8000**

---

## 🧪 Example Testing Data

Below are sample inputs for the 7 primary control areas you can use to test the compliance engine. They vary from fully compliant to having critical gaps relative to standard security policies (referencing `security-control-policy.pdf` expectations).

| Control Area | Sample User Description (Input) | Expected Assessment |
| --- | --- | --- |
| **Data Encryption** | "All databases, including backups, are encrypted using AES-256 at rest. All data in transit uses TLS 1.3 only." | **Compliant** |
| **Access Control** | "Staff log in using basic username and password. We plan to implement MFA next quarter, but currently, only standard passwords are used." | **Gap Found** (Missing MFA requirement) |
| **Audit Logging** | "We collect access logs for all primary servers and retain them for 30 days locally on the disk." | **Partial Implementation** (Logs likely need central aggregation or longer retention) |
| **Incident Response** | "We have a documented Incident Response plan that was finalized last year, but we have not performed tabletop exercises or simulations yet." | **Partial Implementation** (Missing testing requirement) |
| **Data Deletion** | "Users have an automated 'Delete Account' button in the UI. When clicked, all PII and database records are hard-deleted instantly across all systems." | **Compliant** |
| **Third-Party Access** | "Our server vendors have unrestricted VPN connections directly to the production subnet so they can perform maintenance anytime." | **Gap Found** (Violates principal of least privilege) |
| **Network Security** | "Our applications sit behind an edge WAF and load balancer. Internal components communicate via restricted VPC security groups on necessary ports only." | **Compliant** |

---

## 🔮 Future Improvements (If I had more time)

1. **PDF Table & Vision Parsing**: Standard text extraction struggles with complex tables and diagrams in Enterprise policies. Using a vision-capable multi-modal model to parse pages prior to embedding would drastically increase context accuracy.
2. **Asynchronous Task Queues**: Currently, PDF processing blocks the main thread (or is awaited directly). Using `Celery` & `Redis` for background ingestion would allow users to upload 100+ page documents without HTTP timeout risks.
3. **Advanced RAG Strategy (Metadata Filtering)**: Right now, chunks are purely text. Upgrading the ingestion pipeline to tag chunks with metadata (`page_number`, `section_header`, `policy_version`) would allow the LLM to cite specific pages in the UI, proving exactly *where* it found the compliance gap.
4. **Authentication & RBAC**: The UI currently permits any user to run analysis. A production tool needs Oauth2 (Google/Entra ID) and role-based views so non-security engineers cannot delete policy collections.
