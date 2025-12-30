# 🥝 Kiwi-RAG: AI-Powered Google Sheets Analytics

**Natural Language Query Interface for Google Sheets with Voice Support**

A production-ready RAG (Retrieval-Augmented Generation) system that enables natural language querying of Google Sheets data with multilingual voice support, intelligent change detection, and complex spreadsheet handling.

---

## ✨ Features

### Core Capabilities
- 🔍 **Natural Language Queries**: Ask questions in any language (English, Tamil, Hindi, Spanish, etc.)
- 📊 **Google Sheets Integration**: Direct connection via service account with automatic change detection
- 🎤 **Voice Input**: Speech-to-text using ElevenLabs Scribe v1
- 🔊 **Voice Output**: Text-to-speech with ElevenLabs multilingual v2 (fallback to gTTS)
- 🌐 **Universal Multilingual**: Works with any language - not limited to English or Tamil
- 💬 **Conversation History**: Save and manage multiple chat sessions
- 🔐 **Authentication**: Optional Supabase-based user authentication

### Query Types Supported
- **Lookup**: Find specific rows by criteria with LIKE operator for flexible text matching
- **Aggregation**: SUM, AVG, MIN, MAX, COUNT with metric registry
- **Filter**: Filter data by multiple conditions
- **Rank**: Order and limit results
- **Extrema Lookup**: Find minimum/maximum values
- **Aggregation on Subset**: Aggregate filtered or ranked data

### Technical Features
- ⚡ **Fast Analytics**: DuckDB for in-memory SQL execution
- 🎯 **RAG Pipeline**: ChromaDB + Hugging Face embeddings for semantic schema search
- 🤖 **AI Planning**: Gemini 2.5 Pro for intelligent query understanding
- 🔧 **Type Inference**: Automatic data type detection and conversion
- 📅 **Date Handling**: Smart date/time column combination with DD/MM/YYYY format detection
- 🔤 **Fuzzy Matching**: Handles name spelling variations (e.g., "ksh" ↔ "kch")
- 🔐 **Sheet-Level Change Detection**: Hash-based atomic rebuilds for data consistency
- 📝 **Multi-Table Detection**: Handles complex spreadsheets with multiple tables per sheet
- 🎙️ **Voice-Optimized Output**: Natural language responses designed for voice assistants
- 📊 **Sheet Source Attribution**: Always mentions which sheet data came from

### Sheet-Level Change Detection System

The system uses **sheet-level hash-based change detection** to ensure data consistency:

- **Atomic Unit**: Entire sheet is hashed (not individual tables)
- **Deterministic Hashing**: SHA-256 hash of raw sheet data before any processing
- **Incremental Rebuilds**: Only changed sheets are rebuilt, not the entire database
- **Perfect Synchronization**: DuckDB and ChromaDB always stay in sync
- **Automatic Migration**: Old table-level format automatically migrated to new sheet-level format
- **Source ID Tracking**: Each table tagged with `spreadsheet_id#sheet_name` for atomic cleanup

**How it works:**
1. On each query, compute hash of raw sheet data
2. Compare with stored hash in registry (`data_sources/snapshots/sheet_state.json`)
3. If changed: Delete all tables/embeddings from that sheet, rebuild completely
4. If unchanged: Use cached data (no rebuild needed)
5. Guarantees no partial updates or data drift

### Complex Spreadsheet Handling

The **table detector** module handles complex spreadsheets:

- **Multi-Table Detection**: Automatically detects multiple tables within a single sheet
- **Multi-Level Headers**: Normalizes complex multi-row headers
- **Wide Format Transformation**: Converts wide-format data to normalized tables
- **Table Cleaning**: Removes empty rows/columns and normalizes headers
- **Custom Detection**: Configurable table detection rules

---

## 🏗️ Architecture

### High-Level Data Flow

```
User Input (Text/Voice)
         │
         ▼
┌────────────────────┐
│  Streamlit App     │
│  (UI Layer)        │
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│  Change Detection  │ ← Checks sheet hashes
│  (Incremental)     │
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│  Schema Retrieval  │ ← ChromaDB + HuggingFace
│  (RAG)             │   Semantic search
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│  Query Planning    │ ← Gemini 2.5 Pro
│  (LLM)             │   Generates JSON plan
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│  Plan Validation   │ ← Schema validation
│                    │   Type checking
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│  SQL Compilation   │ ← Plan → SQL
│                    │   Safe templating
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│  Execution         │ ← DuckDB
│                    │   In-memory analytics
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│  Explanation       │ ← Gemini 2.5 Pro
│  (Natural Lang)    │   Results → Text
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│  Voice Output      │ ← ElevenLabs TTS
│  (Optional)        │   Multilingual
└────────────────────┘
```

### Component Architecture

```
kiwi-rag/
│
├── app/
│   ├── streamlit_app.py          # Main UI with voice features
│   ├── session_context.py        # Session state management
│   └── ui_components.py          # Reusable UI components
│
├── data_sources/
│   ├── gsheet/
│   │   ├── connector.py          # Google Sheets API integration
│   │   ├── change_detector.py    # Sheet-level hash-based change detection
│   │   ├── sheet_hasher.py       # SHA-256 hashing of raw sheet data
│   │   ├── snapshot_loader.py    # DuckDB snapshot management
│   │   ├── table_detection.py    # Multi-table detection within sheets
│   │   └── wide_format_transformer.py  # Wide → normalized format
│   ├── snapshots/
│   │   ├── latest.duckdb         # DuckDB snapshot
│   │   ├── sheet_state.json      # Sheet hash registry
│   │   └── table_metadata.json   # Table metadata cache
│   └── conversations/            # Saved chat sessions (JSON)
│
├── schema_intelligence/
│   ├── chromadb_client.py        # Vector store for schema embeddings
│   ├── schema_extractor.py       # Extract schema metadata from tables
│   ├── embedding_builder.py      # Build embeddings for schemas
│   └── hybrid_retriever.py       # Semantic schema search
│
├── planning_layer/
│   ├── planner_client.py         # Gemini AI query planner
│   ├── planner_prompt.py         # System prompt for planning
│   ├── plan_schema.json          # JSON schema for query plans
│   └── rule_based_planner.py     # Fallback rule-based planner
│
├── validation_layer/
│   ├── plan_validator.py         # Validate query plans against schema
│   ├── join_rules.py             # Join validation rules
│   └── rejection_handler.py      # Handle invalid plans
│
├── execution_layer/
│   ├── sql_compiler.py           # Convert plans to SQL (safe templating)
│   └── executor.py               # Execute SQL on DuckDB
│
├── explanation_layer/
│   ├── explainer_client.py       # Gemini AI for natural language explanations
│   └── explanation_prompt.py     # System prompt for explanations
│
├── analytics_engine/
│   └── metric_registry.py        # Predefined metrics (SUM, AVG, etc.)
│
├── table detector/
│   ├── table_detector.py         # Detect multiple tables in sheets
│   ├── custom_detector.py        # Custom detection rules
│   ├── table_cleaner.py          # Clean and normalize tables
│   ├── extract_tables.py         # Extract tables from sheets
│   └── sheet_ingestion.py        # Ingest complex spreadsheets
│
├── utils/
│   ├── voice_utils.py            # ElevenLabs voice I/O
│   ├── conversation_manager.py   # Save/load conversations
│   ├── question_cache.py         # Cache query results (disabled by default)
│   ├── context_resolver.py       # Context memory (disabled by default)
│   ├── auth_integration.py       # Authentication integration
│   └── supabase_auth.py          # Supabase authentication
│
├── config/
│   └── settings.yaml             # Configuration file
│
├── credentials/
│   └── service_account.json      # Google Sheets service account
│
├── schema_store/
│   └── chroma.sqlite3            # ChromaDB storage
│
├── .env                          # Environment variables (API keys)
├── requirements.txt              # Python dependencies
└── run_query.py                  # CLI entry point
```

---

## 🚀 Installation

### Prerequisites

- Python 3.9+
- Google Cloud Project with Sheets API enabled
- Gemini API key
- ElevenLabs API key (optional, for voice features)
- Supabase project (optional, for authentication)

### Step 1: Clone Repository

```bash
git clone https://github.com/asif-mp3/kiwi-rag.git
cd kiwi-rag
```

### Step 2: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 3: Set Up Environment Variables

Create a `.env` file in the root directory:

```env
GEMINI_API_KEY=your_gemini_api_key_here
ELEVENLABS_API_KEY=your_elevenlabs_api_key_here  # Optional
SUPABASE_URL=your_supabase_url  # Optional
SUPABASE_KEY=your_supabase_key  # Optional
```

### Step 4: Configure Google Sheets

1. Create a Google Cloud Project
2. Enable Google Sheets API
3. Create a Service Account
4. Download credentials JSON
5. Save as `credentials/service_account.json`

### Step 5: Update Configuration

Edit `config/settings.yaml`:

```yaml
google_sheets:
  credentials_path: "credentials/service_account.json"
  spreadsheet_id: ""  # Set via UI or here

llm:
  model: "gemini-2.5-pro"
  temperature: 0.0
  api_key_env: "GEMINI_API_KEY"
  max_retries: 3

schema_intelligence:
  embedding_model: "text-embedding-3-large"
  top_k: 5

duckdb:
  snapshot_path: "data_sources/snapshots/latest.duckdb"
```

---

## 📖 Usage

### Starting the Application

```bash
streamlit run app/streamlit_app.py
```

The app will open at `http://localhost:8501`

### Basic Workflow

1. **Connect Data Source**
   - Paste your Google Sheets URL in the input field
   - Click "🔄 Load Data"
   - System automatically detects tables and builds schema

2. **Ask Questions**
   - **Text**: Type your question in the chat input
   - **Voice**: Click the microphone icon and speak
   - **Any Language**: Ask in English, Tamil, Hindi, Spanish, etc.

3. **Get Answers**
   - Audio plays automatically (if voice enabled)
   - Click "📝 View Text Answer" to see text response
   - Expand sections for query plan, data, and schema context

### Example Queries

**English:**
- "What is the total sales in October?"
- "Show me products with quantity greater than 10"
- "Who has the highest salary?"
- "What was the gross sales for Dairy and homemade on 6th October?"

**Tamil:**
- "மொத்த விற்பனை எவ்வளவு?"
- "மீனாட்சி எவ்ளோ சம்பளம் வாங்குறா?"
- "அதிக சம்பளம் யாருக்கு?"

**Hindi:**
- "कुल बिक्री कितनी है?"
- "सबसे ज्यादा वेतन किसका है?"

**Works in any language!** The AI understands and responds in the same language you use.

### CLI Usage

For command-line usage:

```bash
python run_query.py
```

Edit the question in `run_query.py` at the bottom:

```python
if __name__ == "__main__":
    run("What was the average temperature on 02/01/2017?")
```

---

## 🔄 Query Processing Workflow

### 1. Change Detection Phase

```python
# Automatic on every query
sheets_with_tables = fetch_sheets_with_tables()  # Computes hashes
needs_refresh, full_reset, changed_sheets = needs_refresh(sheets_with_tables)

if needs_refresh:
    if full_reset:
        # Spreadsheet ID changed or first run
        # Clear ChromaDB + DuckDB, rebuild all
    else:
        # Incremental rebuild for changed sheets only
        # Delete old tables/embeddings, rebuild changed sheets
```

### 2. Schema Retrieval Phase

```python
# Semantic search over table schemas
schema_context = retrieve_schema(question)
# Returns top 5 relevant table schemas using ChromaDB + HuggingFace embeddings
```

### 3. Query Planning Phase

```python
# Gemini AI generates structured query plan
plan = generate_plan(question, schema_context)
# Returns JSON plan with query_type, table, filters, etc.

# Example plan:
{
  "query_type": "lookup",
  "table": "Sales_Table1",
  "select_columns": ["Gross sales - 06/10/2025"],
  "filters": [
    {"column": "Sales by Cat", "operator": "LIKE", "value": "%Dairy and homemade%"}
  ],
  "limit": 1
}
```

### 4. Validation Phase

```python
# Validate plan against actual schema
validate_plan(plan)
# Checks: table exists, columns exist, types match, operators valid
```

### 5. Execution Phase

```python
# Compile plan to SQL
sql = compile_sql(plan)
# Example: SELECT "Gross sales - 06/10/2025" FROM "Sales_Table1" 
#          WHERE LOWER(CAST("Sales by Cat" AS VARCHAR)) LIKE LOWER('%Dairy and homemade%')
#          LIMIT 1

# Execute on DuckDB
result = execute_plan(plan)
# Returns pandas DataFrame
```

### 6. Explanation Phase

```python
# Generate natural language explanation
explanation = explain_results(result, query_plan=plan, original_question=question)
# Gemini AI converts DataFrame to conversational response in same language as question
```

### 7. Voice Output Phase (Optional)

```python
# Convert text to speech
audio_bytes = text_to_speech(explanation)
# ElevenLabs multilingual v2 with fallback to gTTS
# Auto-plays in browser
```

---

## 🔧 Key Features Explained

### Flexible Text Matching with LIKE Operator

The system uses `LIKE` operator with wildcards for text column filters instead of exact `=` matching:

```python
# User query: "What was the gross sales for Dairy and homemade?"

# Generated filter (correct):
{"column": "Sales by Cat", "operator": "LIKE", "value": "%Dairy and homemade%"}

# Compiled SQL:
WHERE LOWER(CAST("Sales by Cat" AS VARCHAR)) LIKE LOWER('%Dairy and homemade%')
```

This handles:
- Partial matches
- Case-insensitive matching
- Variations in spacing
- More robust than exact `=` matching

### Fuzzy Name Matching

The SQL compiler includes fuzzy matching for name variations:

```python
# Handles common Tamil name spelling variations:
# "Meenakshi" ↔ "Meenakchi"
# "ksh" ↔ "kch" ↔ "kchi"
# "sh" ↔ "ch"

# Generates multiple LIKE patterns:
WHERE (
  LOWER(CAST(name AS VARCHAR)) LIKE LOWER('%Meenakshi%') OR
  LOWER(CAST(name AS VARCHAR)) LIKE LOWER('%Meenakchi%')
)
```

### Date Format Detection

Automatically detects DD/MM/YYYY vs MM/DD/YYYY format:

```python
# Looks for unambiguous dates (day > 12)
# "15/03/2024" → Definitely DD/MM/YYYY (day=15)
# "03/15/2024" → Definitely MM/DD/YYYY (day=15)
# Defaults to DD/MM/YYYY if ambiguous
```

### Date+Time Column Combination

Combines separate Date and Time columns into proper timestamps:

```python
# Before: Date="02/01/2017" (VARCHAR), Time="08:00:00" (TIME)
# After:  Date="2017-01-02" (VARCHAR), Time="2017-01-02 08:00:00" (TIMESTAMP)

# Enables time range queries:
WHERE Time >= '2017-01-02 08:00:00' AND Time <= '2017-01-02 16:00:00'
```

### Multi-Table Detection

Detects multiple tables within a single sheet:

```python
# Example: "Sales" sheet contains 16 tables
# Each table gets unique name: Sales_Table1, Sales_Table2, ..., Sales_Table16
# All tagged with source_id: "1mRcD...#Sales"
# Enables atomic cleanup when sheet changes
```

---

## 🔐 Authentication (Optional)

The system supports optional Supabase authentication:

1. Set up Supabase project
2. Add `SUPABASE_URL` and `SUPABASE_KEY` to `.env`
3. Authentication UI appears automatically
4. Users can sign up, log in, and manage sessions

To disable authentication, simply don't set Supabase environment variables.

---

## 🐛 Troubleshooting

### Common Issues

**1. "Column 'X' does not exist"**
- **Cause**: Column name mismatch or multi-level headers
- **Solution**: System auto-detects and normalizes headers. Try reloading data.

**2. "No data found"**
- **Cause**: Exact match failed (old behavior with `=` operator)
- **Solution**: System now uses `LIKE` operator for text matching (fixed in latest version)

**3. "Voice output failed"**
- **Cause**: ElevenLabs quota exceeded or API key missing
- **Solution**: System auto-falls back to gTTS (free)

**4. "ChromaDB connection error"**
- **Cause**: Corrupt vector store
- **Solution**: Delete `schema_store/` folder and restart

**5. Slow initial load**
- **Cause**: Vector store initialization and embeddings generation
- **Solution**: Normal, only happens once per spreadsheet

**6. "Incremental rebuild failed"**
- **Cause**: Sheet hash mismatch or corrupted registry
- **Solution**: Delete `data_sources/snapshots/sheet_state.json` to force full rebuild

---

## 📊 Performance Metrics

- **Query Response Time**: 2-5 seconds (typical)
- **Voice Transcription**: 1-3 seconds
- **Audio Generation**: 1-2 seconds
- **Data Loading**: 3-10 seconds (depends on sheet size)
- **Incremental Rebuild**: 1-3 seconds per changed sheet
- **Full Rebuild**: 5-15 seconds (all sheets)
- **Supported Data Size**: Up to 100K rows (tested)

---

## 🔐 Security Notes

- API keys stored in `.env` (not committed to git)
- Google credentials in separate JSON file (not committed)
- Read-only access to Google Sheets
- Conversation data stored locally (JSON files)
- All processing happens locally (except LLM API calls)
- Optional Supabase authentication for multi-user deployments

---

## 📝 Configuration Reference

### settings.yaml

```yaml
# Google Sheets
google_sheets:
  credentials_path: "credentials/service_account.json"
  spreadsheet_id: "your_spreadsheet_id"  # Set via UI or here

# LLM (Gemini)
llm:
  model: "gemini-2.5-pro"
  temperature: 0.0
  api_key_env: "GEMINI_API_KEY"
  max_retries: 3
  provider: "gemini"

# Schema Intelligence
schema_intelligence:
  embedding_model: "text-embedding-3-large"
  top_k: 5

# DuckDB
duckdb:
  snapshot_path: "data_sources/snapshots/latest.duckdb"

# Project
project:
  name: "rag_gsheet_analytics"
  environment: "local"
```

### .env

```env
# Required
GEMINI_API_KEY=your_gemini_api_key

# Optional (Voice)
ELEVENLABS_API_KEY=your_elevenlabs_api_key

# Optional (Authentication)
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
```

---

## 🧪 Testing

### Manual Testing

1. **Basic Query**: "What is the total sales?"
2. **Filter Query**: "Show products with quantity > 10"
3. **Lookup Query**: "What was the gross sales for Dairy and homemade?"
4. **Date Query**: "What was the average temperature on 02/01/2017?"
5. **Voice Query**: Click microphone and ask a question
6. **Multilingual**: Ask in Tamil, Hindi, or any language

### Change Detection Testing

1. Modify a cell in Google Sheets
2. Run a query in the app
3. Verify "Incremental rebuild" message appears
4. Verify only changed sheet is rebuilt

---

## 📦 Dependencies

### Core
- `google-generativeai>=0.3.0` - Gemini AI
- `streamlit>=1.28.0` - Web UI
- `duckdb>=0.9.0` - In-memory analytics
- `chromadb>=0.4.0` - Vector store
- `sentence-transformers>=2.2.0` - Embeddings

### Google Sheets
- `gspread>=5.0.0` - Google Sheets API
- `google-auth>=2.0.0` - Authentication

### Voice
- `elevenlabs>=1.0.0` - Voice I/O
- `gtts>=2.4.0` - Fallback TTS
- `langdetect>=1.0.9` - Language detection

### Authentication
- `supabase>=2.0.0` - User authentication

### Utilities
- `pandas>=2.0.0` - Data processing
- `pyyaml>=6.0` - Configuration
- `python-dotenv>=1.0.0` - Environment variables

---

## 🙏 Acknowledgments

- **Gemini AI** - Query planning and explanation
- **ElevenLabs** - Voice input/output
- **ChromaDB** - Vector storage
- **Hugging Face** - Embeddings
- **DuckDB** - Fast analytics
- **Streamlit** - Web framework
- **Supabase** - Authentication

---

## 📞 Support

For issues and questions:
- GitHub Issues: [Create an issue](https://github.com/asif-mp3/kiwi-rag/issues)
- Repository: [github.com/asif-mp3/kiwi-rag](https://github.com/asif-mp3/kiwi-rag)

---

**Built with ❤️ for multilingual data analytics**
