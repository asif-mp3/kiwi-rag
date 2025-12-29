"""
Kiwi-RAG Analytics Chatbot - Streamlit Interface with Voice Features

A ChatGPT-like interface for querying Google Sheets data with automatic change detection.
Features voice input (speech-to-text) and voice output (text-to-speech) with multilingual support.
Uses Hugging Face embeddings (no ONNX) for Streamlit-safe operation on Windows.
"""

import sys
from pathlib import Path

# Add project root to Python path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import streamlit as st
from datetime import datetime
import tempfile
import re
from schema_intelligence.hybrid_retriever import retrieve_schema
from planning_layer.planner_client import generate_plan
from validation_layer.plan_validator import validate_plan
from execution_layer.executor import execute_plan
from explanation_layer.explainer_client import explain_results
from data_sources.gsheet.change_detector import needs_refresh, mark_synced
from data_sources.gsheet.snapshot_loader import load_snapshot
from schema_intelligence.chromadb_client import SchemaVectorStore
from utils.voice_utils import transcribe_audio, text_to_speech, save_audio_temp
from utils.conversation_manager import ConversationManager
from utils.question_cache import QuestionCache
from utils.context_resolver import ContextResolver
from utils.auth_integration import setup_authentication, add_auth_sidebar

# Page configuration
st.set_page_config(
    page_title="Kiwi-RAG Analytics",
    page_icon="🥝",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for enhanced UI
st.markdown("""
<style>
    .stChatMessage {
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
    }
    .user-message {
        background-color: #f0f2f6;
    }
    .assistant-message {
        background-color: #e8f4f8;
    }
    .status-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 1rem;
        font-size: 0.875rem;
        font-weight: 500;
        margin: 0.25rem;
    }
    .status-success {
        background-color: #d4edda;
        color: #155724;
    }
    .status-info {
        background-color: #d1ecf1;
        color: #0c5460;
    }
    .status-warning {
        background-color: #fff3cd;
        color: #856404;
    }
    .main-header {
        text-align: center;
        padding: 1rem 0;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 0.5rem;
        margin-bottom: 2rem;
    }
    .sheets-input-container {
        background-color: #f8f9fa;
        padding: 1.5rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
        border: 2px solid #e9ecef;
    }
</style>
""", unsafe_allow_html=True)

# ============================================
# AUTHENTICATION CHECK
# ============================================
# Setup authentication - if auth is enabled and user is not logged in,
# this will show the login page and stop execution
if not setup_authentication():
    st.stop()

# If we reach here, either auth is disabled or user is authenticated
# ============================================

# Initialize session state
if 'conversation_manager' not in st.session_state:
    st.session_state.conversation_manager = ConversationManager()

if 'current_conversation_id' not in st.session_state:
    # Create first conversation
    st.session_state.current_conversation_id = st.session_state.conversation_manager.create_conversation()

if 'messages' not in st.session_state:
    # Load messages from current conversation
    conv = st.session_state.conversation_manager.load_conversation(st.session_state.current_conversation_id)
    st.session_state.messages = conv['messages'] if conv else []

if 'vector_store' not in st.session_state:
    with st.spinner("🔧 Initializing..."):
        st.session_state.vector_store = SchemaVectorStore()

if 'voice_enabled' not in st.session_state:
    st.session_state.voice_enabled = True

if 'sheets_url' not in st.session_state:
    st.session_state.sheets_url = ""

if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False

if 'question_cache' not in st.session_state:
    st.session_state.question_cache = QuestionCache(similarity_threshold=0.95)

if 'context_resolver' not in st.session_state:
    st.session_state.context_resolver = ContextResolver()

if 'last_known_fingerprints' not in st.session_state:
    st.session_state.last_known_fingerprints = None

def extract_spreadsheet_id(url):
    """Extract spreadsheet ID from Google Sheets URL"""
    # Pattern for Google Sheets URLs
    pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
    match = re.search(pattern, url)
    if match:
        return match.group(1)
    # If it's already just the ID
    if re.match(r'^[a-zA-Z0-9-_]+$', url):
        return url
    return None

def load_sheets_data(spreadsheet_id):
    """Load data from Google Sheets with given spreadsheet ID"""
    try:
        import yaml
        
        # Update config with new spreadsheet ID
        config_path = "config/settings.yaml"
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        config['google_sheets']['spreadsheet_id'] = spreadsheet_id
        
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        
        # Reload data using fetch_sheets
        from data_sources.gsheet.connector import fetch_sheets
        sheets = fetch_sheets()
        
        # Clear and rebuild vector store
        store = st.session_state.vector_store
        store.clear_collection()
        load_snapshot(sheets, full_reset=True)
        store.rebuild()
        
        return True, sheets
    except Exception as e:
        return False, str(e)

def check_and_refresh_data():
    """Automatically check for data changes and refresh if needed."""
    from data_sources.gsheet.change_detector import load_sheet_state, compute_current_fingerprints
    
    # Quick check: Compare cached fingerprints with stored state
    stored_state = load_sheet_state()
    stored_fingerprints = stored_state.get('fingerprints', {})
    
    # If we have cached fingerprints and they match stored state, skip refresh
    if st.session_state.last_known_fingerprints is not None:
        if st.session_state.last_known_fingerprints == stored_fingerprints:
            # No changes detected - skip expensive sheet fetch
            return False
    
    # Either no cache or fingerprints differ - do full check
    needs_refresh_flag, full_reset, current_sheets = needs_refresh()
    
    if needs_refresh_flag:
        store = st.session_state.vector_store
        
        if full_reset:
            st.info("📊 Sheet structure changed - performing automatic full reset...")
            store.clear_collection()
            load_snapshot(current_sheets, full_reset=True)
            store.rebuild()
            st.success("✅ Full reset complete")
        else:
            st.info("📊 Content changed - performing automatic incremental refresh...")
            load_snapshot(current_sheets, full_reset=False)
            store.rebuild()
            st.success("✅ Incremental refresh complete")
        
        mark_synced(current_sheets)
        
        # Update cached fingerprints
        new_fingerprints = compute_current_fingerprints(current_sheets)
        st.session_state.last_known_fingerprints = new_fingerprints
        
        return True
    
    # No refresh needed - update cache to match stored state
    st.session_state.last_known_fingerprints = stored_fingerprints
    return False

def save_message(role: str, content: str, metadata: dict = None):
    """Save message to current conversation and session state"""
    # Add to session state
    message = {"role": role, "content": content}
    if metadata:
        message["metadata"] = metadata
    st.session_state.messages.append(message)
    
    # Save to conversation manager
    st.session_state.conversation_manager.save_message(
        st.session_state.current_conversation_id,
        role,
        content,
        metadata
    )

def process_query(question: str):
    """Process a user query through the full RAG pipeline with context memory."""
    try:
        # Step 0: Check question cache for similar questions
        cache_result = st.session_state.question_cache.find_similar(question)
        
        if cache_result:
            answer, metadata, similarity = cache_result
            return {
                'success': True,
                'explanation': answer,
                'data': metadata.get('data'),
                'plan': metadata.get('plan'),
                'schema_context': metadata.get('schema_context'),
                'data_refreshed': False,
                'from_cache': True,
                'similarity': similarity
            }
        
        # Step 0.5: Check if follow-up question and resolve context
        original_question = question
        is_followup = st.session_state.context_resolver.is_followup(
            question, 
            st.session_state.messages
        )
        
        if is_followup:
            resolved_question = st.session_state.context_resolver.resolve_context(
                question,
                st.session_state.messages
            )
            question = resolved_question  # Use resolved question for processing
        
        # Automatic change detection before processing
        data_refreshed = check_and_refresh_data()
        
        # Step 1: Schema retrieval
        schema_context = retrieve_schema(question)
        
        # Step 2: Planning
        plan = generate_plan(question, schema_context)
        validate_plan(plan)
        
        # Step 3: Execution
        result = execute_plan(plan)
        
        # Step 4: Explanation
        explanation = explain_results(result, query_plan=plan, original_question=question)
        
        # Cache the result for future similar questions
        st.session_state.question_cache.add_to_cache(
            original_question,  # Cache with original question
            explanation,
            {
                'plan': plan,
                'data': result.to_dict() if hasattr(result, 'to_dict') else None,
                'schema_context': schema_context,
                'data_refreshed': data_refreshed
            }
        )
        
        return {
            'success': True,
            'explanation': explanation,
            'data': result,
            'plan': plan,
            'schema_context': schema_context,
            'data_refreshed': data_refreshed,
            'from_cache': False,
            'was_followup': is_followup,
            'resolved_question': question if is_followup else None
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'exception': e,
            'from_cache': False
        }

# Header
st.markdown("""
<div class='main-header'>
    <h1>🥝 Kiwi-RAG Analytics Chatbot</h1>
    <p style='margin: 0; font-size: 1.1rem;'>AI-Powered Google Sheets Analytics with Voice Support</p>
</div>
""", unsafe_allow_html=True)

# Google Sheets URL Input Section
with st.container():
    st.markdown("<div class='sheets-input-container'>", unsafe_allow_html=True)
    st.subheader("📊 Connect Your Google Sheet")
    
    col1, col2 = st.columns([4, 1])
    
    with col1:
        sheets_url_input = st.text_input(
            "Google Sheets URL or Spreadsheet ID",
            value=st.session_state.sheets_url,
            placeholder="https://docs.google.com/spreadsheets/d/YOUR_SPREADSHEET_ID/edit or just the ID",
            help="Paste your Google Sheets URL or just the spreadsheet ID"
        )
    
    with col2:
        st.markdown("<div style='margin-top: 1.8rem;'></div>", unsafe_allow_html=True)
        load_button = st.button("🔄 Load Data", type="primary", use_container_width=True)
    
    if load_button and sheets_url_input:
        spreadsheet_id = extract_spreadsheet_id(sheets_url_input)
        
        if spreadsheet_id:
            with st.spinner("Loading data from Google Sheets..."):
                success, result = load_sheets_data(spreadsheet_id)
                
                if success:
                    st.session_state.sheets_url = sheets_url_input
                    st.session_state.data_loaded = True
                    st.success(f"✅ Successfully loaded {len(result)} sheet(s)!")
                    st.rerun()
                else:
                    st.error(f"❌ Failed to load data: {result}")
        else:
            st.error("❌ Invalid Google Sheets URL or ID. Please check and try again.")
    
    # Show current status
    if st.session_state.data_loaded:
        st.success("✅ Data loaded and ready for queries!")
    else:
        st.warning("⚠️ No data loaded. Please enter a Google Sheets URL above.")
    
    st.markdown("</div>", unsafe_allow_html=True)

# Status badges
st.markdown("""
<div style='text-align: center; margin-bottom: 1.5rem;'>
    <span class='status-badge status-success'>✓ Hugging Face Embeddings</span>
    <span class='status-badge status-info'>No ONNX</span>
    <span class='status-badge status-info'>🎤 Voice Enabled</span>
    <span class='status-badge status-warning'>🌐 Multilingual (Tamil/English)</span>
</div>
""", unsafe_allow_html=True)

# Sidebar with Conversation History
with st.sidebar:
    # Show user info if authenticated
    add_auth_sidebar()
    
    st.header("💬 Conversations")
    
    # New Chat button
    if st.button("➕ New Chat", use_container_width=True, type="primary"):
        # Save current conversation before creating new one
        new_conv_id = st.session_state.conversation_manager.create_conversation()
        st.session_state.current_conversation_id = new_conv_id
        st.session_state.messages = []
        st.rerun()
    
    st.markdown("---")
    
    # List all conversations
    conversations = st.session_state.conversation_manager.list_conversations()
    
    if conversations:
        st.subheader("Recent Chats")
        for conv in conversations:
            col1, col2 = st.columns([4, 1])
            
            with col1:
                # Highlight current conversation
                is_current = conv['id'] == st.session_state.current_conversation_id
                button_type = "primary" if is_current else "secondary"
                
                # Truncate title if too long
                display_title = conv['title']
                if len(display_title) > 30:
                    display_title = display_title[:30] + "..."
                
                if st.button(
                    f"{'📌 ' if is_current else '💬 '}{display_title}",
                    key=f"conv_{conv['id']}",
                    use_container_width=True,
                    type=button_type if is_current else "secondary"
                ):
                    # Switch to this conversation
                    if conv['id'] != st.session_state.current_conversation_id:
                        st.session_state.current_conversation_id = conv['id']
                        loaded_conv = st.session_state.conversation_manager.load_conversation(conv['id'])
                        st.session_state.messages = loaded_conv['messages'] if loaded_conv else []
                        st.rerun()
            
            with col2:
                # Delete button
                if st.button("🗑️", key=f"del_{conv['id']}", help="Delete conversation"):
                    st.session_state.conversation_manager.delete_conversation(conv['id'])
                    # If deleting current conversation, create a new one
                    if conv['id'] == st.session_state.current_conversation_id:
                        new_conv_id = st.session_state.conversation_manager.create_conversation()
                        st.session_state.current_conversation_id = new_conv_id
                        st.session_state.messages = []
                    st.rerun()
    
    st.markdown("---")
    st.header("💡 Example Questions")
    st.markdown("""
    - What was the temperature on 02/01/2017?
    - What was the average wind speed in January 2017?
    - What was the maximum PM2.5 level?
    - Show me the ozone levels on 05/01/2017
    - What was the temperature at 10:00 on 02/01/2017?
    """)
    
    st.markdown("---")
    st.header("🎤 Voice Features")
    st.markdown("""
    **Voice Input**: Click the microphone button to record your question
    
    **Voice Output**: Click the speaker button to hear the answer
    
    **Multilingual**: Automatically detects Tamil or English
    
    Powered by ElevenLabs Scribe v1 & Google TTS
    """)
    
    st.markdown("---")
    st.header("ℹ️ About")
    st.markdown("""
    This chatbot uses:
    - **Hugging Face** for embeddings
    - **Gemini** for planning & explanation
    - **DuckDB** for analytics
    - **ChromaDB** for schema search
    - **ElevenLabs** for voice input
    - **Google TTS** for voice output
    
    Data automatically refreshes when changes are detected.
    """)

# Display chat messages
for idx, message in enumerate(st.session_state.messages):
    with st.chat_message(message["role"]):
        # For user messages, show text normally
        if message["role"] == "user":
            st.markdown(message["content"])
        
        # For assistant messages, prioritize audio
        elif message["role"] == "assistant":
            # Check for English translation separator
            content = message["content"]
            tamil_part = content
            english_part = None
            
            if "|||ENGLISH_TRANSLATION|||" in content:
                parts = content.split("|||ENGLISH_TRANSLATION|||")
                tamil_part = parts[0].strip()
                if len(parts) > 1:
                    english_part = parts[1].strip()
            
            # Show text answer in collapsible expander (no auto-play)
            with st.expander("📝 View Text Answer", expanded=False):
                st.markdown(tamil_part)
                
                # If we have an English translation, show it too
                if english_part:
                    st.markdown("---")
                    st.markdown("**English Translation:**")
                    st.markdown(english_part)
            
            # Show additional metadata
            if "metadata" in message:
                metadata = message["metadata"]
                
                if metadata.get("data_refreshed"):
                    st.info("🔄 Data was automatically refreshed before processing this query")
                
                # Expandable sections for details
                if metadata.get("plan"):
                    with st.expander("📋 Query Plan"):
                        st.json(metadata["plan"])
                
                # Handle data - could be dict (from JSON) or DataFrame
                if metadata.get("data") is not None:
                    data = metadata["data"]
                    # Check if it's a dict (loaded from JSON) or DataFrame
                    if isinstance(data, dict) and data:
                        with st.expander("📊 Data"):
                            st.json(data)
                    elif hasattr(data, 'empty') and not data.empty:
                        with st.expander("📊 Data"):
                            st.dataframe(data, use_container_width=True)
                
                if metadata.get("schema_context"):
                    with st.expander("🗂️ Schema Context"):
                        for i, item in enumerate(metadata["schema_context"], 1):
                            st.markdown(f"**{i}.** {item['text']}")

# Voice input section
if st.session_state.data_loaded:
    st.markdown("### 🎤 Voice Input")
    
    # Initialize voice input counter if not exists
    if 'voice_input_counter' not in st.session_state:
        st.session_state.voice_input_counter = 0
    
    col1, col2 = st.columns([3, 1])

    with col1:
        # Use unique key based on counter to allow multiple recordings
        audio_input = st.audio_input(
            "Record your question",
            key=f"audio_input_{st.session_state.voice_input_counter}"
        )

    with col2:
        st.markdown("<div style='margin-top: 1.5rem;'></div>", unsafe_allow_html=True)
        transcribe_button = st.button(
            "📝 Transcribe",
            disabled=audio_input is None,
            key=f"transcribe_btn_{st.session_state.voice_input_counter}"
        )

    if transcribe_button and audio_input:
        with st.spinner("🎤 Transcribing audio..."):
            try:
                # Save audio to temp file
                with tempfile.NamedTemporaryFile(delete=False, suffix='.wav') as tmp_file:
                    tmp_file.write(audio_input.read())
                    tmp_path = tmp_file.name
                
                # Transcribe
                transcribed_text = transcribe_audio(tmp_path)
                
                # Increment counter to reset audio input for next recording
                st.session_state.voice_input_counter += 1
                
                # Add user message
                save_message("user", transcribed_text)
                
                # Process query
                with st.spinner("🤔 Thinking..."):
                    response = process_query(transcribed_text)
                
                if response['success']:
                    save_message(
                        "assistant",
                        response['explanation'],
                        {
                            "plan": response['plan'],
                            "data": response['data'].to_dict() if hasattr(response['data'], 'to_dict') else None,
                            "schema_context": response['schema_context'],
                            "data_refreshed": response['data_refreshed']
                        }
                    )
                else:
                    error_msg = f"❌ **Error:** {response['error']}"
                    save_message("assistant", error_msg)
                
                # Rerun to show new messages and reset voice input
                st.rerun()
                
            except Exception as e:
                st.error(f"❌ Transcription failed: {str(e)}")
                # Increment counter even on error to allow retry
                st.session_state.voice_input_counter += 1

# Text input (traditional)
if st.session_state.data_loaded:
    st.markdown("---")
    st.markdown("### ⌨️ Text Input")

    if prompt := st.chat_input("Ask a question about your data..."):
        # Add user message to chat using save_message
        save_message("user", prompt)
        
        # Display user message
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Process query and display response
        with st.chat_message("assistant"):
            with st.spinner("🤔 Thinking..."):
                response = process_query(prompt)
            
            if response['success']:
                # Display explanation
                st.markdown(response['explanation'])
                
                # Store message with metadata using save_message
                save_message(
                    "assistant",
                    response['explanation'],
                    {
                        "plan": response['plan'],
                        "data": response['data'].to_dict() if hasattr(response['data'], 'to_dict') else None,
                        "schema_context": response['schema_context'],
                        "data_refreshed": response['data_refreshed']
                    }
                )
                
                # Show data refreshed notification
                if response['data_refreshed']:
                    st.info("🔄 Data was automatically refreshed before processing this query")
                
                # Voice output button
                if st.button("🔊 Listen to Answer", key="voice_out_new"):
                    with st.spinner("Generating audio..."):
                        try:
                            audio_bytes = text_to_speech(response['explanation'])
                            st.audio(audio_bytes, format="audio/mp3")
                        except Exception as e:
                            st.error(f"Voice output failed: {str(e)}")
                
                # Expandable sections
                with st.expander("📋 Query Plan"):
                    st.json(response['plan'])
                
                # Handle data - check if DataFrame has data
                if response.get('data') is not None and hasattr(response['data'], 'empty') and not response['data'].empty:
                    with st.expander("📊 Data"):
                        st.dataframe(response['data'], use_container_width=True)
                
                with st.expander("🗂️ Schema Context"):
                    for i, item in enumerate(response['schema_context'], 1):
                        st.markdown(f"**{i}.** {item['text']}")
            else:
                # Display error
                error_msg = f"❌ **Error:** {response['error']}"
                st.error(error_msg)
                
                # Store error message using save_message
                save_message("assistant", error_msg)
                
                # Show exception details in expander
                if 'exception' in response:
                    with st.expander("🔍 Error Details"):
                        st.exception(response['exception'])
else:
    st.info("👆 Please load your Google Sheets data above to start querying!")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; font-size: 0.875rem;'>
    <p><strong>System Status:</strong> ✅ Using Hugging Face Embeddings (No ONNX) | 🎤 Voice Enabled (ElevenLabs + gTTS) | 🌐 Multilingual</p>
    <p>Streamlit-safe | Windows-safe | No Visual C++ required</p>
</div>
""", unsafe_allow_html=True)
