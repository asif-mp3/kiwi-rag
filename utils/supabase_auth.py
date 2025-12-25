"""
Supabase Authentication Module
Handles user authentication with Google OAuth
"""

import streamlit as st
from supabase import create_client, Client
import os
from dotenv import load_dotenv
from typing import Optional, Dict, Any
import json

# Load environment variables
load_dotenv()

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
ENABLE_AUTH = os.getenv("ENABLE_AUTH", "false").lower() == "true"


@st.cache_resource
def get_supabase_client() -> Client:
    """Initialize and return Supabase client"""
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        raise ValueError("Supabase credentials not found in environment variables")
    
    return create_client(SUPABASE_URL, SUPABASE_ANON_KEY)


def init_auth_state():
    """Initialize authentication session state"""
    if 'auth_initialized' not in st.session_state:
        st.session_state.auth_initialized = True
        st.session_state.authenticated = False
        st.session_state.user = None
        st.session_state.user_profile = None


def check_auth_enabled() -> bool:
    """Check if authentication is enabled"""
    # If Supabase credentials are missing, auth is automatically disabled
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        return False
    
    return ENABLE_AUTH


def get_google_oauth_url() -> str:
    """Get Google OAuth URL for login"""
    try:
        supabase = get_supabase_client()
        
        # Get the current URL for redirect
        # In production, this should be your deployed URL
        redirect_url = os.getenv("REDIRECT_URL", "http://localhost:8501")
        
        response = supabase.auth.sign_in_with_oauth({
            "provider": "google",
            "options": {
                "redirect_to": redirect_url
            }
        })
        
        return response.url
    except Exception as e:
        st.error(f"Failed to get OAuth URL: {str(e)}")
        return None


def handle_oauth_callback():
    """Handle OAuth callback from Google - Direct REST API implementation"""
    # Skip if auth is disabled
    if not check_auth_enabled():
        return False
    
    try:
        # Get query parameters
        params = st.query_params
        
        # Check if we already have an authenticated session
        if st.session_state.get('authenticated') and st.session_state.get('user'):
            return True
        
        # Handle authorization code (PKCE flow)
        if 'code' in params:
            auth_code = params['code']
            
            # Check if we've already processed this code
            if st.session_state.get('processed_code') == auth_code:
                return st.session_state.get('authenticated', False)
            
            st.info("🔄 Authenticating...")
            
            try:
                # Use direct REST API call instead of SDK method
                import requests
                
                # Exchange code for session using Supabase REST API
                url = f"{SUPABASE_URL}/auth/v1/token?grant_type=authorization_code"
                headers = {
                    "apikey": SUPABASE_ANON_KEY,
                    "Content-Type": "application/json"
                }
                data = {
                    "auth_code": auth_code
                }
                
                response = requests.post(url, headers=headers, json=data)
                
                if response.status_code == 200:
                    session_data = response.json()
                    
                    # Extract user and tokens
                    if 'user' in session_data and 'access_token' in session_data:
                        # Mark this code as processed
                        st.session_state.processed_code = auth_code
                        
                        # Set authentication state
                        st.session_state.authenticated = True
                        st.session_state.user = type('User', (), session_data['user'])()  # Convert dict to object
                        st.session_state.access_token = session_data['access_token']
                        st.session_state.refresh_token = session_data.get('refresh_token')
                        
                        # Store user data for profile
                        user_data = session_data['user']
                        st.session_state.user.id = user_data.get('id')
                        st.session_state.user.email = user_data.get('email')
                        st.session_state.user.user_metadata = user_data.get('user_metadata', {})
                        
                        # Load user profile
                        load_user_profile()
                        
                        # Clear query parameters
                        st.query_params.clear()
                        
                        st.success("✅ Login successful!")
                        
                        # Force rerun
                        st.rerun()
                        
                        return True
                    else:
                        st.error("❌ Invalid response from authentication server")
                        st.write("DEBUG: Response:", session_data)
                        st.query_params.clear()
                        return False
                else:
                    st.error(f"❌ Authentication failed: {response.status_code}")
                    st.write("DEBUG: Response:", response.text)
                    st.query_params.clear()
                    return False
                    
            except Exception as e:
                st.error(f"❌ Authentication error: {str(e)}")
                st.write("DEBUG: Exception type:", type(e).__name__)
                import traceback
                st.code(traceback.format_exc())
                st.query_params.clear()
                return False
        
        # Check for existing session
        if st.session_state.get('access_token'):
            try:
                supabase = get_supabase_client()
                # Verify token is still valid
                user_response = supabase.auth.get_user(st.session_state.access_token)
                if user_response and user_response.user:
                    st.session_state.authenticated = True
                    st.session_state.user = user_response.user
                    return True
            except:
                # Token expired or invalid
                st.session_state.authenticated = False
                st.session_state.user = None
                st.session_state.access_token = None
            
    except Exception as e:
        st.error(f"❌ OAuth callback error: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
        return False
    
    return False


def load_user_profile():
    """Load user profile from database"""
    if not st.session_state.user:
        return None
    
    try:
        supabase = get_supabase_client()
        user_id = st.session_state.user.id
        
        response = supabase.table('user_profiles').select('*').eq('id', user_id).execute()
        
        if response.data and len(response.data) > 0:
            st.session_state.user_profile = response.data[0]
            return response.data[0]
        else:
            # Profile doesn't exist, create it
            create_user_profile()
            return st.session_state.user_profile
            
    except Exception as e:
        print(f"Error loading user profile: {str(e)}")
        return None


def create_user_profile():
    """Create user profile in database"""
    if not st.session_state.user:
        return None
    
    try:
        supabase = get_supabase_client()
        user = st.session_state.user
        
        profile_data = {
            'id': user.id,
            'email': user.email,
            'full_name': user.user_metadata.get('full_name') or user.user_metadata.get('name') or 'User',
            'avatar_url': user.user_metadata.get('avatar_url') or user.user_metadata.get('picture'),
            'preferences': {}
        }
        
        response = supabase.table('user_profiles').insert(profile_data).execute()
        
        if response.data:
            st.session_state.user_profile = response.data[0]
            return response.data[0]
            
    except Exception as e:
        print(f"Error creating user profile: {str(e)}")
        return None


def logout():
    """Logout user and clear session"""
    try:
        supabase = get_supabase_client()
        supabase.auth.sign_out()
    except:
        pass
    
    # Clear session state
    st.session_state.authenticated = False
    st.session_state.user = None
    st.session_state.user_profile = None
    
    # Clear messages and conversations
    if 'messages' in st.session_state:
        st.session_state.messages = []
    
    st.rerun()


def get_user_id() -> Optional[str]:
    """Get current user ID"""
    if st.session_state.get('authenticated') and st.session_state.get('user'):
        return st.session_state.user.id
    return None


def get_user_email() -> Optional[str]:
    """Get current user email"""
    if st.session_state.get('authenticated') and st.session_state.get('user'):
        return st.session_state.user.email
    return None


def get_user_name() -> Optional[str]:
    """Get current user name"""
    if st.session_state.get('user_profile'):
        return st.session_state.user_profile.get('full_name', 'User')
    elif st.session_state.get('user'):
        return st.session_state.user.user_metadata.get('name', 'User')
    return 'User'


def get_user_avatar() -> Optional[str]:
    """Get current user avatar URL"""
    if st.session_state.get('user_profile'):
        return st.session_state.user_profile.get('avatar_url')
    elif st.session_state.get('user'):
        return st.session_state.user.user_metadata.get('picture')
    return None


def require_auth(func):
    """Decorator to require authentication for a function"""
    def wrapper(*args, **kwargs):
        if not check_auth_enabled():
            # Auth disabled, proceed normally
            return func(*args, **kwargs)
        
        if not st.session_state.get('authenticated', False):
            st.warning("⚠️ Please login to access this feature")
            return None
        
        return func(*args, **kwargs)
    
    return wrapper


def show_login_page():
    """Display login page"""
    st.title("🥝 Kiwi-RAG Analytics Chatbot")
    st.markdown("### Welcome! Please login to continue")
    
    st.markdown("")
    st.markdown("")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        if st.button("🔐 Sign in with Google", use_container_width=True, type="primary"):
            oauth_url = get_google_oauth_url()
            if oauth_url:
                # Redirect to OAuth URL
                st.markdown(
                    f'<meta http-equiv="refresh" content="0;url={oauth_url}">',
                    unsafe_allow_html=True
                )
                st.info("Redirecting to Google Sign-In...")
    
    st.markdown("")
    st.markdown("")
    
    # Info about the app
    with st.expander("ℹ️ About Kiwi-RAG"):
        st.markdown("""
        **Kiwi-RAG Analytics Chatbot** is an AI-powered tool for analyzing Google Sheets data.
        
        **Features:**
        - 🔍 Natural language queries in Tamil & English
        - 🎤 Voice input and output
        - 📊 Advanced analytics with DuckDB
        - 🧠 AI-powered query understanding
        - 💬 Conversation history
        
        **Secure & Private:**
        - Your data stays in your Google Sheets
        - Conversations are private to your account
        - No data is shared with third parties
        """)


def show_user_info_sidebar():
    """Show user info in sidebar"""
    if not st.session_state.get('authenticated'):
        return
    
    with st.sidebar:
        st.markdown("---")
        st.markdown("### 👤 User Profile")
        
        # User avatar and name
        avatar_url = get_user_avatar()
        user_name = get_user_name()
        user_email = get_user_email()
        
        if avatar_url:
            col1, col2 = st.columns([1, 3])
            with col1:
                st.image(avatar_url, width=50)
            with col2:
                st.markdown(f"**{user_name}**")
                st.markdown(f"<small>{user_email}</small>", unsafe_allow_html=True)
        else:
            st.markdown(f"**{user_name}**")
            st.markdown(f"<small>{user_email}</small>", unsafe_allow_html=True)
        
        st.markdown("")
        
        if st.button("🚪 Logout", use_container_width=True):
            logout()
