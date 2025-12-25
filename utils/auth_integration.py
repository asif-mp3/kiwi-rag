"""
Authentication Integration for Streamlit App
This module provides a simple wrapper to add authentication without breaking existing code
"""

import streamlit as st
from utils.supabase_auth import (
    init_auth_state,
    check_auth_enabled,
    handle_oauth_callback,
    show_login_page,
    show_user_info_sidebar,
    get_user_id,
    get_user_name
)


def setup_authentication():
    """
    Setup authentication for the app
    Returns True if user is authenticated (or auth is disabled)
    Returns False if authentication is required but user is not logged in
    """
    # Initialize auth state
    init_auth_state()
    
    # Check if auth is enabled
    if not check_auth_enabled():
        # Auth is disabled, proceed normally
        return True
    
    # Handle OAuth callback
    handle_oauth_callback()
    
    # Check if user is authenticated
    if not st.session_state.get('authenticated', False):
        # Show login page
        show_login_page()
        return False
    
    # User is authenticated
    return True


def add_auth_sidebar():
    """Add authentication info to sidebar"""
    if check_auth_enabled() and st.session_state.get('authenticated', False):
        show_user_info_sidebar()


def get_current_user_id():
    """Get current user ID (returns None if auth is disabled)"""
    if not check_auth_enabled():
        return None
    return get_user_id()


def get_current_user_name():
    """Get current user name"""
    if not check_auth_enabled():
        return "Guest"
    return get_user_name()
