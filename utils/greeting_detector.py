"""
Greeting Detector

Detects casual greetings and returns appropriate responses.
Prevents unnecessary sheet queries for simple greetings.
Enhanced with dynamic responses based on greeting type.
"""

import time
import random
import re
from datetime import datetime

# Greeting patterns with categories (case-insensitive)
GREETING_CATEGORIES = {
    'casual': [
        r'\b(hi|hello|hey|hola|yo)\b',
        r'^(hi|hello|hey)$',
    ],
    'formal': [
        r'\b(good\s+(morning|afternoon|evening|day))\b',
        r'\b(greetings)\b',
    ],
    'cultural': [
        r'\b(namaste|namaskar|नमस्ते)\b',  # Hindi
        r'\b(vanakkam|வணக்கம்|vanakam)\b',  # Tamil
        r'\b(salaam|salam|सलाम|assalamu\s+alaikum)\b',  # Arabic/Urdu
        r'\b(bonjour|bon\s+jour)\b',  # French
        r'\b(konnichiwa|こんにちは)\b',  # Japanese
        r'\b(ni\s+hao|你好)\b',  # Chinese
    ],
    'casual_question': [
        r'\b(what\'?s\s+up|whats\s+up|wassup|sup)\b',
        r'\b(how\s+(are\s+you|r\s+u|are\s+ya))\b',
        r'\b(how\'?s\s+it\s+going)\b',
    ],
    'time_based': [
        r'\b(good\s+morning)\b',
        r'\b(good\s+afternoon)\b',
        r'\b(good\s+evening)\b',
        r'\b(good\s+night)\b',
    ]
}

# Dynamic response templates by category
RESPONSE_TEMPLATES = {
    'casual': [
        "Hi there! I'm Kiwi, your analytics assistant. What would you like to know about your data?",
        "Hello! I'm Kiwi. Ready to help you analyze your sheets. What can I do for you?",
        "Hey! Kiwi here. How can I assist you with your data today?",
    ],
    'formal': [
        "Good day! I'm Kiwi, your analytics assistant. How may I help you with your spreadsheet today?",
        "Greetings! I'm Kiwi. I'm here to help you analyze your data. What would you like to explore?",
    ],
    'namaste': [
        "Namaste! I'm Kiwi, your analytics assistant. How can I help you today?",
        "Namaste! I'm Kiwi. Ready to help you with your data analysis. What can I do for you?",
    ],
    'vanakkam': [
        "Vanakkam! I'm Kiwi, your analytics assistant. How can I help you today?",
        "Vanakkam! I'm Kiwi. Ready to help you with your data analysis. What can I do for you?",
    ],
    'salaam': [
        "Salaam! I'm Kiwi, your analytics assistant. How may I assist you with your sheets?",
        "Salaam! I'm Kiwi. Ready to help you analyze your data. What can I do for you?",
    ],
    'bonjour': [
        "Bonjour! I'm Kiwi, your analytics assistant. How can I help you today?",
        "Bonjour! I'm Kiwi. Ready to help you with your data. What can I do for you?",
    ],
    'konnichiwa': [
        "Konnichiwa! I'm Kiwi, your analytics assistant. How can I help you today?",
        "Konnichiwa! I'm Kiwi. Ready to help you analyze your sheets. What can I do for you?",
    ],
    'nihao': [
        "Ni hao! I'm Kiwi, your analytics assistant. How can I help you today?",
        "Ni hao! I'm Kiwi. Ready to help you with your data. What can I do for you?",
    ],
    'casual_question': [
        "I'm doing great, thanks for asking! I'm Kiwi, and I'm here to help you analyze your data. What would you like to know?",
        "All good here! I'm Kiwi, your analytics assistant. Ready to dive into your sheets. What can I help you with?",
    ],
    'morning': [
        "Good morning! I'm Kiwi, your analytics assistant. Ready to start the day with some data insights?",
        "Morning! I'm Kiwi. Let's make today productive. What would you like to analyze?",
    ],
    'afternoon': [
        "Good afternoon! I'm Kiwi, your analytics assistant. How can I help you with your data today?",
        "Afternoon! I'm Kiwi. Ready to help you explore your sheets. What can I do for you?",
    ],
    'evening': [
        "Good evening! I'm Kiwi, your analytics assistant. How can I assist you with your data tonight?",
        "Evening! I'm Kiwi. Let's analyze your data. What would you like to know?",
    ],
    'night': [
        "Good night! I'm Kiwi, your analytics assistant. Working late? How can I help with your data?",
        "Hello! I'm Kiwi. Burning the midnight oil? Let me help you with your sheets. What do you need?",
    ]
}


def is_greeting(text: str) -> bool:
    """
    Check if the input is a casual greeting.
    
    Args:
        text: User input text
        
    Returns:
        True if it's a greeting, False otherwise
    """
    if not text or len(text.strip()) == 0:
        return False
    
    text_lower = text.lower().strip()
    
    # Check against all patterns
    for category, patterns in GREETING_CATEGORIES.items():
        for pattern in patterns:
            if re.search(pattern, text_lower, re.IGNORECASE):
                # Make sure it's not part of a longer question
                # e.g., "Hi, what is the total sales?" should not be treated as just a greeting
                if len(text_lower.split()) <= 5:  # Allow slightly longer greetings
                    return True
    
    return False


def _detect_greeting_category(text: str) -> str:
    """
    Detect which category of greeting was used.
    
    Args:
        text: User input text
        
    Returns:
        Category name or 'casual' as default
    """
    text_lower = text.lower().strip()
    
    # Check time-based greetings first for specific responses
    if re.search(r'\b(good\s+morning)\b', text_lower):
        return 'morning'
    elif re.search(r'\b(good\s+afternoon)\b', text_lower):
        return 'afternoon'
    elif re.search(r'\b(good\s+evening)\b', text_lower):
        return 'evening'
    elif re.search(r'\b(good\s+night)\b', text_lower):
        return 'night'
    
    # Check for specific cultural greetings
    if re.search(r'\b(namaste|namaskar|नमस्ते)\b', text_lower):
        return 'namaste'
    elif re.search(r'\b(vanakkam|வணக்கம்|vanakam)\b', text_lower):
        return 'vanakkam'
    elif re.search(r'\b(salaam|salam|सलाम|assalamu\s+alaikum)\b', text_lower):
        return 'salaam'
    elif re.search(r'\b(bonjour|bon\s+jour)\b', text_lower):
        return 'bonjour'
    elif re.search(r'\b(konnichiwa|こんにちは)\b', text_lower):
        return 'konnichiwa'
    elif re.search(r'\b(ni\s+hao|你好)\b', text_lower):
        return 'nihao'
    
    # Check other categories
    for category, patterns in GREETING_CATEGORIES.items():
        if category in ['time_based', 'cultural']:
            continue  # Already handled above
        for pattern in patterns:
            if re.search(pattern, text_lower, re.IGNORECASE):
                return category
    
    return 'casual'  # Default


def get_greeting_response(user_input: str = "") -> str:
    """
    Get a dynamic greeting response based on the type of greeting.
    
    Args:
        user_input: The user's greeting text (optional, for context)
        
    Returns:
        Greeting message
    """
    # Add a small delay to make it feel more natural (1-2 seconds)
    delay = random.uniform(1.0, 2.0)
    time.sleep(delay)
    
    # Detect greeting category
    category = _detect_greeting_category(user_input) if user_input else 'casual'
    
    # Get appropriate response template
    templates = RESPONSE_TEMPLATES.get(category, RESPONSE_TEMPLATES['casual'])
    
    # Randomly select a response from the category
    response = random.choice(templates)
    
    return response
