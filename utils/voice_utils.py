"""
Voice utilities for ElevenLabs integration.
Provides speech-to-text and text-to-speech functionality.
"""

import os
from elevenlabs.client import ElevenLabs
import tempfile
from pathlib import Path
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Get API key from environment
ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY")

def transcribe_audio(audio_file_path: str) -> str:
    """
    Transcribe audio file to text using ElevenLabs Scribe v1.
    
    Args:
        audio_file_path: Path to audio file
        
    Returns:
        Transcribed text
    """
    try:
        client = ElevenLabs(api_key=ELEVENLABS_API_KEY)
        
        # Use Scribe v1 for transcription with explicit English language
        with open(audio_file_path, 'rb') as audio_file:
            # Correct API call for speech-to-text
            result = client.speech_to_text.convert(
                file=audio_file,
                model_id="scribe_v1",
                language_code="en"  # Force English instead of auto-detect
            )
        
        # Extract text from result
        if hasattr(result, 'text'):
            return result.text
        elif isinstance(result, dict):
            return result.get('text', str(result))
        else:
            return str(result)
            
    except Exception as e:
        raise Exception(f"Transcription failed: {str(e)}")

def text_to_speech(text: str, voice_id: str = "pNInz6obpgDQGcFmaJgB") -> bytes:
    """
    Convert text to speech using ElevenLabs multilingual TTS.
    Supports Tamil and English with high quality voice.
    
    Args:
        text: Text to convert to speech
        voice_id: ElevenLabs voice ID (default: Adam - multilingual)
        
    Returns:
        Audio bytes (MP3 format)
    """
    try:
        from elevenlabs.client import ElevenLabs
        from langdetect import detect, LangDetectException
        import io
        import re
        
        # Initialize ElevenLabs client
        client = ElevenLabs(api_key=ELEVENLABS_API_KEY)
        
        # Detect language from text
        language_code = "en"  # Default
        try:
            # Check if text contains Tamil characters
            tamil_pattern = re.compile(r'[\u0B80-\u0BFF]')
            has_tamil = bool(tamil_pattern.search(text))
            
            if has_tamil:
                language_code = "ta"  # Tamil
            else:
                # Otherwise, try to detect language
                detected_lang = detect(text)
                if detected_lang == 'ta':
                    language_code = "ta"
                elif detected_lang in ['en', 'en-us', 'en-gb']:
                    language_code = "en"
        except (LangDetectException, Exception):
            pass
        
        # Use ElevenLabs multilingual v2 model for better Tamil support
        # Using streaming for lower latency
        audio_stream = client.text_to_speech.convert(
            voice_id=voice_id,  # Adam voice supports multilingual
            text=text,
            model_id="eleven_multilingual_v2",  # Best for Tamil
            output_format="mp3_44100_128",  # High quality, reasonable size
        )
        
        # Collect audio chunks
        audio_bytes = b""
        for chunk in audio_stream:
            if chunk:
                audio_bytes += chunk
        
        return audio_bytes
        
    except Exception as e:
        # Fallback to gTTS if ElevenLabs fails
        print(f"ElevenLabs TTS failed, falling back to gTTS: {e}")
        return _fallback_gtts(text)


def _fallback_gtts(text: str) -> bytes:
    """
    Fallback to Google TTS if ElevenLabs fails.
    """
    try:
        from gtts import gTTS
        from langdetect import detect, LangDetectException
        import io
        import re
        
        # Detect language
        lang = 'en'
        try:
            tamil_pattern = re.compile(r'[\u0B80-\u0BFF]')
            if tamil_pattern.search(text):
                lang = 'ta'
            else:
                detected = detect(text)
                if detected == 'ta':
                    lang = 'ta'
        except:
            pass
        
        # Create TTS
        tts = gTTS(text=text, lang=lang, slow=False)
        
        # Save to bytes
        audio_fp = io.BytesIO()
        tts.write_to_fp(audio_fp)
        audio_fp.seek(0)
        
        return audio_fp.read()
        
    except Exception as e:
        raise Exception(f"Both ElevenLabs and gTTS failed: {str(e)}")

def save_audio_temp(audio_bytes: bytes) -> str:
    """
    Save audio bytes to a temporary file.
    
    Args:
        audio_bytes: Audio data
        
    Returns:
        Path to temporary audio file
    """
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.mp3')
    temp_file.write(audio_bytes)
    temp_file.close()
    return temp_file.name
