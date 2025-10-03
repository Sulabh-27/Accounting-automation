"""
CSV utilities with encoding detection and error handling
"""
import pandas as pd
import chardet
import os
import tempfile
from typing import Optional


def _resolve_file_path(file_path: str) -> str:
    """
    Resolve file path - download from Supabase if needed.
    
    Args:
        file_path: Local path or Supabase storage path
        
    Returns:
        Local file path that can be read
    """
    print(f"🔍 Resolving file path: {file_path}")
    
    # If it's a local file that exists, return as-is
    if os.path.exists(file_path):
        print(f"✅ Local file exists: {file_path}")
        return file_path
    
    # If it looks like a Supabase storage path, try to download it
    if "/" in file_path and not os.path.exists(file_path):
        print(f"🌐 Attempting to download from Supabase: {file_path}")
        try:
            from .supabase_client import SupabaseClientWrapper
            
            # Create a temporary local file path
            temp_dir = tempfile.gettempdir()
            filename = os.path.basename(file_path)
            local_temp_path = os.path.join(temp_dir, f"supabase_{filename}")
            print(f"📁 Temp download path: {local_temp_path}")
            
            # Try to download from Supabase
            supabase = SupabaseClientWrapper()
            downloaded_path = supabase.download_file(file_path, local_temp_path)
            
            if os.path.exists(downloaded_path):
                print(f"📥 Successfully downloaded from Supabase: {file_path} -> {downloaded_path}")
                return downloaded_path
            else:
                print(f"⚠️  Downloaded file not found at: {downloaded_path}")
                # Try a small delay and retry once
                import time
                time.sleep(1)
                downloaded_path = supabase.download_file(file_path, local_temp_path)
                if os.path.exists(downloaded_path):
                    print(f"📥 Successfully downloaded from Supabase (retry): {file_path} -> {downloaded_path}")
                    return downloaded_path
                else:
                    print(f"⚠️  Download failed even after retry")
                    return file_path
                
        except Exception as e:
            print(f"⚠️  Error downloading from Supabase: {e}")
            import traceback
            traceback.print_exc()
            return file_path
    
    print(f"⚠️  File not found and not a Supabase path: {file_path}")
    return file_path


def safe_read_csv(file_path: str, encoding: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """
    Safely read CSV files with automatic encoding detection.
    
    Args:
        file_path: Path to the CSV file
        encoding: Specific encoding to use (optional)
        **kwargs: Additional arguments to pass to pd.read_csv()
    
    Returns:
        DataFrame with the CSV data
    
    Raises:
        Exception: If file cannot be read with any encoding
    """
    
    # Resolve the file path (download from Supabase if needed)
    resolved_path = _resolve_file_path(file_path)
    
    # List of common encodings to try
    encodings_to_try = [
        'utf-8',
        'utf-8-sig',  # UTF-8 with BOM
        'latin-1',    # ISO-8859-1
        'cp1252',     # Windows-1252
        'ascii'
    ]
    
    # If specific encoding provided, try it first
    if encoding:
        encodings_to_try.insert(0, encoding)
    
    # Try to detect encoding automatically
    try:
        with open(resolved_path, 'rb') as f:
            raw_data = f.read(10000)  # Read first 10KB for detection
            detected = chardet.detect(raw_data)
            if detected['encoding'] and detected['confidence'] > 0.7:
                detected_encoding = detected['encoding']
                if detected_encoding not in encodings_to_try:
                    encodings_to_try.insert(0, detected_encoding)
                print(f"🔍 Detected encoding: {detected_encoding} (confidence: {detected['confidence']:.2f})")
    except Exception as e:
        print(f"⚠️  Encoding detection failed: {e}")
    
    # Try each encoding until one works
    last_error = None
    for enc in encodings_to_try:
        try:
            print(f"🔧 Trying to read CSV with encoding: {enc}")
            
            # First try with default settings
            try:
                df = pd.read_csv(resolved_path, encoding=enc, **kwargs)
                print(f"✅ Successfully read CSV with encoding: {enc}")
                return df
            except pd.errors.ParserError as parse_error:
                print(f"⚠️  Parser error with {enc}, trying with error handling...")
                # Try with error handling for malformed CSV
                df = pd.read_csv(resolved_path, encoding=enc, on_bad_lines='skip', **kwargs)
                print(f"✅ Successfully read CSV with encoding: {enc} (skipped bad lines)")
                return df
                
        except UnicodeDecodeError as e:
            last_error = e
            print(f"❌ Failed with {enc}: {str(e)[:100]}...")
            continue
        except Exception as e:
            last_error = e
            print(f"❌ Failed with {enc}: {str(e)[:100]}...")
            continue
    
    # If all encodings failed, raise the last error
    raise Exception(f"Could not read CSV file {resolved_path} with any encoding. Last error: {last_error}")


def safe_read_excel_or_csv(file_path: str, **kwargs) -> pd.DataFrame:
    """
    Safely read Excel or CSV files with proper encoding handling.
    
    Args:
        file_path: Path to the file
        **kwargs: Additional arguments
    
    Returns:
        DataFrame with the file data
    """
    # Resolve the file path (download from Supabase if needed)
    resolved_path = _resolve_file_path(file_path)
    
    if resolved_path.lower().endswith(('.xlsx', '.xls')):
        return pd.read_excel(resolved_path, **kwargs)
    else:
        return safe_read_csv(resolved_path, **kwargs)
