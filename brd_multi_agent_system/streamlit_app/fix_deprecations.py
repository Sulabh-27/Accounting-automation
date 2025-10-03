"""
Quick script to fix Streamlit deprecation warnings
"""

import os
import re

def fix_use_container_width(directory):
    """Replace use_container_width with width parameter"""
    
    # Pattern to match width='stretch'
    pattern_true = r'width='stretch''
    replacement_true = "width='stretch'"
    
    # Pattern to match width='content'
    pattern_false = r'width='content''
    replacement_false = "width='content'"
    
    files_processed = 0
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    original_content = content
                    
                    # Replace both patterns
                    content = re.sub(pattern_true, replacement_true, content)
                    content = re.sub(pattern_false, replacement_false, content)
                    
                    if content != original_content:
                        with open(file_path, 'w', encoding='utf-8') as f:
                            f.write(content)
                        
                        files_processed += 1
                        print(f"Fixed: {file_path}")
                
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
    
    print(f"\nTotal files processed: {files_processed}")

if __name__ == "__main__":
    # Fix all files in the streamlit_app directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    fix_use_container_width(current_dir)
    print("All deprecation warnings fixed!")
