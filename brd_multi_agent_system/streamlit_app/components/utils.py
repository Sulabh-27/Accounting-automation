"""
Utility functions for Streamlit components
"""

import streamlit as st
import os
from pathlib import Path

def setup_page_config():
    """Configure Streamlit page settings"""
    st.set_page_config(
        page_title="Multi-Agent Accounting System",
        page_icon="🎉",
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={
            'Get Help': 'https://github.com/your-repo/accounting-system',
            'Report a bug': "https://github.com/your-repo/accounting-system/issues",
            'About': "# Multi-Agent Accounting System\nEnterprise e-commerce accounting automation"
        }
    )

def load_custom_css():
    """Load custom CSS styles"""
    css = """
    <style>
    /* Main theme colors */
    :root {
        --primary-color: #1f77b4;
        --secondary-color: #ff7f0e;
        --success-color: #2ca02c;
        --warning-color: #ff7f0e;
        --error-color: #d62728;
        --background-color: #f8f9fa;
        --card-background: #ffffff;
        --text-color: #333333;
        --border-color: #e1e5e9;
    }
    
    /* Main header styling */
    .main-header {
        background: linear-gradient(90deg, var(--primary-color), var(--secondary-color));
        color: white;
        padding: 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        text-align: center;
    }
    
    .main-header h1 {
        margin: 0;
        font-size: 2.5rem;
        font-weight: 700;
    }
    
    .main-header p {
        margin: 0.5rem 0 0 0;
        font-size: 1.2rem;
        opacity: 0.9;
    }
    
    /* Metric cards */
    .metric-card {
        background: var(--card-background);
        padding: 1.5rem;
        border-radius: 10px;
        border: 1px solid var(--border-color);
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        text-align: center;
        margin-bottom: 1rem;
    }
    
    .metric-icon {
        font-size: 2rem;
        margin-bottom: 0.5rem;
    }
    
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: var(--primary-color);
        margin: 0.5rem 0;
    }
    
    .metric-label {
        font-size: 0.9rem;
        color: var(--text-color);
        margin-bottom: 0.5rem;
    }
    
    .metric-change {
        font-size: 0.8rem;
        color: var(--success-color);
    }
    
    /* Status indicators */
    .status-indicator {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        padding: 0.5rem;
        background: var(--card-background);
        border-radius: 5px;
        border: 1px solid var(--border-color);
    }
    
    .status-dot {
        width: 10px;
        height: 10px;
        border-radius: 50%;
    }
    
    .status-online {
        background-color: var(--success-color);
        animation: pulse 2s infinite;
    }
    
    .status-warning {
        background-color: var(--warning-color);
    }
    
    .status-error {
        background-color: var(--error-color);
    }
    
    @keyframes pulse {
        0% { opacity: 1; }
        50% { opacity: 0.5; }
        100% { opacity: 1; }
    }
    
    /* Activity items */
    .activity-item {
        display: flex;
        align-items: center;
        gap: 1rem;
        padding: 0.75rem;
        background: var(--card-background);
        border-radius: 8px;
        border: 1px solid var(--border-color);
        margin-bottom: 0.5rem;
    }
    
    .activity-icon {
        font-size: 1.5rem;
        width: 40px;
        text-align: center;
    }
    
    .activity-content {
        flex: 1;
    }
    
    .activity-title {
        font-weight: 600;
        color: var(--text-color);
        font-size: 0.9rem;
    }
    
    .activity-time {
        color: #666;
        font-size: 0.8rem;
    }
    
    /* Processing steps */
    .processing-step {
        display: flex;
        align-items: center;
        gap: 1rem;
        padding: 0.75rem;
        background: var(--card-background);
        border-radius: 8px;
        border: 1px solid var(--border-color);
        margin-bottom: 0.5rem;
    }
    
    .step-status {
        width: 20px;
        text-align: center;
    }
    
    .step-name {
        flex: 1;
        font-weight: 500;
    }
    
    .step-icon {
        font-size: 1.2rem;
    }
    
    /* File upload area */
    .upload-area {
        border: 2px dashed var(--border-color);
        border-radius: 10px;
        padding: 2rem;
        text-align: center;
        background: var(--background-color);
        transition: all 0.3s ease;
    }
    
    .upload-area:hover {
        border-color: var(--primary-color);
        background: rgba(31, 119, 180, 0.05);
    }
    
    /* Exception badges */
    .exception-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 600;
        margin: 0.25rem;
    }
    
    .exception-critical {
        background: rgba(214, 39, 40, 0.1);
        color: var(--error-color);
        border: 1px solid var(--error-color);
    }
    
    .exception-warning {
        background: rgba(255, 127, 14, 0.1);
        color: var(--warning-color);
        border: 1px solid var(--warning-color);
    }
    
    .exception-info {
        background: rgba(31, 119, 180, 0.1);
        color: var(--primary-color);
        border: 1px solid var(--primary-color);
    }
    
    /* Progress indicators */
    .progress-container {
        background: var(--background-color);
        border-radius: 10px;
        padding: 1rem;
        margin: 1rem 0;
    }
    
    .progress-step {
        display: flex;
        align-items: center;
        gap: 1rem;
        padding: 0.5rem 0;
    }
    
    .progress-number {
        width: 30px;
        height: 30px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        font-weight: 600;
        font-size: 0.9rem;
    }
    
    .progress-completed {
        background: var(--success-color);
        color: white;
    }
    
    .progress-active {
        background: var(--warning-color);
        color: white;
    }
    
    .progress-pending {
        background: var(--border-color);
        color: var(--text-color);
    }
    
    /* Tables */
    .data-table {
        background: var(--card-background);
        border-radius: 10px;
        overflow: hidden;
        border: 1px solid var(--border-color);
    }
    
    /* Buttons */
    .stButton > button {
        border-radius: 8px;
        border: none;
        background: var(--primary-color);
        color: white;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        background: #1565c0;
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        background: var(--card-background);
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

def format_currency(amount):
    """Format currency with Indian formatting"""
    return f"₹{amount:,.2f}"

def format_number(number):
    """Format numbers with commas"""
    return f"{number:,}"

def get_status_color(status):
    """Get color for status indicators"""
    colors = {
        "success": "#2ca02c",
        "warning": "#ff7f0e", 
        "error": "#d62728",
        "info": "#1f77b4",
        "pending": "#9467bd"
    }
    return colors.get(status, "#666666")

def create_download_link(file_path, file_name):
    """Create a download link for files"""
    if os.path.exists(file_path):
        with open(file_path, "rb") as f:
            data = f.read()
        return st.download_button(
            label=f"Download {file_name}",
            data=data,
            file_name=file_name,
            mime="application/octet-stream"
        )
    else:
        st.error(f"File not found: {file_name}")
        return False
