"""
Data Processing Pipeline Page
File upload, processing steps visualization, and real-time logs
"""

import streamlit as st
import sys
import os
from pathlib import Path
import subprocess
import time
import pandas as pd

# Add parent directory to path for imports
parent_dir = Path(__file__).parent.parent.parent
sys.path.append(str(parent_dir))

from streamlit_app.components.utils import setup_page_config, load_custom_css, format_currency
from streamlit_app.components.cards import create_progress_card, create_processing_timeline_card
from streamlit_app.data.sample_data import get_processing_status

def main():
    setup_page_config()
    load_custom_css()
    
    st.markdown("# 📊 Data Processing Pipeline")
    st.markdown("Upload and process e-commerce transaction files through the 8-part pipeline")
    
    # File upload section
    render_file_upload_section()
    
    st.markdown("---")
    
    # Processing visualization
    col1, col2 = st.columns([2, 1])
    
    with col1:
        render_processing_steps()
    
    with col2:
        render_processing_status()
    
    st.markdown("---")
    
    # Processing logs and results
    render_processing_logs()

def render_file_upload_section():
    """Render file upload interface"""
    st.markdown("## 📤 File Upload")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # File uploader
        uploaded_file = st.file_uploader(
            "Choose Excel/CSV file",
            type=['xlsx', 'xls', 'csv'],
            help="Upload Amazon MTR/STR, Flipkart, or Pepperfry transaction files"
        )
        
        if uploaded_file:
            st.success(f"✅ File uploaded: {uploaded_file.name}")
            
            # File info
            file_size = len(uploaded_file.getvalue())
            st.info(f"📁 File size: {file_size:,} bytes")
    
    with col2:
        # Processing options
        st.markdown("### ⚙️ Processing Options")
        
        channel = st.selectbox(
            "Channel",
            ["amazon", "flipkart", "pepperfry", "custom"],
            help="Select the e-commerce platform"
        )
        
        agent_type = st.selectbox(
            "Agent Type", 
            ["amazon_mtr", "amazon_str", "flipkart", "pepperfry", "universal"],
            help="Select the processing agent"
        )
        
        gstin = st.selectbox(
            "GSTIN",
            ["06ABGCS4796R1ZA", "07ABGCS4796R1Z8", "09ABGCS4796R1Z4", "24ABGCS4796R1ZC", "29ABGCS4796R1Z2"],
            help="Select company GSTIN"
        )
        
        month = st.date_input(
            "Processing Month",
            help="Month for which to process transactions"
        )
    
    # Processing controls
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🚀 Start Full Pipeline", type="primary", width='stretch'):
            if uploaded_file:
                start_processing(uploaded_file, channel, agent_type, gstin, month, full_pipeline=True)
            else:
                st.error("Please upload a file first")
    
    with col2:
        if st.button("⚡ Quick Process", width='stretch'):
            if uploaded_file:
                start_processing(uploaded_file, channel, agent_type, gstin, month, full_pipeline=False)
            else:
                st.error("Please upload a file first")
    
    with col3:
        if st.button("🧪 Validate Only", width='stretch'):
            if uploaded_file:
                validate_file(uploaded_file)
            else:
                st.error("Please upload a file first")

def render_processing_steps():
    """Render 8-step processing visualization"""
    st.markdown("## 🔄 Processing Steps")
    
    steps = [
        {
            "number": 1,
            "name": "Data Ingestion",
            "description": "Normalize Excel/CSV to standard schema",
            "status": "completed",
            "time": "2 min"
        },
        {
            "number": 2,
            "name": "Mapping & Enrichment", 
            "description": "Map SKUs to Final Goods and Ledgers",
            "status": "completed",
            "time": "1 min"
        },
        {
            "number": 3,
            "name": "Tax Computation",
            "description": "Calculate GST and generate invoices",
            "status": "completed", 
            "time": "30 sec"
        },
        {
            "number": 4,
            "name": "Pivot & Batch",
            "description": "Create summaries and GST rate batches",
            "status": "completed",
            "time": "45 sec"
        },
        {
            "number": 5,
            "name": "Tally Export",
            "description": "Generate X2Beta Excel files",
            "status": "in_progress",
            "time": "1 min"
        },
        {
            "number": 6,
            "name": "Expense Processing",
            "description": "Process seller invoices and expenses",
            "status": "pending",
            "time": "-"
        },
        {
            "number": 7,
            "name": "Exception Handling",
            "description": "Detect and resolve exceptions",
            "status": "pending",
            "time": "-"
        },
        {
            "number": 8,
            "name": "MIS & Audit",
            "description": "Generate reports and audit trail",
            "status": "pending",
            "time": "-"
        }
    ]
    
    for step in steps:
        status_color = {
            "completed": "#2ca02c",
            "in_progress": "#ff7f0e",
            "pending": "#9467bd"
        }[step["status"]]
        
        status_icon = {
            "completed": "✅",
            "in_progress": "🔄",
            "pending": "⏳"
        }[step["status"]]
        
        st.markdown(f"""
        <div class="processing-step">
            <div class="progress-number progress-{step['status']}">{step['number']}</div>
            <div class="step-content">
                <div class="step-name">{step['name']}</div>
                <div style="font-size: 0.9rem; color: #666;">{step['description']}</div>
            </div>
            <div class="step-time" style="color: {status_color};">{step['time']}</div>
            <div class="step-icon">{status_icon}</div>
        </div>
        """, unsafe_allow_html=True)
    
    # Overall progress
    completed_steps = sum(1 for step in steps if step["status"] == "completed")
    in_progress_steps = sum(1 for step in steps if step["status"] == "in_progress")
    total_progress = (completed_steps + in_progress_steps * 0.5) / len(steps)
    
    st.markdown("### 📈 Overall Progress")
    progress_bar = st.progress(total_progress)
    st.caption(f"{completed_steps} completed, {in_progress_steps} in progress, {len(steps) - completed_steps - in_progress_steps} pending")

def render_processing_status():
    """Render current processing status"""
    st.markdown("## 📋 Current Status")
    
    status = get_processing_status()
    
    # Current file info
    st.markdown(f"**Processing:** {status['current_file']}")
    st.markdown(f"**Step:** {status['step']}")
    st.markdown(f"**ETA:** {status['estimated_completion']}")
    
    # Progress metrics
    create_progress_card(
        "Records Processed",
        status['records_processed'],
        698,  # total
        f"Processing at 25 records/sec"
    )
    
    # Status indicators
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Errors", status['errors'], delta=0, delta_color="inverse")
    
    with col2:
        st.metric("Warnings", status['warnings'], delta=-2, delta_color="inverse")
    
    with col3:
        st.metric("Success Rate", "98.2%", delta="0.5%")
    
    # Processing timeline
    st.markdown("### ⏱️ Timeline")
    create_processing_timeline_card()

def render_processing_logs():
    """Render real-time processing logs"""
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("## 📜 Processing Logs")
        
        # Log container
        log_container = st.container()
        
        with log_container:
            # Sample logs
            logs = [
                {"time": "15:42:33", "level": "INFO", "message": "Starting Tally export for GSTIN 06ABGCS4796R1ZA"},
                {"time": "15:42:31", "level": "INFO", "message": "Pivot generation completed: 2 GST rate batches created"},
                {"time": "15:42:28", "level": "INFO", "message": "Tax computation completed for 698 transactions"},
                {"time": "15:42:25", "level": "WARN", "message": "3 SKUs not found in item master, queued for approval"},
                {"time": "15:42:22", "level": "INFO", "message": "Ledger mapping completed: 95.3% success rate"},
                {"time": "15:42:18", "level": "INFO", "message": "Item master mapping completed: 96.8% success rate"},
                {"time": "15:42:15", "level": "INFO", "message": "Data ingestion completed: 698 records normalized"},
                {"time": "15:42:12", "level": "INFO", "message": "Starting pipeline for Amazon MTR B2C Report - Sample.xlsx"}
            ]
            
            for log in logs:
                level_color = {
                    "INFO": "#1f77b4",
                    "WARN": "#ff7f0e", 
                    "ERROR": "#d62728"
                }[log["level"]]
                
                st.markdown(f"""
                <div style="font-family: monospace; font-size: 0.85rem; padding: 0.25rem; 
                           border-left: 3px solid {level_color}; margin: 0.25rem 0; 
                           background: rgba(31, 119, 180, 0.05);">
                    <span style="color: #666;">{log['time']}</span> 
                    <span style="color: {level_color}; font-weight: 600;">[{log['level']}]</span> 
                    {log['message']}
                </div>
                """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("## 📊 Processing Results")
        
        # Results summary
        if st.session_state.get('processing_complete', False):
            st.success("✅ Processing completed successfully!")
            
            # Results metrics
            results = {
                "Total Records": "698",
                "Processing Time": "4 min 23 sec",
                "Success Rate": "98.2%",
                "Files Generated": "5",
                "Exceptions": "3 (resolved)"
            }
            
            for key, value in results.items():
                st.markdown(f"**{key}:** {value}")
            
            # Generated files
            st.markdown("### 📁 Generated Files")
            
            files = [
                {"name": "amazon_mtr_06ABGCS4796R1ZA_2025-08_0pct_x2beta.xlsx", "size": "5.2 KB", "type": "X2Beta"},
                {"name": "amazon_mtr_06ABGCS4796R1ZA_2025-08_18pct_x2beta.xlsx", "size": "5.3 KB", "type": "X2Beta"},
                {"name": "mis_report_amazon_06ABGCS4796R1ZA_2025-08.csv", "size": "369 B", "type": "MIS Report"},
                {"name": "audit_log_2025-08-28.json", "size": "2.1 KB", "type": "Audit Log"}
            ]
            
            for file in files:
                col_name, col_download = st.columns([3, 1])
                
                with col_name:
                    st.markdown(f"📄 **{file['name']}**")
                    st.caption(f"{file['type']} • {file['size']}")
                
                with col_download:
                    st.button("📥", key=f"download_{file['name']}", help=f"Download {file['name']}")
        
        else:
            st.info("🔄 Processing in progress...")
            
            # Live metrics during processing
            col_a, col_b = st.columns(2)
            
            with col_a:
                st.metric("Records/sec", "25.3", delta="2.1")
                st.metric("Memory Usage", "245 MB", delta="12 MB")
            
            with col_b:
                st.metric("CPU Usage", "67%", delta="5%")
                st.metric("Queue Size", "0", delta="-3")

def start_processing(uploaded_file, channel, agent_type, gstin, month, full_pipeline=True):
    """Start the processing pipeline"""
    st.session_state['processing_started'] = True
    st.session_state['processing_complete'] = False
    
    with st.spinner("🚀 Starting processing pipeline..."):
        time.sleep(2)  # Simulate processing start
        
        st.success("✅ Processing started successfully!")
        st.info("🔄 Monitor progress in the Processing Steps section above")
        
        # Simulate completion after some time
        if st.button("🎯 Simulate Completion"):
            st.session_state['processing_complete'] = True
            st.rerun()

def validate_file(uploaded_file):
    """Validate uploaded file"""
    with st.spinner("🔍 Validating file..."):
        time.sleep(1)
        
        # Simulate validation
        validation_results = {
            "file_format": "✅ Valid Excel format",
            "required_columns": "✅ All required columns present", 
            "data_types": "✅ Data types are correct",
            "record_count": "✅ 698 records found",
            "date_range": "✅ Dates within valid range",
            "duplicates": "⚠️ 2 potential duplicates found"
        }
        
        st.markdown("### 🔍 Validation Results")
        
        for check, result in validation_results.items():
            st.markdown(f"**{check.replace('_', ' ').title()}:** {result}")

if __name__ == "__main__":
    main()
