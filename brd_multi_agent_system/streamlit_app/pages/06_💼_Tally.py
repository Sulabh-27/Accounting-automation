"""
Tally Integration Page
X2Beta file management, template configuration, and import status tracking
"""

import streamlit as st
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta

# Add parent directory to path for imports
parent_dir = Path(__file__).parent.parent.parent
sys.path.append(str(parent_dir))

from streamlit_app.components.utils import setup_page_config, load_custom_css, format_currency
from streamlit_app.data.sample_data import get_tally_integration_data

def main():
    setup_page_config()
    load_custom_css()
    
    st.markdown("# 💼 Tally Integration")
    st.markdown("Manage X2Beta files, templates, and monitor Tally import status")
    
    # Tally integration overview
    render_tally_overview()
    
    st.markdown("---")
    
    # Tally integration tabs
    render_tally_tabs()

def render_tally_overview():
    """Render Tally integration overview and key metrics"""
    st.markdown("## 📊 Tally Integration Overview")
    
    tally_data = get_tally_integration_data()
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "X2Beta Files",
            len(tally_data['x2beta_files']),
            delta="+2",
            help="Total X2Beta files generated"
        )
    
    with col2:
        st.metric(
            "Success Rate",
            f"{tally_data['import_status']['success_rate']:.1f}%",
            delta="+1.2%",
            help="Tally import success rate"
        )
    
    with col3:
        st.metric(
            "Active Templates",
            tally_data['templates']['active_templates'],
            delta="0",
            help="Number of active X2Beta templates"
        )
    
    with col4:
        total_amount = sum(file['amount'] for file in tally_data['x2beta_files'])
        st.metric(
            "Total Value",
            format_currency(total_amount),
            delta=format_currency(45000),
            help="Total value in generated files"
        )
    
    # Quick actions
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("🚀 Generate X2Beta", width='stretch'):
            st.success("✅ X2Beta generation initiated")
    
    with col2:
        if st.button("📥 Download All", width='stretch'):
            st.success("📥 All X2Beta files downloaded")
    
    with col3:
        if st.button("🔄 Sync Status", width='stretch'):
            st.success("🔄 Import status synchronized")
    
    with col4:
        if st.button("📊 Generate Report", width='stretch'):
            st.success("📊 Tally integration report generated")

def render_tally_tabs():
    """Render Tally integration management tabs"""
    tab1, tab2, tab3, tab4 = st.tabs(["📁 X2Beta Files", "📋 Templates", "📊 Import Status", "⚙️ Configuration"])
    
    with tab1:
        render_x2beta_files()
    
    with tab2:
        render_templates()
    
    with tab3:
        render_import_status()
    
    with tab4:
        render_configuration()

def render_x2beta_files():
    """Render X2Beta files management"""
    st.markdown("### 📁 X2Beta Files Management")
    
    tally_data = get_tally_integration_data()
    
    # Filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        gstin_filter = st.selectbox(
            "Filter by GSTIN",
            ["All"] + tally_data['templates']['gstins'],
            help="Filter files by GSTIN"
        )
    
    with col2:
        gst_rate_filter = st.selectbox(
            "Filter by GST Rate",
            ["All", "0%", "5%", "12%", "18%", "28%"],
            help="Filter files by GST rate"
        )
    
    with col3:
        date_filter = st.date_input(
            "Created After",
            value=datetime.now() - timedelta(days=7),
            help="Show files created after this date"
        )
    
    # X2Beta files list
    st.markdown("#### 📋 Generated X2Beta Files")
    
    for file in tally_data['x2beta_files']:
        with st.container():
            st.markdown(f"""
            <div style="border: 1px solid #e1e5e9; border-radius: 10px; padding: 1rem; 
                        margin: 0.5rem 0; background: rgba(255, 255, 255, 0.8);">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <h4 style="margin: 0; color: #1f77b4;">📄 {file['filename']}</h4>
                        <p style="margin: 0.25rem 0; color: #666; font-size: 0.9rem;">
                            GSTIN: {file['gstin']} | GST Rate: {file['gst_rate']} | 
                            Records: {file['records']} | Amount: {format_currency(file['amount'])}
                        </p>
                        <p style="margin: 0; color: #888; font-size: 0.8rem;">
                            Created: {file['created'].strftime('%Y-%m-%d %H:%M:%S')}
                        </p>
                    </div>
                    <div style="text-align: right;">
                        <span style="background: #2ca02c; color: white; padding: 0.25rem 0.75rem; 
                                     border-radius: 20px; font-size: 0.8rem; font-weight: 600;">
                            {file['status'].upper()}
                        </span>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # File actions
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                if st.button("📥 Download", key=f"download_{file['filename']}", width='stretch'):
                    st.success(f"📥 {file['filename']} downloaded")
            
            with col2:
                if st.button("👁️ Preview", key=f"preview_{file['filename']}", width='stretch'):
                    render_file_preview(file)
            
            with col3:
                if st.button("📊 Validate", key=f"validate_{file['filename']}", width='stretch'):
                    st.success(f"✅ {file['filename']} validation passed")
            
            with col4:
                if st.button("🔄 Re-generate", key=f"regen_{file['filename']}", width='stretch'):
                    st.success(f"🔄 {file['filename']} re-generation initiated")

def render_file_preview(file):
    """Render file preview in expander"""
    with st.expander(f"👁️ Preview: {file['filename']}", expanded=True):
        # Sample X2Beta data preview
        preview_data = pd.DataFrame([
            {
                "Date": "2025-08-15",
                "Voucher No": "AMZ-HR-001",
                "Party Ledger": "Amazon HARYANA",
                "Item Name": "Toilet Cleaner",
                "Quantity": 1,
                "Rate": 449.00,
                "Amount": 449.00,
                "CGST": 40.41,
                "SGST": 40.41,
                "Total": 529.82
            },
            {
                "Date": "2025-08-15", 
                "Voucher No": "AMZ-HR-002",
                "Party Ledger": "Amazon HARYANA",
                "Item Name": "Fabric Conditioner",
                "Quantity": 1,
                "Rate": 1059.00,
                "Amount": 1059.00,
                "CGST": 95.31,
                "SGST": 95.31,
                "Total": 1249.62
            }
        ])
        
        st.dataframe(preview_data, width='stretch', hide_index=True)
        
        # File statistics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Records", file['records'])
        
        with col2:
            st.metric("Total Amount", format_currency(file['amount']))
        
        with col3:
            st.metric("File Size", "5.2 KB")

def render_templates():
    """Render X2Beta templates management"""
    st.markdown("### 📋 X2Beta Templates Management")
    
    tally_data = get_tally_integration_data()
    
    # Template overview
    st.markdown("#### 📊 Template Overview")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Templates", tally_data['templates']['total_templates'])
    
    with col2:
        st.metric("Active Templates", tally_data['templates']['active_templates'])
    
    with col3:
        st.metric("GSTINs Covered", len(tally_data['templates']['gstins']))
    
    # Templates list
    st.markdown("#### 📋 Available Templates")
    
    template_data = []
    for i, gstin in enumerate(tally_data['templates']['gstins']):
        company_names = {
            "06ABGCS4796R1ZA": "Haryana Operations Pvt Ltd",
            "07ABGCS4796R1Z8": "Delhi Trading Co Ltd", 
            "09ABGCS4796R1Z4": "UP Manufacturing Ltd",
            "24ABGCS4796R1ZC": "Gujarat Exports Ltd",
            "29ABGCS4796R1Z2": "Karnataka Services Ltd"
        }
        
        template_data.append({
            "GSTIN": gstin,
            "Company Name": company_names.get(gstin, "Unknown Company"),
            "Template File": f"X2Beta Sales Template - {gstin}.xlsx",
            "Status": "Active",
            "Last Updated": "2025-08-15",
            "Version": "v2.1"
        })
    
    # Display templates
    df_templates = pd.DataFrame(template_data)
    
    # Template actions
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col2:
        if st.button("➕ Add Template", width='stretch'):
            st.session_state['show_add_template'] = True
    
    with col3:
        if st.button("📥 Export All", width='stretch'):
            st.success("📥 All templates exported")
    
    # Show add template form if requested
    if st.session_state.get('show_add_template', False):
        render_add_template_form()
    
    # Display template data with actions
    for i, template in df_templates.iterrows():
        with st.container():
            st.markdown(f"""
            <div style="border: 1px solid #1f77b4; border-radius: 10px; padding: 1rem; 
                        margin: 0.5rem 0; background: rgba(31, 119, 180, 0.05);">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <h4 style="margin: 0; color: #1f77b4;">🏢 {template['Company Name']}</h4>
                        <p style="margin: 0.25rem 0; color: #666; font-size: 0.9rem;">
                            GSTIN: {template['GSTIN']} | Template: {template['Template File']}
                        </p>
                        <p style="margin: 0; color: #888; font-size: 0.8rem;">
                            Version: {template['Version']} | Updated: {template['Last Updated']}
                        </p>
                    </div>
                    <div>
                        <span style="background: #2ca02c; color: white; padding: 0.25rem 0.75rem; 
                                     border-radius: 20px; font-size: 0.8rem; font-weight: 600;">
                            {template['Status'].upper()}
                        </span>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Template actions
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                if st.button("📥 Download", key=f"dl_template_{i}", width='stretch'):
                    st.success(f"📥 Template for {template['GSTIN']} downloaded")
            
            with col2:
                if st.button("✏️ Edit", key=f"edit_template_{i}", width='stretch'):
                    st.info(f"✏️ Editing template for {template['GSTIN']}")
            
            with col3:
                if st.button("🔍 Validate", key=f"val_template_{i}", width='stretch'):
                    st.success(f"✅ Template for {template['GSTIN']} is valid")
            
            with col4:
                if st.button("🗑️ Delete", key=f"del_template_{i}", width='stretch'):
                    st.error(f"🗑️ Template for {template['GSTIN']} marked for deletion")

def render_add_template_form():
    """Render add new template form"""
    with st.expander("➕ Add New X2Beta Template", expanded=True):
        col1, col2 = st.columns(2)
        
        with col1:
            new_gstin = st.text_input("GSTIN", placeholder="Enter GSTIN...")
            new_company = st.text_input("Company Name", placeholder="Enter company name...")
            template_file = st.file_uploader("Upload Template", type=['xlsx', 'xls'])
        
        with col2:
            new_version = st.text_input("Version", placeholder="e.g., v2.1")
            new_status = st.selectbox("Status", ["Active", "Inactive"])
            new_description = st.text_area("Description", placeholder="Template description...")
        
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col2:
            if st.button("✅ Add Template", width='stretch'):
                if new_gstin and new_company:
                    st.success(f"✅ Template for {new_gstin} added successfully")
                    st.session_state['show_add_template'] = False
                    st.rerun()
                else:
                    st.error("Please provide GSTIN and company name")
        
        with col3:
            if st.button("❌ Cancel", width='stretch'):
                st.session_state['show_add_template'] = False
                st.rerun()

def render_import_status():
    """Render Tally import status tracking"""
    st.markdown("### 📊 Tally Import Status")
    
    tally_data = get_tally_integration_data()
    
    # Import statistics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Successful Imports",
            tally_data['import_status']['successful_imports'],
            delta="+5"
        )
    
    with col2:
        st.metric(
            "Failed Imports",
            tally_data['import_status']['failed_imports'],
            delta="-1",
            delta_color="inverse"
        )
    
    with col3:
        st.metric(
            "Pending Imports",
            tally_data['import_status']['pending_imports'],
            delta="0"
        )
    
    with col4:
        st.metric(
            "Success Rate",
            f"{tally_data['import_status']['success_rate']:.1f}%",
            delta="+1.2%"
        )
    
    # Import history
    st.markdown("#### 📋 Import History")
    
    import_history = pd.DataFrame([
        {
            "File Name": "amazon_mtr_06ABGCS4796R1ZA_2025-08_18pct_x2beta.xlsx",
            "Import Date": "2025-08-28 14:30:00",
            "Status": "Success",
            "Records": 573,
            "Amount": "₹3,65,000.00",
            "Tally Company": "Haryana Operations Pvt Ltd",
            "Import Time": "2.3 sec"
        },
        {
            "File Name": "amazon_mtr_06ABGCS4796R1ZA_2025-08_0pct_x2beta.xlsx",
            "Import Date": "2025-08-28 14:28:00", 
            "Status": "Success",
            "Records": 125,
            "Amount": "₹85,000.00",
            "Tally Company": "Haryana Operations Pvt Ltd",
            "Import Time": "1.1 sec"
        },
        {
            "File Name": "flipkart_07ABGCS4796R1Z8_2025-08_18pct_x2beta.xlsx",
            "Import Date": "2025-08-27 16:45:00",
            "Status": "Failed",
            "Records": 89,
            "Amount": "₹56,000.00", 
            "Tally Company": "Delhi Trading Co Ltd",
            "Import Time": "0.8 sec",
            "Error": "Ledger not found"
        },
        {
            "File Name": "pepperfry_09ABGCS4796R1Z4_2025-08_12pct_x2beta.xlsx",
            "Import Date": "2025-08-27 11:20:00",
            "Status": "Pending",
            "Records": 36,
            "Amount": "₹23,000.00",
            "Tally Company": "UP Manufacturing Ltd",
            "Import Time": "-"
        }
    ])
    
    # Status filter
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col2:
        status_filter = st.selectbox(
            "Filter by Status",
            ["All", "Success", "Failed", "Pending"],
            help="Filter imports by status"
        )
    
    with col3:
        if st.button("🔄 Refresh Status", width='stretch'):
            st.success("🔄 Import status refreshed")
    
    # Display import history with status colors
    for i, import_record in import_history.iterrows():
        status_colors = {
            "Success": "#2ca02c",
            "Failed": "#d62728", 
            "Pending": "#ff7f0e"
        }
        
        status_color = status_colors[import_record['Status']]
        
        with st.container():
            st.markdown(f"""
            <div style="border: 1px solid {status_color}; border-radius: 10px; padding: 1rem; 
                        margin: 0.5rem 0; background: rgba(255, 255, 255, 0.8);">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <h4 style="margin: 0; color: {status_color};">📄 {import_record['File Name']}</h4>
                        <p style="margin: 0.25rem 0; color: #666; font-size: 0.9rem;">
                            Company: {import_record['Tally Company']} | Records: {import_record['Records']} | 
                            Amount: {import_record['Amount']}
                        </p>
                        <p style="margin: 0; color: #888; font-size: 0.8rem;">
                            Import Date: {import_record['Import Date']} | Time: {import_record['Import Time']}
                        </p>
                        {f'<p style="margin: 0; color: #d62728; font-size: 0.8rem;">Error: {import_record.get("Error", "")}</p>' if import_record['Status'] == 'Failed' else ''}
                    </div>
                    <div>
                        <span style="background: {status_color}; color: white; padding: 0.25rem 0.75rem; 
                                     border-radius: 20px; font-size: 0.8rem; font-weight: 600;">
                            {import_record['Status'].upper()}
                        </span>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Import actions
            if import_record['Status'] == 'Failed':
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if st.button("🔄 Retry Import", key=f"retry_{i}", width='stretch'):
                        st.success(f"🔄 Retry initiated for {import_record['File Name']}")
                
                with col2:
                    if st.button("🔍 View Error", key=f"error_{i}", width='stretch'):
                        st.error(f"Error: {import_record.get('Error', 'Unknown error')}")
                
                with col3:
                    if st.button("🛠️ Fix & Retry", key=f"fix_{i}", width='stretch'):
                        st.info(f"🛠️ Opening fix dialog for {import_record['File Name']}")

def render_configuration():
    """Render Tally integration configuration"""
    st.markdown("### ⚙️ Tally Integration Configuration")
    
    # Connection settings
    st.markdown("#### 🔗 Connection Settings")
    
    col1, col2 = st.columns(2)
    
    with col1:
        tally_server = st.text_input(
            "Tally Server",
            value="localhost",
            help="Tally server hostname or IP address"
        )
        
        tally_port = st.number_input(
            "Tally Port",
            value=9000,
            min_value=1,
            max_value=65535,
            help="Tally server port number"
        )
        
        connection_timeout = st.number_input(
            "Connection Timeout (seconds)",
            value=30,
            min_value=5,
            max_value=300,
            help="Connection timeout in seconds"
        )
    
    with col2:
        auto_import = st.checkbox(
            "Auto Import",
            value=True,
            help="Automatically import X2Beta files to Tally"
        )
        
        backup_before_import = st.checkbox(
            "Backup Before Import",
            value=True,
            help="Create backup before importing data"
        )
        
        validate_before_import = st.checkbox(
            "Validate Before Import",
            value=True,
            help="Validate X2Beta files before import"
        )
    
    # Test connection
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col2:
        if st.button("🔍 Test Connection", width='stretch'):
            st.success("✅ Connection to Tally server successful")
    
    with col3:
        if st.button("💾 Save Settings", width='stretch'):
            st.success("💾 Configuration saved successfully")
    
    # Import settings
    st.markdown("#### 📥 Import Settings")
    
    col1, col2 = st.columns(2)
    
    with col1:
        import_mode = st.selectbox(
            "Import Mode",
            ["Create", "Alter", "Create or Alter"],
            help="How to handle existing vouchers"
        )
        
        voucher_class = st.selectbox(
            "Voucher Class",
            ["Sales", "Purchase", "Receipt", "Payment"],
            help="Default voucher class for imports"
        )
    
    with col2:
        duplicate_handling = st.selectbox(
            "Duplicate Handling",
            ["Skip", "Overwrite", "Create New"],
            help="How to handle duplicate vouchers"
        )
        
        error_handling = st.selectbox(
            "Error Handling",
            ["Stop on Error", "Skip and Continue", "Log and Continue"],
            help="How to handle import errors"
        )
    
    # Company mapping
    st.markdown("#### 🏢 Company Mapping")
    
    company_mapping = pd.DataFrame([
        {"GSTIN": "06ABGCS4796R1ZA", "Tally Company": "Haryana Operations Pvt Ltd", "Status": "Active"},
        {"GSTIN": "07ABGCS4796R1Z8", "Tally Company": "Delhi Trading Co Ltd", "Status": "Active"},
        {"GSTIN": "09ABGCS4796R1Z4", "Tally Company": "UP Manufacturing Ltd", "Status": "Active"},
        {"GSTIN": "24ABGCS4796R1ZC", "Tally Company": "Gujarat Exports Ltd", "Status": "Active"},
        {"GSTIN": "29ABGCS4796R1Z2", "Tally Company": "Karnataka Services Ltd", "Status": "Active"}
    ])
    
    edited_mapping = st.data_editor(
        company_mapping,
        width='stretch',
        hide_index=True,
        column_config={
            "Status": st.column_config.SelectboxColumn(
                "Status",
                options=["Active", "Inactive"],
                required=True
            )
        },
        disabled=["GSTIN"]
    )
    
    # Advanced settings
    st.markdown("#### 🔧 Advanced Settings")
    
    col1, col2 = st.columns(2)
    
    with col1:
        batch_size = st.number_input(
            "Batch Size",
            value=100,
            min_value=1,
            max_value=1000,
            help="Number of vouchers to import in each batch"
        )
        
        retry_attempts = st.number_input(
            "Retry Attempts",
            value=3,
            min_value=0,
            max_value=10,
            help="Number of retry attempts for failed imports"
        )
    
    with col2:
        log_level = st.selectbox(
            "Log Level",
            ["DEBUG", "INFO", "WARNING", "ERROR"],
            index=1,
            help="Logging level for import operations"
        )
        
        cleanup_after_import = st.checkbox(
            "Cleanup After Import",
            value=False,
            help="Delete X2Beta files after successful import"
        )
    
    # Save configuration
    if st.button("💾 Save All Configuration", type="primary"):
        st.success("✅ All Tally integration settings saved successfully")

if __name__ == "__main__":
    main()
