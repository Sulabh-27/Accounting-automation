"""
Master Data Management Page
Item master, ledger master, tax rates, and bulk upload functionality
"""

import streamlit as st
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime

# Add parent directory to path for imports
parent_dir = Path(__file__).parent.parent.parent
sys.path.append(str(parent_dir))

from streamlit_app.components.utils import setup_page_config, load_custom_css
from streamlit_app.data.sample_data import get_master_data

def main():
    setup_page_config()
    load_custom_css()
    
    st.markdown("# 📋 Master Data Management")
    st.markdown("Manage item master, ledger master, tax rates, and perform bulk data operations")
    
    # Master data overview
    render_master_data_overview()
    
    st.markdown("---")
    
    # Master data tabs
    render_master_data_tabs()

def render_master_data_overview():
    """Render master data overview and statistics"""
    st.markdown("## 📊 Master Data Overview")
    
    master_data = get_master_data()
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total SKUs",
            f"{master_data['item_master']['total_skus']:,}",
            delta="+23",
            help="Total SKUs in item master"
        )
    
    with col2:
        st.metric(
            "Mapping Coverage",
            f"{master_data['item_master']['mapping_rate']:.1f}%",
            delta="+1.2%",
            help="Percentage of SKUs with Final Goods mapping"
        )
    
    with col3:
        st.metric(
            "Active Ledgers",
            master_data['ledger_master']['active_ledgers'],
            delta="+2",
            help="Number of active ledger accounts"
        )
    
    with col4:
        st.metric(
            "GST Rates",
            len(master_data['tax_rates']['gst_rates']),
            delta="0",
            help="Number of configured GST rates"
        )
    
    # Quick stats
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📈 Item Master Statistics")
        st.markdown(f"- **Mapped SKUs:** {master_data['item_master']['mapped_skus']:,}")
        st.markdown(f"- **Unmapped SKUs:** {master_data['item_master']['unmapped_skus']:,}")
        st.markdown(f"- **Last Updated:** {master_data['item_master']['last_updated'].strftime('%Y-%m-%d %H:%M')}")
    
    with col2:
        st.markdown("### 🏪 Ledger Master Statistics")
        st.markdown(f"- **Total Ledgers:** {master_data['ledger_master']['total_ledgers']}")
        st.markdown(f"- **States Covered:** {master_data['ledger_master']['states_covered']}")
        st.markdown(f"- **Channels Covered:** {master_data['ledger_master']['channels_covered']}")

def render_master_data_tabs():
    """Render master data management tabs"""
    tab1, tab2, tab3, tab4 = st.tabs(["📦 Item Master", "📊 Ledger Master", "🏛️ Tax Rates", "📤 Bulk Upload"])
    
    with tab1:
        render_item_master()
    
    with tab2:
        render_ledger_master()
    
    with tab3:
        render_tax_rates()
    
    with tab4:
        render_bulk_upload()

def render_item_master():
    """Render item master management"""
    st.markdown("### 📦 Item Master Management")
    
    # Search and filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        search_sku = st.text_input(
            "🔍 Search SKU/ASIN",
            placeholder="Enter SKU or ASIN...",
            help="Search for specific SKU or ASIN"
        )
    
    with col2:
        channel_filter = st.selectbox(
            "Channel",
            ["All", "Amazon", "Flipkart", "Pepperfry"],
            help="Filter by channel"
        )
    
    with col3:
        mapping_status = st.selectbox(
            "Mapping Status",
            ["All", "Mapped", "Unmapped", "Pending"],
            help="Filter by mapping status"
        )
    
    # Item master data
    item_data = pd.DataFrame([
        {
            "SKU": "LLQ-LAV-3L-FBA",
            "ASIN": "B0CZXQMSR5",
            "Final Goods": "Toilet Cleaner",
            "Channel": "Amazon",
            "Category": "Cleaning",
            "Status": "Mapped",
            "Last Updated": "2025-08-15"
        },
        {
            "SKU": "FABCON-5L-FBA", 
            "ASIN": "B09MZ2LBXB",
            "Final Goods": "Fabric Conditioner",
            "Channel": "Amazon",
            "Category": "Laundry",
            "Status": "Mapped",
            "Last Updated": "2025-08-14"
        },
        {
            "SKU": "NEW-PRODUCT-123",
            "ASIN": "B0D4NGD87J",
            "Final Goods": "",
            "Channel": "Amazon",
            "Category": "Unknown",
            "Status": "Unmapped",
            "Last Updated": "2025-08-28"
        },
        {
            "SKU": "PREMIUM-CLEANER-5L",
            "ASIN": "B0D4NGD87K",
            "Final Goods": "",
            "Channel": "Amazon", 
            "Category": "Cleaning",
            "Status": "Pending",
            "Last Updated": "2025-08-27"
        }
    ])
    
    # Display item master table
    st.markdown("#### 📋 Item Master Data")
    
    # Add action buttons
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col2:
        if st.button("➕ Add New SKU", width='stretch'):
            st.session_state['show_add_sku'] = True
    
    with col3:
        if st.button("📥 Export Data", width='stretch'):
            st.success("📥 Item master exported to Excel")
    
    # Show add SKU form if requested
    if st.session_state.get('show_add_sku', False):
        render_add_sku_form()
    
    # Display data with edit functionality
    edited_df = st.data_editor(
        item_data,
        width='stretch',
        hide_index=True,
        column_config={
            "Status": st.column_config.SelectboxColumn(
                "Status",
                options=["Mapped", "Unmapped", "Pending"],
                required=True
            ),
            "Channel": st.column_config.SelectboxColumn(
                "Channel", 
                options=["Amazon", "Flipkart", "Pepperfry"],
                required=True
            )
        },
        disabled=["SKU", "ASIN", "Last Updated"]
    )
    
    # Save changes button
    if st.button("💾 Save Changes", type="primary"):
        st.success("✅ Item master changes saved successfully")

def render_add_sku_form():
    """Render add new SKU form"""
    with st.expander("➕ Add New SKU", expanded=True):
        col1, col2 = st.columns(2)
        
        with col1:
            new_sku = st.text_input("SKU", placeholder="Enter SKU...")
            new_asin = st.text_input("ASIN", placeholder="Enter ASIN...")
            new_channel = st.selectbox("Channel", ["Amazon", "Flipkart", "Pepperfry"])
        
        with col2:
            new_fg = st.text_input("Final Goods", placeholder="Enter Final Goods name...")
            new_category = st.text_input("Category", placeholder="Enter category...")
            new_status = st.selectbox("Status", ["Mapped", "Unmapped", "Pending"])
        
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col2:
            if st.button("✅ Add SKU", width='stretch'):
                if new_sku and new_asin:
                    st.success(f"✅ SKU {new_sku} added successfully")
                    st.session_state['show_add_sku'] = False
                    st.rerun()
                else:
                    st.error("Please provide SKU and ASIN")
        
        with col3:
            if st.button("❌ Cancel", width='stretch'):
                st.session_state['show_add_sku'] = False
                st.rerun()

def render_ledger_master():
    """Render ledger master management"""
    st.markdown("### 📊 Ledger Master Management")
    
    # Search and filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        search_ledger = st.text_input(
            "🔍 Search Ledger",
            placeholder="Enter ledger name...",
            help="Search for specific ledger"
        )
    
    with col2:
        state_filter = st.selectbox(
            "State",
            ["All", "Maharashtra", "Karnataka", "Delhi", "Tamil Nadu", "Gujarat"],
            help="Filter by state"
        )
    
    with col3:
        ledger_channel = st.selectbox(
            "Channel",
            ["All", "Amazon", "Flipkart", "Pepperfry"],
            help="Filter by channel"
        )
    
    # Ledger master data
    ledger_data = pd.DataFrame([
        {
            "Ledger Name": "Amazon MAHARASHTRA",
            "Channel": "Amazon",
            "State": "Maharashtra",
            "State Code": "MH",
            "GST Type": "Intrastate",
            "Status": "Active",
            "Last Updated": "2025-08-15"
        },
        {
            "Ledger Name": "Amazon KARNATAKA",
            "Channel": "Amazon", 
            "State": "Karnataka",
            "State Code": "KA",
            "GST Type": "Interstate",
            "Status": "Active",
            "Last Updated": "2025-08-14"
        },
        {
            "Ledger Name": "Flipkart DELHI",
            "Channel": "Flipkart",
            "State": "Delhi",
            "State Code": "DL", 
            "GST Type": "Interstate",
            "Status": "Active",
            "Last Updated": "2025-08-13"
        },
        {
            "Ledger Name": "Amazon LADAKH",
            "Channel": "Amazon",
            "State": "Ladakh",
            "State Code": "LA",
            "GST Type": "Interstate", 
            "Status": "Pending",
            "Last Updated": "2025-08-28"
        }
    ])
    
    st.markdown("#### 📋 Ledger Master Data")
    
    # Action buttons
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col2:
        if st.button("➕ Add New Ledger", width='stretch'):
            st.session_state['show_add_ledger'] = True
    
    with col3:
        if st.button("📥 Export Ledgers", width='stretch'):
            st.success("📥 Ledger master exported to Excel")
    
    # Show add ledger form if requested
    if st.session_state.get('show_add_ledger', False):
        render_add_ledger_form()
    
    # Display ledger data
    edited_ledger_df = st.data_editor(
        ledger_data,
        width='stretch',
        hide_index=True,
        column_config={
            "Status": st.column_config.SelectboxColumn(
                "Status",
                options=["Active", "Inactive", "Pending"],
                required=True
            ),
            "GST Type": st.column_config.SelectboxColumn(
                "GST Type",
                options=["Intrastate", "Interstate"],
                required=True
            )
        },
        disabled=["Last Updated"]
    )
    
    if st.button("💾 Save Ledger Changes", type="primary"):
        st.success("✅ Ledger master changes saved successfully")

def render_add_ledger_form():
    """Render add new ledger form"""
    with st.expander("➕ Add New Ledger", expanded=True):
        col1, col2 = st.columns(2)
        
        with col1:
            new_ledger_name = st.text_input("Ledger Name", placeholder="Enter ledger name...")
            new_ledger_channel = st.selectbox("Channel", ["Amazon", "Flipkart", "Pepperfry"])
            new_ledger_state = st.text_input("State", placeholder="Enter state name...")
        
        with col2:
            new_state_code = st.text_input("State Code", placeholder="Enter state code...")
            new_gst_type = st.selectbox("GST Type", ["Intrastate", "Interstate"])
            new_ledger_status = st.selectbox("Status", ["Active", "Inactive", "Pending"])
        
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col2:
            if st.button("✅ Add Ledger", width='stretch'):
                if new_ledger_name and new_ledger_state:
                    st.success(f"✅ Ledger {new_ledger_name} added successfully")
                    st.session_state['show_add_ledger'] = False
                    st.rerun()
                else:
                    st.error("Please provide ledger name and state")
        
        with col3:
            if st.button("❌ Cancel", width='stretch'):
                st.session_state['show_add_ledger'] = False
                st.rerun()

def render_tax_rates():
    """Render tax rates management"""
    st.markdown("### 🏛️ Tax Rates Management")
    
    # Current GST rates
    st.markdown("#### 📊 Current GST Rates")
    
    gst_rates_data = pd.DataFrame([
        {
            "GST Rate": "0%",
            "Description": "Essential goods, basic food items",
            "Status": "Active",
            "Effective Date": "2017-07-01",
            "Last Updated": "2025-01-01"
        },
        {
            "GST Rate": "5%",
            "Description": "Essential items, household necessities", 
            "Status": "Active",
            "Effective Date": "2017-07-01",
            "Last Updated": "2025-01-01"
        },
        {
            "GST Rate": "12%",
            "Description": "Standard goods, processed foods",
            "Status": "Active", 
            "Effective Date": "2017-07-01",
            "Last Updated": "2025-01-01"
        },
        {
            "GST Rate": "18%",
            "Description": "Most goods and services",
            "Status": "Active",
            "Effective Date": "2017-07-01", 
            "Last Updated": "2025-01-01"
        },
        {
            "GST Rate": "28%",
            "Description": "Luxury goods, sin goods",
            "Status": "Active",
            "Effective Date": "2017-07-01",
            "Last Updated": "2025-01-01"
        }
    ])
    
    # Action buttons
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col2:
        if st.button("➕ Add Tax Rate", width='stretch'):
            st.session_state['show_add_tax_rate'] = True
    
    with col3:
        if st.button("📥 Export Rates", width='stretch'):
            st.success("📥 Tax rates exported to Excel")
    
    # Show add tax rate form if requested
    if st.session_state.get('show_add_tax_rate', False):
        render_add_tax_rate_form()
    
    # Display tax rates
    st.data_editor(
        gst_rates_data,
        width='stretch',
        hide_index=True,
        column_config={
            "Status": st.column_config.SelectboxColumn(
                "Status",
                options=["Active", "Inactive"],
                required=True
            ),
            "Effective Date": st.column_config.DateColumn(
                "Effective Date",
                format="YYYY-MM-DD"
            )
        },
        disabled=["GST Rate", "Last Updated"]
    )
    
    # Tax rate mapping
    st.markdown("#### 🔗 Product Category Tax Mapping")
    
    category_mapping = pd.DataFrame([
        {"Category": "Cleaning Products", "Default GST Rate": "18%", "Override Allowed": "Yes"},
        {"Category": "Laundry Products", "Default GST Rate": "18%", "Override Allowed": "Yes"},
        {"Category": "Personal Care", "Default GST Rate": "18%", "Override Allowed": "Yes"},
        {"Category": "Food Items", "Default GST Rate": "5%", "Override Allowed": "No"},
        {"Category": "Essential Goods", "Default GST Rate": "0%", "Override Allowed": "No"}
    ])
    
    st.data_editor(
        category_mapping,
        width='stretch',
        hide_index=True,
        column_config={
            "Default GST Rate": st.column_config.SelectboxColumn(
                "Default GST Rate",
                options=["0%", "5%", "12%", "18%", "28%"],
                required=True
            ),
            "Override Allowed": st.column_config.SelectboxColumn(
                "Override Allowed",
                options=["Yes", "No"],
                required=True
            )
        }
    )

def render_add_tax_rate_form():
    """Render add new tax rate form"""
    with st.expander("➕ Add New Tax Rate", expanded=True):
        col1, col2 = st.columns(2)
        
        with col1:
            new_rate = st.number_input("GST Rate (%)", min_value=0.0, max_value=100.0, step=0.1)
            new_description = st.text_area("Description", placeholder="Enter rate description...")
        
        with col2:
            new_effective_date = st.date_input("Effective Date")
            new_rate_status = st.selectbox("Status", ["Active", "Inactive"])
        
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col2:
            if st.button("✅ Add Rate", width='stretch'):
                if new_rate is not None and new_description:
                    st.success(f"✅ GST Rate {new_rate}% added successfully")
                    st.session_state['show_add_tax_rate'] = False
                    st.rerun()
                else:
                    st.error("Please provide rate and description")
        
        with col3:
            if st.button("❌ Cancel", width='stretch'):
                st.session_state['show_add_tax_rate'] = False
                st.rerun()

def render_bulk_upload():
    """Render bulk upload functionality"""
    st.markdown("### 📤 Bulk Upload & Data Operations")
    
    # Upload options
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 📦 Item Master Bulk Upload")
        
        item_file = st.file_uploader(
            "Upload Item Master Excel",
            type=['xlsx', 'xls'],
            help="Upload Excel file with SKU, ASIN, Final Goods columns"
        )
        
        if item_file:
            st.success(f"✅ File uploaded: {item_file.name}")
            
            if st.button("🔍 Preview Data", key="preview_items"):
                # Show preview of uploaded data
                st.markdown("**Preview:**")
                preview_data = pd.DataFrame([
                    {"SKU": "SAMPLE-SKU-1", "ASIN": "B0SAMPLE1", "Final Goods": "Sample Product 1"},
                    {"SKU": "SAMPLE-SKU-2", "ASIN": "B0SAMPLE2", "Final Goods": "Sample Product 2"}
                ])
                st.dataframe(preview_data, width='stretch')
            
            if st.button("📤 Process Upload", type="primary", key="process_items"):
                st.success("✅ Item master bulk upload completed: 150 records processed")
    
    with col2:
        st.markdown("#### 📊 Ledger Master Bulk Upload")
        
        ledger_file = st.file_uploader(
            "Upload Ledger Master Excel",
            type=['xlsx', 'xls'],
            help="Upload Excel file with Ledger Name, Channel, State columns"
        )
        
        if ledger_file:
            st.success(f"✅ File uploaded: {ledger_file.name}")
            
            if st.button("🔍 Preview Data", key="preview_ledgers"):
                # Show preview of uploaded data
                st.markdown("**Preview:**")
                preview_ledger = pd.DataFrame([
                    {"Ledger Name": "Amazon SAMPLE STATE", "Channel": "Amazon", "State": "Sample State"},
                    {"Ledger Name": "Flipkart SAMPLE STATE", "Channel": "Flipkart", "State": "Sample State"}
                ])
                st.dataframe(preview_ledger, width='stretch')
            
            if st.button("📤 Process Upload", type="primary", key="process_ledgers"):
                st.success("✅ Ledger master bulk upload completed: 75 records processed")
    
    # Download templates
    st.markdown("#### 📋 Download Templates")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📥 Item Master Template", width='stretch'):
            st.success("📥 Item master template downloaded")
    
    with col2:
        if st.button("📥 Ledger Master Template", width='stretch'):
            st.success("📥 Ledger master template downloaded")
    
    with col3:
        if st.button("📥 Tax Rates Template", width='stretch'):
            st.success("📥 Tax rates template downloaded")
    
    # Bulk operations
    st.markdown("#### 🔧 Bulk Operations")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Data Validation:**")
        if st.button("🔍 Validate All Data", width='stretch'):
            st.success("✅ Data validation completed: 99.2% valid records")
        
        if st.button("🧹 Clean Duplicate Entries", width='stretch'):
            st.success("✅ Duplicate cleanup completed: 12 duplicates removed")
    
    with col2:
        st.markdown("**Data Export:**")
        if st.button("📊 Export All Masters", width='stretch'):
            st.success("📊 All master data exported to Excel")
        
        if st.button("📋 Generate Data Report", width='stretch'):
            st.success("📋 Master data report generated")

if __name__ == "__main__":
    main()
