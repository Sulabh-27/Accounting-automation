"""
🎉 Multi-Agent Accounting System - Streamlit Dashboard
Enterprise-grade UI for 8-part e-commerce accounting automation
"""

import streamlit as st
import sys
import os
from pathlib import Path

# Add parent directory to path for imports
parent_dir = Path(__file__).parent.parent
sys.path.append(str(parent_dir))

from components.utils import setup_page_config, load_custom_css
from components.cards import create_metric_card
from components.charts import create_status_chart
from data.sample_data import get_dashboard_metrics, get_recent_activity

def main():
    """Main Streamlit application"""
    
    # Page configuration
    setup_page_config()
    
    # Load custom CSS
    load_custom_css()
    
    # Main header
    st.markdown("""
    <div class="main-header">
        <h1>🎉 Multi-Agent Accounting System</h1>
        <p>Enterprise E-commerce Accounting Automation Dashboard</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Sidebar navigation
    with st.sidebar:
        st.markdown("### 🧭 Navigation")
        st.markdown("---")
        
        # System status indicator
        st.markdown("""
        <div class="status-indicator">
            <span class="status-dot status-online"></span>
            <span>System Online</span>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Quick actions
        st.markdown("### ⚡ Quick Actions")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📤 Upload File", width='stretch'):
                st.switch_page("pages/01_📊_Pipeline.py")
        
        with col2:
            if st.button("📈 View Reports", width='stretch'):
                st.switch_page("pages/02_📈_Analytics.py")
        
        if st.button("⚠️ Check Exceptions", width='stretch'):
            st.switch_page("pages/04_⚠️_Exceptions.py")
        
        if st.button("💼 Tally Files", width='stretch'):
            st.switch_page("pages/06_💼_Tally.py")
    
    # Main dashboard content
    render_home_dashboard()

def render_home_dashboard():
    """Render the main dashboard content"""
    
    # Key metrics section
    st.markdown("## 📊 Key Metrics")
    
    metrics = get_dashboard_metrics()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        create_metric_card(
            "Total Transactions",
            metrics["total_transactions"],
            "+12% from last month",
            "📈"
        )
    
    with col2:
        create_metric_card(
            "Value Processed",
            f"₹{metrics['value_processed']:,.2f}",
            "+8% from last month",
            "💰"
        )
    
    with col3:
        create_metric_card(
            "Success Rate",
            f"{metrics['success_rate']:.1f}%",
            "+2.1% from last month",
            "✅"
        )
    
    with col4:
        create_metric_card(
            "Active Exceptions",
            metrics["exception_count"],
            "-5 from yesterday",
            "⚠️"
        )
    
    st.markdown("---")
    
    # Processing status and recent activity
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### 🔄 Processing Status")
        
        # Pipeline status
        pipeline_steps = [
            ("Data Ingestion", "completed", "✅"),
            ("Mapping & Enrichment", "completed", "✅"),
            ("Tax Computation", "completed", "✅"),
            ("Pivot & Batch", "completed", "✅"),
            ("Tally Export", "in_progress", "🔄"),
            ("Expense Processing", "pending", "⏳"),
            ("Exception Handling", "pending", "⏳"),
            ("MIS & Audit", "pending", "⏳")
        ]
        
        for step, status, icon in pipeline_steps:
            status_color = {
                "completed": "🟢",
                "in_progress": "🟡", 
                "pending": "⚪"
            }[status]
            
            st.markdown(f"{status_color} **{step}** {icon}")
        
        # Processing progress
        st.markdown("#### Current Processing")
        progress_bar = st.progress(0.625)  # 5/8 steps completed
        st.caption("5 of 8 steps completed (62.5%)")
    
    with col2:
        st.markdown("### 📋 Recent Activity")
        
        activities = get_recent_activity()
        
        for activity in activities:
            st.markdown(f"""
            <div class="activity-item">
                <div class="activity-icon">{activity['icon']}</div>
                <div class="activity-content">
                    <div class="activity-title">{activity['title']}</div>
                    <div class="activity-time">{activity['time']}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # System overview
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🏗️ System Architecture")
        
        st.markdown("""
        **8-Part Pipeline:**
        1. 📥 Data Ingestion & Normalization
        2. 🔗 Item & Ledger Master Mapping  
        3. 🧮 Tax Computation & Invoice Generation
        4. 📊 Pivot Summaries & Batch Processing
        5. 💼 Tally Export & X2Beta Integration
        6. 💸 Expense Processing & Seller Invoices
        7. ⚠️ Exception Handling & Approval Workflows
        8. 📈 MIS Reports & Audit Trail
        """)
    
    with col2:
        st.markdown("### 🎯 Supported Platforms")
        
        platforms = [
            ("Amazon MTR", "✅", "Monthly Transaction Reports"),
            ("Amazon STR", "✅", "Settlement Reports"),
            ("Flipkart", "✅", "Transaction Data"),
            ("Pepperfry", "✅", "Marketplace Data"),
            ("Custom Format", "✅", "Universal Agent")
        ]
        
        for platform, status, description in platforms:
            st.markdown(f"{status} **{platform}** - {description}")
    
    # Quick stats
    st.markdown("---")
    st.markdown("### 📊 Quick Statistics")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Files Processed", "1,247", "↗️ 23")
    
    with col2:
        st.metric("GST Computed", "₹45,678", "↗️ ₹2,341")
    
    with col3:
        st.metric("X2Beta Files", "89", "↗️ 7")
    
    with col4:
        st.metric("Audit Events", "12,456", "↗️ 234")
    
    with col5:
        st.metric("Uptime", "99.8%", "↗️ 0.1%")

if __name__ == "__main__":
    main()
