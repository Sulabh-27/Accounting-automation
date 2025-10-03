"""
Exception Management Page
Exception dashboard, approval queue, and resolution workflows
"""

import streamlit as st
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta

# Add parent directory to path for imports
parent_dir = Path(__file__).parent.parent.parent
sys.path.append(str(parent_dir))

from streamlit_app.components.utils import setup_page_config, load_custom_css
from streamlit_app.components.charts import create_exception_trend_chart
from streamlit_app.data.sample_data import get_exception_data, get_approval_queue

def main():
    setup_page_config()
    load_custom_css()
    
    st.markdown("# ⚠️ Exception Management")
    st.markdown("Monitor, manage, and resolve exceptions with automated workflows and human oversight")
    
    # Exception overview
    render_exception_overview()
    
    st.markdown("---")
    
    # Exception dashboard
    render_exception_dashboard()
    
    st.markdown("---")
    
    # Approval queue
    render_approval_queue()
    
    st.markdown("---")
    
    # Exception resolution
    render_exception_resolution()

def render_exception_overview():
    """Render exception overview and key metrics"""
    st.markdown("## 📊 Exception Overview")
    
    exception_data = get_exception_data()
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    total_exceptions = sum(cat["count"] for cat in exception_data["categories"].values())
    critical_count = sum(cat["count"] for cat in exception_data["categories"].values() if cat["severity"] == "critical")
    pending_count = len([exc for exc in exception_data["recent_exceptions"] if exc["status"] == "pending"])
    
    with col1:
        st.metric(
            "Total Exceptions",
            total_exceptions,
            delta="-5",
            delta_color="inverse",
            help="Total exceptions across all categories"
        )
    
    with col2:
        st.metric(
            "Critical Issues",
            critical_count,
            delta="0",
            help="Critical exceptions requiring immediate attention"
        )
    
    with col3:
        st.metric(
            "Pending Resolution",
            pending_count,
            delta="-2",
            delta_color="inverse",
            help="Exceptions awaiting resolution"
        )
    
    with col4:
        resolution_rate = ((total_exceptions - pending_count) / total_exceptions * 100) if total_exceptions > 0 else 100
        st.metric(
            "Resolution Rate",
            f"{resolution_rate:.1f}%",
            delta="+2.3%",
            help="Percentage of exceptions resolved"
        )
    
    # Exception trend chart
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### 📈 Exception Trend (Last 30 Days)")
        fig_trend = create_exception_trend_chart()
        st.plotly_chart(fig_trend, width='stretch')
    
    with col2:
        st.markdown("### 🎯 Quick Actions")
        
        if st.button("🔍 View All Exceptions", width='stretch'):
            st.info("Showing all exceptions...")
        
        if st.button("⚡ Auto-Resolve", width='stretch'):
            st.success("✅ 3 exceptions auto-resolved")
        
        if st.button("📧 Send Alerts", width='stretch'):
            st.success("📧 Alert notifications sent")
        
        if st.button("📊 Generate Report", width='stretch'):
            st.success("📊 Exception report generated")

def render_exception_dashboard():
    """Render exception categories and details"""
    st.markdown("## 🗂️ Exception Categories")
    
    exception_data = get_exception_data()
    
    # Exception categories grid
    col1, col2 = st.columns(2)
    
    categories = list(exception_data["categories"].items())
    mid_point = len(categories) // 2
    
    with col1:
        for category, data in categories[:mid_point]:
            render_exception_category_card(category, data)
    
    with col2:
        for category, data in categories[mid_point:]:
            render_exception_category_card(category, data)
    
    # Recent exceptions
    st.markdown("### 🕒 Recent Exceptions")
    
    for exc in exception_data["recent_exceptions"]:
        severity_color = {
            "critical": "#d62728",
            "error": "#ff7f0e",
            "warning": "#ff7f0e",
            "info": "#1f77b4"
        }[exc["severity"]]
        
        status_icon = "🔄" if exc["status"] == "pending" else "✅"
        
        with st.expander(f"{status_icon} {exc['category']}-{exc['id']} - {exc['message'][:50]}...", expanded=False):
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown(f"**Exception ID:** {exc['id']}")
                st.markdown(f"**Category:** {exc['category']}")
                st.markdown(f"**Message:** {exc['message']}")
                st.markdown(f"**Timestamp:** {exc['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
            
            with col2:
                st.markdown(f"""
                <div style="background: {severity_color}; color: white; padding: 0.5rem; 
                           border-radius: 8px; text-align: center; margin-bottom: 1rem;">
                    <strong>{exc['severity'].upper()}</strong>
                </div>
                """, unsafe_allow_html=True)
                
                if exc["status"] == "pending":
                    if st.button(f"🔧 Resolve {exc['id']}", key=f"resolve_{exc['id']}"):
                        st.success(f"✅ Exception {exc['id']} marked for resolution")
                else:
                    st.success("✅ Resolved")

def render_exception_category_card(category, data):
    """Render individual exception category card"""
    severity_colors = {
        "critical": "#d62728",
        "error": "#ff7f0e", 
        "warning": "#ff7f0e",
        "info": "#1f77b4"
    }
    
    color = severity_colors[data["severity"]]
    
    st.markdown(f"""
    <div style="border: 1px solid {color}; border-radius: 10px; padding: 1rem; margin: 0.5rem 0;
                background: rgba(31, 119, 180, 0.05);">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <h4 style="margin: 0; color: {color};">{category}</h4>
            <span style="background: {color}; color: white; padding: 0.25rem 0.75rem; 
                         border-radius: 20px; font-weight: 600;">{data['count']}</span>
        </div>
        <p style="margin: 0.5rem 0 0 0; color: #666; font-size: 0.9rem;">{data['description']}</p>
        <div style="margin-top: 0.5rem;">
            <span style="background: rgba(31, 119, 180, 0.1); color: {color}; 
                         padding: 0.25rem 0.5rem; border-radius: 15px; font-size: 0.8rem;">
                {data['severity'].upper()}
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)

def render_approval_queue():
    """Render approval queue and workflow management"""
    st.markdown("## 👥 Approval Queue")
    
    approval_queue = get_approval_queue()
    
    if not approval_queue:
        st.info("🎉 No pending approvals - all exceptions have been resolved!")
        return
    
    # Approval queue controls
    col1, col2, col3 = st.columns(3)
    
    with col1:
        priority_filter = st.selectbox(
            "Filter by Priority",
            ["All", "Critical", "High", "Medium", "Low"],
            help="Filter approvals by priority level"
        )
    
    with col2:
        approval_type = st.selectbox(
            "Approval Type",
            ["All", "sku_mapping", "ledger_mapping", "tax_rate", "data_quality"],
            help="Filter by type of approval needed"
        )
    
    with col3:
        if st.button("🚀 Bulk Approve", width='stretch'):
            st.success("✅ Bulk approval initiated")
    
    # Approval items
    for approval in approval_queue:
        priority_color = {
            "critical": "#d62728",
            "high": "#ff7f0e",
            "medium": "#1f77b4",
            "low": "#2ca02c"
        }[approval["priority"]]
        
        with st.container():
            st.markdown(f"""
            <div style="border: 1px solid {priority_color}; border-radius: 10px; padding: 1rem; 
                        margin: 1rem 0; background: rgba(255, 255, 255, 0.8);">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
                    <h4 style="margin: 0;">{approval['id']} - {approval['description']}</h4>
                    <span style="background: {priority_color}; color: white; padding: 0.25rem 0.75rem; 
                                 border-radius: 20px; font-size: 0.8rem; font-weight: 600;">
                        {approval['priority'].upper()}
                    </span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Approval context and actions
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown("**Context:**")
                for key, value in approval["context"].items():
                    st.markdown(f"- **{key.replace('_', ' ').title()}:** {value}")
                
                st.markdown(f"**Created:** {approval['created'].strftime('%Y-%m-%d %H:%M:%S')}")
            
            with col2:
                st.markdown("**Actions:**")
                
                col_approve, col_reject = st.columns(2)
                
                with col_approve:
                    if st.button("✅ Approve", key=f"approve_{approval['id']}", width='stretch'):
                        st.success(f"✅ {approval['id']} approved successfully")
                
                with col_reject:
                    if st.button("❌ Reject", key=f"reject_{approval['id']}", width='stretch'):
                        st.error(f"❌ {approval['id']} rejected")
                
                if st.button("ℹ️ More Info", key=f"info_{approval['id']}", width='stretch'):
                    st.info(f"Detailed information for {approval['id']}")

def render_exception_resolution():
    """Render exception resolution tools and workflows"""
    st.markdown("## 🔧 Exception Resolution")
    
    tab1, tab2, tab3 = st.tabs(["🔧 Manual Resolution", "⚙️ Auto-Resolution Rules", "📊 Resolution Analytics"])
    
    with tab1:
        render_manual_resolution()
    
    with tab2:
        render_auto_resolution_rules()
    
    with tab3:
        render_resolution_analytics()

def render_manual_resolution():
    """Render manual exception resolution interface"""
    st.markdown("### 🔧 Manual Exception Resolution")
    
    # Exception selection
    col1, col2 = st.columns(2)
    
    with col1:
        exception_id = st.text_input(
            "Exception ID",
            placeholder="Enter exception ID (e.g., EXC-001)",
            help="Enter the specific exception ID to resolve"
        )
    
    with col2:
        resolution_type = st.selectbox(
            "Resolution Type",
            ["Approve Mapping", "Update Master Data", "Override Validation", "Mark as Resolved", "Escalate"],
            help="Select the type of resolution action"
        )
    
    # Resolution details
    resolution_notes = st.text_area(
        "Resolution Notes",
        placeholder="Enter detailed notes about the resolution...",
        help="Provide detailed explanation of the resolution"
    )
    
    # Resolution actions
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔧 Apply Resolution", type="primary", width='stretch'):
            if exception_id and resolution_notes:
                st.success(f"✅ Resolution applied to {exception_id}")
            else:
                st.error("Please provide exception ID and resolution notes")
    
    with col2:
        if st.button("💾 Save Draft", width='stretch'):
            st.info("💾 Resolution draft saved")
    
    with col3:
        if st.button("📧 Notify Team", width='stretch'):
            st.success("📧 Team notification sent")

def render_auto_resolution_rules():
    """Render auto-resolution rules configuration"""
    st.markdown("### ⚙️ Auto-Resolution Rules")
    
    # Current rules
    st.markdown("#### 📋 Active Rules")
    
    rules = [
        {
            "name": "Auto-approve standard SKUs",
            "condition": "SKU matches pattern 'STD-*'",
            "action": "Approve mapping",
            "status": "Active"
        },
        {
            "name": "Auto-resolve duplicate invoices",
            "condition": "Invoice number exists with same amount",
            "action": "Mark as duplicate",
            "status": "Active"
        },
        {
            "name": "Auto-approve small amounts",
            "condition": "Transaction amount < ₹1,000",
            "action": "Auto-approve",
            "status": "Inactive"
        }
    ]
    
    for i, rule in enumerate(rules):
        with st.expander(f"📜 {rule['name']}", expanded=False):
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown(f"**Condition:** {rule['condition']}")
                st.markdown(f"**Action:** {rule['action']}")
                st.markdown(f"**Status:** {rule['status']}")
            
            with col2:
                status_color = "#2ca02c" if rule['status'] == "Active" else "#666666"
                
                if st.button(f"{'🔴 Disable' if rule['status'] == 'Active' else '🟢 Enable'}", 
                           key=f"toggle_rule_{i}"):
                    new_status = "Inactive" if rule['status'] == "Active" else "Active"
                    st.success(f"Rule {new_status.lower()}")
    
    # Add new rule
    st.markdown("#### ➕ Add New Rule")
    
    col1, col2 = st.columns(2)
    
    with col1:
        rule_name = st.text_input("Rule Name", placeholder="Enter rule name...")
        rule_condition = st.text_input("Condition", placeholder="Enter condition logic...")
    
    with col2:
        rule_action = st.selectbox("Action", ["Approve", "Reject", "Escalate", "Auto-resolve"])
        rule_priority = st.selectbox("Priority", ["High", "Medium", "Low"])
    
    if st.button("➕ Add Rule", type="primary"):
        if rule_name and rule_condition:
            st.success(f"✅ Rule '{rule_name}' added successfully")
        else:
            st.error("Please provide rule name and condition")

def render_resolution_analytics():
    """Render resolution analytics and metrics"""
    st.markdown("### 📊 Resolution Analytics")
    
    # Resolution metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Avg Resolution Time", "2.3 hours", delta="-0.5 hours", delta_color="inverse")
    
    with col2:
        st.metric("Auto-Resolution Rate", "67.8%", delta="+5.2%")
    
    with col3:
        st.metric("Escalation Rate", "8.5%", delta="-1.2%", delta_color="inverse")
    
    with col4:
        st.metric("User Satisfaction", "4.2/5", delta="+0.1")
    
    # Resolution breakdown
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🔧 Resolution Methods")
        
        resolution_methods = pd.DataFrame([
            {"Method": "Auto-Resolution", "Count": 156, "Percentage": "67.8%"},
            {"Method": "Manual Approval", "Count": 45, "Percentage": "19.6%"},
            {"Method": "Data Update", "Count": 23, "Percentage": "10.0%"},
            {"Method": "Escalation", "Count": 6, "Percentage": "2.6%"}
        ])
        
        st.dataframe(resolution_methods, width='stretch', hide_index=True)
    
    with col2:
        st.markdown("#### ⏱️ Resolution Time Distribution")
        
        time_distribution = pd.DataFrame([
            {"Time Range": "< 1 hour", "Count": 89, "Percentage": "38.7%"},
            {"Time Range": "1-4 hours", "Count": 67, "Percentage": "29.1%"},
            {"Time Range": "4-24 hours", "Count": 45, "Percentage": "19.6%"},
            {"Time Range": "> 24 hours", "Count": 29, "Percentage": "12.6%"}
        ])
        
        st.dataframe(time_distribution, width='stretch', hide_index=True)

if __name__ == "__main__":
    main()
