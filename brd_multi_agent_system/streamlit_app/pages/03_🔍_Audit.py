"""
Audit Trail Page
Searchable audit logs, event tracking, and compliance reporting
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
from streamlit_app.components.charts import create_audit_activity_chart
from streamlit_app.data.sample_data import get_audit_logs

def main():
    setup_page_config()
    load_custom_css()
    
    st.markdown("# 🔍 Audit Trail & Compliance")
    st.markdown("Complete audit trail with immutable event tracking and compliance reporting")
    
    # Audit controls
    render_audit_controls()
    
    st.markdown("---")
    
    # Audit overview
    render_audit_overview()
    
    st.markdown("---")
    
    # Audit logs
    render_audit_logs()
    
    st.markdown("---")
    
    # Compliance reports
    render_compliance_reports()

def render_audit_controls():
    """Render audit search and filter controls"""
    st.markdown("## 🔍 Audit Search & Filters")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        search_term = st.text_input(
            "🔍 Search Events",
            placeholder="Search by event type, actor, or details...",
            help="Search across all audit log fields"
        )
    
    with col2:
        event_types = st.multiselect(
            "Event Types",
            ["INGEST_START", "INGEST_COMPLETE", "MAPPING_START", "MAPPING_COMPLETE", 
             "TAX_COMPUTE_START", "TAX_COMPUTE_COMPLETE", "TALLY_EXPORT", "MIS_GENERATED",
             "EXCEPTION_DETECTED", "EXCEPTION_RESOLVED", "APPROVAL_REQUESTED", "APPROVAL_GRANTED"],
            default=["TALLY_EXPORT", "MIS_GENERATED", "EXCEPTION_RESOLVED"],
            help="Filter by specific event types"
        )
    
    with col3:
        date_range = st.date_input(
            "Date Range",
            value=[datetime.now() - timedelta(days=7), datetime.now()],
            help="Select date range for audit logs"
        )
    
    # Additional filters
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        actors = st.multiselect(
            "Actors",
            ["system", "admin", "user", "api"],
            default=["system", "admin"],
            help="Filter by who performed the action"
        )
    
    with col2:
        severity = st.selectbox(
            "Severity Level",
            ["All", "Info", "Warning", "Error", "Critical"],
            help="Filter by event severity"
        )
    
    with col3:
        entities = st.multiselect(
            "Entity Types",
            ["amazon_mtr", "x2beta_file", "mis_report", "approval_queue", "exception"],
            help="Filter by entity type"
        )
    
    with col4:
        if st.button("🔍 Apply Filters", type="primary", width='stretch'):
            st.success("✅ Filters applied successfully")

def render_audit_overview():
    """Render audit overview and statistics"""
    st.markdown("## 📊 Audit Overview")
    
    # Key audit metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Events",
            "12,456",
            delta="+234",
            help="Total audit events logged"
        )
    
    with col2:
        st.metric(
            "Active Sessions",
            "3",
            delta="+1",
            help="Currently active audit sessions"
        )
    
    with col3:
        st.metric(
            "Data Integrity",
            "100%",
            delta="0%",
            help="Audit log integrity verification"
        )
    
    with col4:
        st.metric(
            "Compliance Score",
            "98.5%",
            delta="+0.3%",
            help="Overall compliance rating"
        )
    
    # Audit activity heatmap
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### 📈 Audit Activity Heatmap")
        fig_heatmap = create_audit_activity_chart()
        st.plotly_chart(fig_heatmap, width='stretch')
    
    with col2:
        st.markdown("### 🎯 Event Categories")
        
        event_stats = {
            "Processing Events": 8456,
            "Export Events": 2134,
            "Exception Events": 1245,
            "Approval Events": 456,
            "System Events": 165
        }
        
        for category, count in event_stats.items():
            st.markdown(f"""
            <div style="display: flex; justify-content: space-between; align-items: center; 
                        padding: 0.5rem; background: rgba(31, 119, 180, 0.05); 
                        border-radius: 5px; margin: 0.25rem 0;">
                <span>{category}</span>
                <span style="background: #1f77b4; color: white; padding: 0.25rem 0.5rem; 
                           border-radius: 15px; font-size: 0.8rem;">{count:,}</span>
            </div>
            """, unsafe_allow_html=True)

def render_audit_logs():
    """Render searchable audit log entries"""
    st.markdown("## 📜 Audit Log Entries")
    
    # Get audit logs
    audit_logs = get_audit_logs()
    
    # Convert to DataFrame for display
    df_logs = pd.DataFrame(audit_logs)
    
    # Format timestamp
    df_logs['timestamp'] = df_logs['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Display options
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        show_details = st.checkbox("Show Metadata", value=True, help="Show detailed metadata for each event")
    
    with col2:
        page_size = st.selectbox("Entries per page", [10, 25, 50, 100], index=1)
    
    with col3:
        if st.button("📥 Export Logs", width='stretch'):
            st.success("📥 Audit logs exported to CSV")
    
    # Display audit logs
    for i, log in enumerate(audit_logs[:page_size]):
        with st.expander(f"🔍 {log['event_type']} - {log['timestamp'].strftime('%H:%M:%S')}", expanded=False):
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown(f"**Event:** {log['event_type']}")
                st.markdown(f"**Actor:** {log['actor']}")
                st.markdown(f"**Action:** {log['action']}")
                st.markdown(f"**Entity:** {log['entity']}")
                st.markdown(f"**Details:** {log['details']}")
            
            with col2:
                st.markdown(f"**Timestamp:** {log['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
                
                if show_details and log.get('metadata'):
                    st.markdown("**Metadata:**")
                    for key, value in log['metadata'].items():
                        st.markdown(f"- {key}: {value}")
                
                # Event severity indicator
                severity_color = {
                    "info": "#1f77b4",
                    "warning": "#ff7f0e",
                    "error": "#d62728",
                    "critical": "#d62728"
                }.get("info", "#1f77b4")  # Default to info
                
                st.markdown(f"""
                <div style="background: {severity_color}; color: white; padding: 0.25rem 0.5rem; 
                           border-radius: 15px; text-align: center; font-size: 0.8rem; margin-top: 0.5rem;">
                    INFO
                </div>
                """, unsafe_allow_html=True)
    
    # Pagination
    if len(audit_logs) > page_size:
        st.markdown(f"Showing {page_size} of {len(audit_logs)} entries")
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col1:
            if st.button("⬅️ Previous"):
                st.info("Previous page functionality")
        with col3:
            if st.button("Next ➡️"):
                st.info("Next page functionality")

def render_compliance_reports():
    """Render compliance and audit reports"""
    st.markdown("## 📋 Compliance Reports")
    
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Summary Report", "🔒 Security Audit", "📈 Performance Report", "⚖️ Regulatory Compliance"])
    
    with tab1:
        render_summary_report()
    
    with tab2:
        render_security_audit()
    
    with tab3:
        render_performance_report()
    
    with tab4:
        render_regulatory_compliance()

def render_summary_report():
    """Render audit summary report"""
    st.markdown("### 📊 Audit Summary Report")
    
    # Summary metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("#### 📈 Processing Statistics")
        st.markdown("- **Total Runs:** 1,247")
        st.markdown("- **Successful Runs:** 1,223 (98.1%)")
        st.markdown("- **Failed Runs:** 24 (1.9%)")
        st.markdown("- **Average Processing Time:** 28.5 seconds")
    
    with col2:
        st.markdown("#### ⚠️ Exception Summary")
        st.markdown("- **Total Exceptions:** 456")
        st.markdown("- **Resolved:** 423 (92.8%)")
        st.markdown("- **Pending:** 33 (7.2%)")
        st.markdown("- **Critical Issues:** 3")
    
    with col3:
        st.markdown("#### 👥 User Activity")
        st.markdown("- **System Events:** 11,234")
        st.markdown("- **Admin Actions:** 1,123")
        st.markdown("- **User Approvals:** 99")
        st.markdown("- **API Calls:** 2,456")
    
    # Compliance checklist
    st.markdown("#### ✅ Compliance Checklist")
    
    compliance_items = [
        ("Audit Log Integrity", "✅", "All logs verified with hash validation"),
        ("Data Retention Policy", "✅", "7-year retention policy enforced"),
        ("Access Control", "✅", "Role-based access implemented"),
        ("Backup & Recovery", "✅", "Daily backups with 99.9% availability"),
        ("Regulatory Reporting", "✅", "GST compliance reports generated"),
        ("Security Monitoring", "⚠️", "2 minor security alerts resolved")
    ]
    
    for item, status, description in compliance_items:
        st.markdown(f"{status} **{item}** - {description}")

def render_security_audit():
    """Render security audit report"""
    st.markdown("### 🔒 Security Audit Report")
    
    # Security metrics
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🛡️ Security Events")
        
        security_events = pd.DataFrame([
            {"Event": "Login Attempt", "Count": 1234, "Status": "Normal", "Risk": "Low"},
            {"Event": "Failed Authentication", "Count": 23, "Status": "Monitored", "Risk": "Medium"},
            {"Event": "Privilege Escalation", "Count": 0, "Status": "None", "Risk": "Low"},
            {"Event": "Data Access", "Count": 5678, "Status": "Normal", "Risk": "Low"},
            {"Event": "Configuration Change", "Count": 12, "Status": "Approved", "Risk": "Low"}
        ])
        
        st.dataframe(security_events, width='stretch', hide_index=True)
    
    with col2:
        st.markdown("#### 🔐 Access Control")
        
        st.markdown("**User Roles:**")
        st.markdown("- Admin: 3 users")
        st.markdown("- Operator: 8 users") 
        st.markdown("- Viewer: 15 users")
        
        st.markdown("**Permissions:**")
        st.markdown("- File Upload: Admin, Operator")
        st.markdown("- Approval Actions: Admin only")
        st.markdown("- Report Generation: All users")
        st.markdown("- System Configuration: Admin only")
    
    # Security recommendations
    st.markdown("#### 🎯 Security Recommendations")
    
    recommendations = [
        "✅ Enable two-factor authentication for admin accounts",
        "⚠️ Review and rotate API keys quarterly",
        "✅ Implement session timeout policies",
        "⚠️ Conduct quarterly security assessments",
        "✅ Monitor for unusual access patterns"
    ]
    
    for rec in recommendations:
        st.markdown(rec)

def render_performance_report():
    """Render performance audit report"""
    st.markdown("### 📈 Performance Audit Report")
    
    # Performance metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Avg Response Time", "245ms", delta="-15ms", delta_color="inverse")
        st.metric("Throughput", "1,250 req/min", delta="+125", delta_color="normal")
    
    with col2:
        st.metric("Error Rate", "0.12%", delta="-0.03%", delta_color="inverse")
        st.metric("Uptime", "99.97%", delta="+0.02%", delta_color="normal")
    
    with col3:
        st.metric("CPU Usage", "23.5%", delta="-2.1%", delta_color="inverse")
        st.metric("Memory Usage", "67.2%", delta="+1.8%", delta_color="normal")
    
    # Performance trends
    st.markdown("#### 📊 Performance Trends")
    
    perf_data = pd.DataFrame([
        {"Metric": "Processing Time", "Current": "28.5s", "Previous": "31.2s", "Change": "-8.7%", "Status": "Improved"},
        {"Metric": "Memory Usage", "Current": "245MB", "Previous": "267MB", "Change": "-8.2%", "Status": "Improved"},
        {"Metric": "Error Rate", "Current": "0.12%", "Previous": "0.18%", "Change": "-33.3%", "Status": "Improved"},
        {"Metric": "Throughput", "Current": "1,250/min", "Previous": "1,125/min", "Change": "+11.1%", "Status": "Improved"}
    ])
    
    st.dataframe(perf_data, width='stretch', hide_index=True)

def render_regulatory_compliance():
    """Render regulatory compliance report"""
    st.markdown("### ⚖️ Regulatory Compliance Report")
    
    # Compliance status
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🏛️ GST Compliance")
        
        gst_compliance = [
            ("GSTR-1 Filing", "✅ Compliant", "Monthly returns filed on time"),
            ("GSTR-3B Filing", "✅ Compliant", "Summary returns submitted"),
            ("Invoice Numbering", "✅ Compliant", "Sequential numbering maintained"),
            ("Tax Calculation", "✅ Compliant", "Accurate GST computation"),
            ("Record Keeping", "✅ Compliant", "7-year retention policy")
        ]
        
        for item, status, description in gst_compliance:
            st.markdown(f"**{item}:** {status}")
            st.caption(description)
    
    with col2:
        st.markdown("#### 📋 Data Protection")
        
        data_protection = [
            ("Data Encryption", "✅ Compliant", "AES-256 encryption at rest"),
            ("Access Logging", "✅ Compliant", "All access events logged"),
            ("Data Backup", "✅ Compliant", "Daily automated backups"),
            ("Retention Policy", "✅ Compliant", "Policy enforced automatically"),
            ("Privacy Controls", "✅ Compliant", "PII protection implemented")
        ]
        
        for item, status, description in data_protection:
            st.markdown(f"**{item}:** {status}")
            st.caption(description)
    
    # Compliance timeline
    st.markdown("#### 📅 Compliance Timeline")
    
    timeline_data = pd.DataFrame([
        {"Date": "2025-08-01", "Event": "Monthly GST Return Filed", "Status": "Completed", "Due Date": "2025-08-20"},
        {"Date": "2025-08-15", "Event": "Quarterly Security Review", "Status": "Completed", "Due Date": "2025-08-31"},
        {"Date": "2025-09-01", "Event": "Annual Compliance Audit", "Status": "Scheduled", "Due Date": "2025-09-30"},
        {"Date": "2025-09-20", "Event": "Next GST Return", "Status": "Upcoming", "Due Date": "2025-09-20"}
    ])
    
    st.dataframe(timeline_data, width='stretch', hide_index=True)

if __name__ == "__main__":
    main()
