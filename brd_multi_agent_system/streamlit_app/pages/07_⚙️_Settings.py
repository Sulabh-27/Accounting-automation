"""
Settings Page
System configuration, user management, and health monitoring
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
from streamlit_app.components.charts import create_status_chart
from streamlit_app.data.sample_data import get_system_health

def main():
    setup_page_config()
    load_custom_css()
    
    st.markdown("# ⚙️ System Settings")
    st.markdown("Configure system settings, manage users, and monitor system health")
    
    # Settings tabs
    render_settings_tabs()

def render_settings_tabs():
    """Render settings management tabs"""
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["🔧 System Config", "👥 User Management", "🏥 System Health", "🔐 Security", "📊 Monitoring"])
    
    with tab1:
        render_system_config()
    
    with tab2:
        render_user_management()
    
    with tab3:
        render_system_health()
    
    with tab4:
        render_security_settings()
    
    with tab5:
        render_monitoring_settings()

def render_system_config():
    """Render system configuration settings"""
    st.markdown("### 🔧 System Configuration")
    
    # Database configuration
    st.markdown("#### 🗄️ Database Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        db_host = st.text_input(
            "Database Host",
            value="supabase.co",
            help="Database server hostname"
        )
        
        db_port = st.number_input(
            "Database Port",
            value=5432,
            min_value=1,
            max_value=65535,
            help="Database server port"
        )
        
        db_name = st.text_input(
            "Database Name",
            value="accounting_system",
            help="Database name"
        )
    
    with col2:
        connection_pool_size = st.number_input(
            "Connection Pool Size",
            value=10,
            min_value=1,
            max_value=100,
            help="Maximum database connections"
        )
        
        query_timeout = st.number_input(
            "Query Timeout (seconds)",
            value=30,
            min_value=5,
            max_value=300,
            help="Database query timeout"
        )
        
        enable_ssl = st.checkbox(
            "Enable SSL",
            value=True,
            help="Use SSL for database connections"
        )
    
    # Test database connection
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col2:
        if st.button("🔍 Test Connection", width='stretch'):
            st.success("✅ Database connection successful")
    
    with col3:
        if st.button("💾 Save DB Config", width='stretch'):
            st.success("💾 Database configuration saved")
    
    # Processing configuration
    st.markdown("#### ⚙️ Processing Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        max_file_size = st.number_input(
            "Max File Size (MB)",
            value=100,
            min_value=1,
            max_value=1000,
            help="Maximum upload file size"
        )
        
        batch_size = st.number_input(
            "Processing Batch Size",
            value=1000,
            min_value=100,
            max_value=10000,
            help="Records processed per batch"
        )
        
        parallel_workers = st.number_input(
            "Parallel Workers",
            value=4,
            min_value=1,
            max_value=16,
            help="Number of parallel processing workers"
        )
    
    with col2:
        enable_caching = st.checkbox(
            "Enable Caching",
            value=True,
            help="Enable result caching for performance"
        )
        
        cache_ttl = st.number_input(
            "Cache TTL (minutes)",
            value=60,
            min_value=1,
            max_value=1440,
            help="Cache time-to-live in minutes"
        )
        
        auto_cleanup = st.checkbox(
            "Auto Cleanup",
            value=True,
            help="Automatically cleanup temporary files"
        )
    
    # File storage configuration
    st.markdown("#### 📁 File Storage Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        storage_type = st.selectbox(
            "Storage Type",
            ["Local", "Supabase Storage", "AWS S3", "Azure Blob"],
            help="File storage backend"
        )
        
        storage_path = st.text_input(
            "Storage Path",
            value="/app/data",
            help="Base path for file storage"
        )
    
    with col2:
        retention_days = st.number_input(
            "File Retention (days)",
            value=365,
            min_value=1,
            max_value=3650,
            help="Number of days to retain files"
        )
        
        backup_enabled = st.checkbox(
            "Enable Backups",
            value=True,
            help="Enable automatic file backups"
        )
    
    # Save all configuration
    if st.button("💾 Save All Configuration", type="primary"):
        st.success("✅ All system configuration saved successfully")

def render_user_management():
    """Render user management interface"""
    st.markdown("### 👥 User Management")
    
    # Current users
    st.markdown("#### 👤 Current Users")
    
    users_data = pd.DataFrame([
        {
            "Username": "admin",
            "Full Name": "System Administrator",
            "Email": "admin@company.com",
            "Role": "Admin",
            "Status": "Active",
            "Last Login": "2025-08-28 14:30:00",
            "Created": "2025-01-01"
        },
        {
            "Username": "operator1",
            "Full Name": "John Operator",
            "Email": "john@company.com",
            "Role": "Operator",
            "Status": "Active", 
            "Last Login": "2025-08-28 09:15:00",
            "Created": "2025-02-15"
        },
        {
            "Username": "viewer1",
            "Full Name": "Jane Viewer",
            "Email": "jane@company.com",
            "Role": "Viewer",
            "Status": "Active",
            "Last Login": "2025-08-27 16:45:00",
            "Created": "2025-03-01"
        },
        {
            "Username": "temp_user",
            "Full Name": "Temporary User",
            "Email": "temp@company.com",
            "Role": "Viewer",
            "Status": "Inactive",
            "Last Login": "2025-08-20 11:30:00",
            "Created": "2025-08-01"
        }
    ])
    
    # User management actions
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col2:
        if st.button("➕ Add User", width='stretch'):
            st.session_state['show_add_user'] = True
    
    with col3:
        if st.button("📥 Export Users", width='stretch'):
            st.success("📥 User list exported to CSV")
    
    # Show add user form if requested
    if st.session_state.get('show_add_user', False):
        render_add_user_form()
    
    # Display users with actions
    for i, user in users_data.iterrows():
        status_color = "#2ca02c" if user['Status'] == "Active" else "#666666"
        
        with st.container():
            st.markdown(f"""
            <div style="border: 1px solid {status_color}; border-radius: 10px; padding: 1rem; 
                        margin: 0.5rem 0; background: rgba(255, 255, 255, 0.8);">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <h4 style="margin: 0; color: {status_color};">👤 {user['Full Name']} ({user['Username']})</h4>
                        <p style="margin: 0.25rem 0; color: #666; font-size: 0.9rem;">
                            Email: {user['Email']} | Role: {user['Role']} | 
                            Last Login: {user['Last Login']}
                        </p>
                        <p style="margin: 0; color: #888; font-size: 0.8rem;">
                            Created: {user['Created']}
                        </p>
                    </div>
                    <div>
                        <span style="background: {status_color}; color: white; padding: 0.25rem 0.75rem; 
                                     border-radius: 20px; font-size: 0.8rem; font-weight: 600;">
                            {user['Status'].upper()}
                        </span>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # User actions
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                if st.button("✏️ Edit", key=f"edit_user_{i}", width='stretch'):
                    st.info(f"✏️ Editing user {user['Username']}")
            
            with col2:
                action_text = "🔴 Deactivate" if user['Status'] == "Active" else "🟢 Activate"
                if st.button(action_text, key=f"toggle_user_{i}", width='stretch'):
                    new_status = "Inactive" if user['Status'] == "Active" else "Active"
                    st.success(f"✅ User {user['Username']} {new_status.lower()}")
            
            with col3:
                if st.button("🔑 Reset Password", key=f"reset_pwd_{i}", width='stretch'):
                    st.success(f"🔑 Password reset email sent to {user['Email']}")
            
            with col4:
                if st.button("🗑️ Delete", key=f"delete_user_{i}", width='stretch'):
                    st.error(f"🗑️ User {user['Username']} marked for deletion")

def render_add_user_form():
    """Render add new user form"""
    with st.expander("➕ Add New User", expanded=True):
        col1, col2 = st.columns(2)
        
        with col1:
            new_username = st.text_input("Username", placeholder="Enter username...")
            new_full_name = st.text_input("Full Name", placeholder="Enter full name...")
            new_email = st.text_input("Email", placeholder="Enter email address...")
        
        with col2:
            new_role = st.selectbox("Role", ["Admin", "Operator", "Viewer"])
            new_password = st.text_input("Password", type="password", placeholder="Enter password...")
            new_status = st.selectbox("Status", ["Active", "Inactive"])
        
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col2:
            if st.button("✅ Add User", width='stretch'):
                if new_username and new_email and new_password:
                    st.success(f"✅ User {new_username} added successfully")
                    st.session_state['show_add_user'] = False
                    st.rerun()
                else:
                    st.error("Please provide username, email, and password")
        
        with col3:
            if st.button("❌ Cancel", width='stretch'):
                st.session_state['show_add_user'] = False
                st.rerun()

def render_system_health():
    """Render system health monitoring"""
    st.markdown("### 🏥 System Health")
    
    health_data = get_system_health()
    
    # Overall system status
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "System Status",
            "🟢 Healthy",
            help="Overall system health status"
        )
    
    with col2:
        st.metric(
            "Uptime",
            "99.97%",
            delta="+0.02%",
            help="System uptime percentage"
        )
    
    with col3:
        st.metric(
            "Active Connections",
            health_data['database']['connections'],
            delta="+2",
            help="Active database connections"
        )
    
    with col4:
        st.metric(
            "Response Time",
            f"{health_data['database']['query_time']}ms",
            delta="-5ms",
            delta_color="inverse",
            help="Average response time"
        )
    
    # System components status
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🗄️ Database Health")
        
        db_health = health_data['database']
        
        st.markdown(f"**Status:** {'🟢 Online' if db_health['status'] == 'online' else '🔴 Offline'}")
        st.markdown(f"**Connections:** {db_health['connections']}/50")
        st.markdown(f"**Query Time:** {db_health['query_time']}ms")
        st.markdown(f"**Storage Used:** {db_health['storage_used']:.1f} GB")
        st.markdown(f"**Last Backup:** {db_health['last_backup'].strftime('%Y-%m-%d %H:%M')}")
        
        # Database actions
        col_a, col_b = st.columns(2)
        with col_a:
            if st.button("🔄 Test DB", width='stretch'):
                st.success("✅ Database connection test passed")
        with col_b:
            if st.button("💾 Backup Now", width='stretch'):
                st.success("💾 Database backup initiated")
    
    with col2:
        st.markdown("#### ⚙️ Processing Health")
        
        proc_health = health_data['processing']
        
        st.markdown(f"**CPU Usage:** {proc_health['cpu_usage']:.1f}%")
        st.markdown(f"**Memory Usage:** {proc_health['memory_usage']:.1f}%")
        st.markdown(f"**Disk Usage:** {proc_health['disk_usage']:.1f}%")
        st.markdown(f"**Active Jobs:** {proc_health['active_jobs']}")
        st.markdown(f"**Queue Size:** {proc_health['queue_size']}")
        
        # Processing actions
        col_a, col_b = st.columns(2)
        with col_a:
            if st.button("📊 View Details", width='stretch'):
                st.info("📊 Detailed performance metrics")
        with col_b:
            if st.button("🧹 Clear Cache", width='stretch'):
                st.success("🧹 System cache cleared")
    
    # Integration status
    st.markdown("#### 🔗 Integration Status")
    
    integrations = health_data['integrations']
    
    for service, status in integrations.items():
        status_icon = "🟢" if status == "online" else "🔴"
        st.markdown(f"**{service.replace('_', ' ').title()}:** {status_icon} {status.title()}")
    
    # System logs
    st.markdown("#### 📜 Recent System Logs")
    
    logs = [
        {"time": "15:45:23", "level": "INFO", "message": "System health check completed successfully"},
        {"time": "15:40:15", "level": "INFO", "message": "Database backup completed"},
        {"time": "15:35:42", "level": "WARN", "message": "High memory usage detected (78%)"},
        {"time": "15:30:18", "level": "INFO", "message": "User login: admin@company.com"},
        {"time": "15:25:33", "level": "INFO", "message": "Processing job completed: 698 records"}
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

def render_security_settings():
    """Render security settings"""
    st.markdown("### 🔐 Security Settings")
    
    # Authentication settings
    st.markdown("#### 🔑 Authentication")
    
    col1, col2 = st.columns(2)
    
    with col1:
        enable_2fa = st.checkbox(
            "Enable Two-Factor Authentication",
            value=True,
            help="Require 2FA for all users"
        )
        
        session_timeout = st.number_input(
            "Session Timeout (minutes)",
            value=60,
            min_value=5,
            max_value=480,
            help="Automatic session timeout"
        )
        
        max_login_attempts = st.number_input(
            "Max Login Attempts",
            value=5,
            min_value=3,
            max_value=10,
            help="Maximum failed login attempts before lockout"
        )
    
    with col2:
        password_min_length = st.number_input(
            "Minimum Password Length",
            value=8,
            min_value=6,
            max_value=20,
            help="Minimum required password length"
        )
        
        password_expiry_days = st.number_input(
            "Password Expiry (days)",
            value=90,
            min_value=30,
            max_value=365,
            help="Password expiration period"
        )
        
        require_password_complexity = st.checkbox(
            "Require Password Complexity",
            value=True,
            help="Require uppercase, lowercase, numbers, and symbols"
        )
    
    # API security
    st.markdown("#### 🔌 API Security")
    
    col1, col2 = st.columns(2)
    
    with col1:
        api_rate_limit = st.number_input(
            "API Rate Limit (requests/minute)",
            value=100,
            min_value=10,
            max_value=1000,
            help="Maximum API requests per minute per user"
        )
        
        api_key_expiry = st.number_input(
            "API Key Expiry (days)",
            value=365,
            min_value=30,
            max_value=3650,
            help="API key expiration period"
        )
    
    with col2:
        enable_api_logging = st.checkbox(
            "Enable API Logging",
            value=True,
            help="Log all API requests and responses"
        )
        
        require_api_authentication = st.checkbox(
            "Require API Authentication",
            value=True,
            help="Require authentication for all API endpoints"
        )
    
    # Security monitoring
    st.markdown("#### 🛡️ Security Monitoring")
    
    col1, col2 = st.columns(2)
    
    with col1:
        enable_intrusion_detection = st.checkbox(
            "Enable Intrusion Detection",
            value=True,
            help="Monitor for suspicious activities"
        )
        
        log_failed_logins = st.checkbox(
            "Log Failed Logins",
            value=True,
            help="Log all failed login attempts"
        )
    
    with col2:
        enable_audit_logging = st.checkbox(
            "Enable Audit Logging",
            value=True,
            help="Log all user actions for audit trail"
        )
        
        alert_on_suspicious_activity = st.checkbox(
            "Alert on Suspicious Activity",
            value=True,
            help="Send alerts for suspicious activities"
        )
    
    # Save security settings
    if st.button("💾 Save Security Settings", type="primary"):
        st.success("✅ Security settings saved successfully")

def render_monitoring_settings():
    """Render monitoring and alerting settings"""
    st.markdown("### 📊 Monitoring & Alerting")
    
    # Performance monitoring
    st.markdown("#### 📈 Performance Monitoring")
    
    col1, col2 = st.columns(2)
    
    with col1:
        enable_performance_monitoring = st.checkbox(
            "Enable Performance Monitoring",
            value=True,
            help="Monitor system performance metrics"
        )
        
        cpu_threshold = st.number_input(
            "CPU Alert Threshold (%)",
            value=80,
            min_value=50,
            max_value=95,
            help="CPU usage threshold for alerts"
        )
        
        memory_threshold = st.number_input(
            "Memory Alert Threshold (%)",
            value=85,
            min_value=50,
            max_value=95,
            help="Memory usage threshold for alerts"
        )
    
    with col2:
        disk_threshold = st.number_input(
            "Disk Alert Threshold (%)",
            value=90,
            min_value=70,
            max_value=95,
            help="Disk usage threshold for alerts"
        )
        
        response_time_threshold = st.number_input(
            "Response Time Threshold (ms)",
            value=1000,
            min_value=100,
            max_value=5000,
            help="Response time threshold for alerts"
        )
        
        monitoring_interval = st.number_input(
            "Monitoring Interval (seconds)",
            value=60,
            min_value=10,
            max_value=300,
            help="How often to collect metrics"
        )
    
    # Alert configuration
    st.markdown("#### 🚨 Alert Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        enable_email_alerts = st.checkbox(
            "Enable Email Alerts",
            value=True,
            help="Send alerts via email"
        )
        
        alert_email = st.text_input(
            "Alert Email Address",
            value="admin@company.com",
            help="Email address for system alerts"
        )
        
        enable_slack_alerts = st.checkbox(
            "Enable Slack Alerts",
            value=False,
            help="Send alerts to Slack channel"
        )
    
    with col2:
        slack_webhook = st.text_input(
            "Slack Webhook URL",
            placeholder="https://hooks.slack.com/...",
            help="Slack webhook URL for alerts"
        )
        
        alert_frequency = st.selectbox(
            "Alert Frequency",
            ["Immediate", "Every 5 minutes", "Every 15 minutes", "Hourly"],
            help="How often to send repeated alerts"
        )
        
        enable_sms_alerts = st.checkbox(
            "Enable SMS Alerts",
            value=False,
            help="Send critical alerts via SMS"
        )
    
    # Log retention
    st.markdown("#### 📜 Log Retention")
    
    col1, col2 = st.columns(2)
    
    with col1:
        system_log_retention = st.number_input(
            "System Log Retention (days)",
            value=90,
            min_value=7,
            max_value=3650,
            help="How long to keep system logs"
        )
        
        audit_log_retention = st.number_input(
            "Audit Log Retention (days)",
            value=2555,  # 7 years
            min_value=365,
            max_value=3650,
            help="How long to keep audit logs"
        )
    
    with col2:
        performance_log_retention = st.number_input(
            "Performance Log Retention (days)",
            value=30,
            min_value=7,
            max_value=365,
            help="How long to keep performance logs"
        )
        
        enable_log_compression = st.checkbox(
            "Enable Log Compression",
            value=True,
            help="Compress old log files to save space"
        )
    
    # Save monitoring settings
    if st.button("💾 Save Monitoring Settings", type="primary"):
        st.success("✅ Monitoring settings saved successfully")

if __name__ == "__main__":
    main()
