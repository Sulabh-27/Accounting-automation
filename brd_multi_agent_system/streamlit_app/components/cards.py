"""
Card components for Streamlit dashboard
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

def create_metric_card(title, value, change, icon):
    """Create a metric card with icon and change indicator"""
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-icon">{icon}</div>
        <div class="metric-label">{title}</div>
        <div class="metric-value">{value}</div>
        <div class="metric-change">{change}</div>
    </div>
    """, unsafe_allow_html=True)

def create_status_card(title, status, description, icon):
    """Create a status card with colored indicator"""
    status_colors = {
        "online": "#2ca02c",
        "warning": "#ff7f0e",
        "error": "#d62728",
        "offline": "#666666"
    }
    
    color = status_colors.get(status, "#666666")
    
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-icon" style="color: {color};">{icon}</div>
        <div class="metric-label">{title}</div>
        <div class="metric-value" style="color: {color};">{status.upper()}</div>
        <div class="metric-change">{description}</div>
    </div>
    """, unsafe_allow_html=True)

def create_progress_card(title, current, total, description):
    """Create a progress card with progress bar"""
    percentage = (current / total) * 100 if total > 0 else 0
    
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{title}</div>
        <div class="metric-value">{current}/{total}</div>
        <div style="background: #e1e5e9; border-radius: 10px; height: 8px; margin: 0.5rem 0;">
            <div style="background: #1f77b4; height: 100%; border-radius: 10px; width: {percentage}%;"></div>
        </div>
        <div class="metric-change">{description}</div>
    </div>
    """, unsafe_allow_html=True)

def create_exception_summary_card():
    """Create exception summary card with breakdown"""
    exceptions = {
        "Critical": 3,
        "Warning": 12,
        "Info": 8
    }
    
    total = sum(exceptions.values())
    
    # Create pie chart
    fig = go.Figure(data=[go.Pie(
        labels=list(exceptions.keys()),
        values=list(exceptions.values()),
        hole=0.6,
        marker_colors=['#d62728', '#ff7f0e', '#1f77b4']
    )])
    
    fig.update_layout(
        showlegend=False,
        height=200,
        margin=dict(t=0, b=0, l=0, r=0),
        annotations=[dict(text=str(total), x=0.5, y=0.5, font_size=20, showarrow=False)]
    )
    
    st.plotly_chart(fig, width='stretch')
    
    # Exception breakdown
    for exc_type, count in exceptions.items():
        color = {"Critical": "#d62728", "Warning": "#ff7f0e", "Info": "#1f77b4"}[exc_type]
        st.markdown(f"""
        <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.25rem 0;">
            <span style="color: {color};">● {exc_type}</span>
            <span style="font-weight: 600;">{count}</span>
        </div>
        """, unsafe_allow_html=True)

def create_processing_timeline_card():
    """Create processing timeline card"""
    timeline_data = [
        {"step": "Ingestion", "status": "completed", "time": "2 min"},
        {"step": "Mapping", "status": "completed", "time": "1 min"},
        {"step": "Tax Calc", "status": "completed", "time": "30 sec"},
        {"step": "Pivot", "status": "in_progress", "time": "45 sec"},
        {"step": "Export", "status": "pending", "time": "-"},
    ]
    
    for item in timeline_data:
        status_icon = {
            "completed": "✅",
            "in_progress": "🔄", 
            "pending": "⏳"
        }[item["status"]]
        
        status_color = {
            "completed": "#2ca02c",
            "in_progress": "#ff7f0e",
            "pending": "#666666"
        }[item["status"]]
        
        st.markdown(f"""
        <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.5rem; 
                    background: rgba(31, 119, 180, 0.05); border-radius: 5px; margin: 0.25rem 0;">
            <span>{status_icon} {item['step']}</span>
            <span style="color: {status_color}; font-weight: 600;">{item['time']}</span>
        </div>
        """, unsafe_allow_html=True)

def create_file_stats_card():
    """Create file statistics card"""
    stats = {
        "X2Beta Files": 15,
        "MIS Reports": 8,
        "Audit Logs": 45,
        "Exception Reports": 3
    }
    
    for file_type, count in stats.items():
        st.markdown(f"""
        <div style="display: flex; justify-content: space-between; align-items: center; 
                    padding: 0.5rem; border-bottom: 1px solid #e1e5e9;">
            <span>{file_type}</span>
            <span style="background: #1f77b4; color: white; padding: 0.25rem 0.5rem; 
                         border-radius: 15px; font-size: 0.8rem;">{count}</span>
        </div>
        """, unsafe_allow_html=True)

def create_channel_performance_card():
    """Create channel performance comparison card"""
    channels = {
        "Amazon MTR": {"processed": 450, "success": 98.5},
        "Amazon STR": {"processed": 123, "success": 97.2},
        "Flipkart": {"processed": 89, "success": 96.8},
        "Pepperfry": {"processed": 36, "success": 95.1}
    }
    
    for channel, data in channels.items():
        success_color = "#2ca02c" if data["success"] >= 98 else "#ff7f0e" if data["success"] >= 95 else "#d62728"
        
        st.markdown(f"""
        <div style="padding: 0.75rem; background: rgba(31, 119, 180, 0.05); 
                    border-radius: 8px; margin: 0.5rem 0;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <span style="font-weight: 600;">{channel}</span>
                <span style="color: {success_color}; font-weight: 600;">{data['success']:.1f}%</span>
            </div>
            <div style="font-size: 0.9rem; color: #666; margin-top: 0.25rem;">
                {data['processed']} transactions processed
            </div>
        </div>
        """, unsafe_allow_html=True)
