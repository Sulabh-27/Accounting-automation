"""
Chart components for Streamlit dashboard using Plotly
"""

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def create_sales_trend_chart(data=None):
    """Create sales trend chart"""
    if data is None:
        # Sample data
        dates = pd.date_range(start='2025-01-01', end='2025-08-31', freq='M')
        sales = [250000, 280000, 320000, 295000, 340000, 380000, 420000, 450000]
        data = pd.DataFrame({'date': dates, 'sales': sales})
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=data['date'],
        y=data['sales'],
        mode='lines+markers',
        name='Sales',
        line=dict(color='#1f77b4', width=3),
        marker=dict(size=8)
    ))
    
    fig.update_layout(
        title="Sales Trend Analysis",
        xaxis_title="Month",
        yaxis_title="Sales Amount (₹)",
        height=400,
        showlegend=False,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig

def create_gst_breakdown_chart():
    """Create GST breakdown pie chart"""
    gst_data = {
        'CGST (9%)': 45000,
        'SGST (9%)': 45000,
        'IGST (18%)': 120000
    }
    
    fig = go.Figure(data=[go.Pie(
        labels=list(gst_data.keys()),
        values=list(gst_data.values()),
        hole=0.4,
        marker_colors=['#ff7f0e', '#2ca02c', '#d62728']
    )])
    
    fig.update_layout(
        title="GST Breakdown",
        height=400,
        showlegend=True,
        legend=dict(orientation="v", yanchor="middle", y=0.5)
    )
    
    return fig

def create_state_wise_sales_chart():
    """Create state-wise sales distribution"""
    states = ['Maharashtra', 'Karnataka', 'Delhi', 'Tamil Nadu', 'Gujarat', 'Uttar Pradesh']
    sales = [850000, 720000, 650000, 580000, 520000, 480000]
    
    fig = go.Figure(data=[go.Bar(
        x=states,
        y=sales,
        marker_color='#1f77b4',
        text=[f'₹{s/1000:.0f}K' for s in sales],
        textposition='auto'
    )])
    
    fig.update_layout(
        title="State-wise Sales Distribution",
        xaxis_title="State",
        yaxis_title="Sales Amount (₹)",
        height=400,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig

def create_channel_performance_chart():
    """Create channel performance comparison"""
    channels = ['Amazon MTR', 'Amazon STR', 'Flipkart', 'Pepperfry']
    transactions = [450, 123, 89, 36]
    success_rates = [98.5, 97.2, 96.8, 95.1]
    
    fig = go.Figure()
    
    # Bar chart for transactions
    fig.add_trace(go.Bar(
        name='Transactions',
        x=channels,
        y=transactions,
        yaxis='y',
        marker_color='#1f77b4',
        text=transactions,
        textposition='auto'
    ))
    
    # Line chart for success rates
    fig.add_trace(go.Scatter(
        name='Success Rate (%)',
        x=channels,
        y=success_rates,
        yaxis='y2',
        mode='lines+markers',
        line=dict(color='#ff7f0e', width=3),
        marker=dict(size=10)
    ))
    
    fig.update_layout(
        title="Channel Performance Analysis",
        xaxis_title="Channel",
        yaxis=dict(title="Transactions", side="left"),
        yaxis2=dict(title="Success Rate (%)", side="right", overlaying="y"),
        height=400,
        legend=dict(x=0.7, y=1),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig

def create_exception_trend_chart():
    """Create exception trend over time"""
    dates = pd.date_range(start='2025-08-01', end='2025-08-31', freq='D')
    critical = np.random.poisson(1, len(dates))
    warning = np.random.poisson(3, len(dates))
    info = np.random.poisson(2, len(dates))
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=dates, y=critical, name='Critical',
        stackgroup='one', fillcolor='rgba(214, 39, 40, 0.7)'
    ))
    
    fig.add_trace(go.Scatter(
        x=dates, y=warning, name='Warning',
        stackgroup='one', fillcolor='rgba(255, 127, 14, 0.7)'
    ))
    
    fig.add_trace(go.Scatter(
        x=dates, y=info, name='Info',
        stackgroup='one', fillcolor='rgba(31, 119, 180, 0.7)'
    ))
    
    fig.update_layout(
        title="Exception Trend (Last 30 Days)",
        xaxis_title="Date",
        yaxis_title="Exception Count",
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    return fig

def create_processing_time_chart():
    """Create processing time analysis"""
    steps = ['Ingestion', 'Mapping', 'Tax Calc', 'Pivot', 'Export', 'MIS Gen']
    avg_time = [120, 45, 30, 60, 90, 15]  # seconds
    
    fig = go.Figure(data=[go.Bar(
        x=steps,
        y=avg_time,
        marker_color=['#2ca02c' if t < 60 else '#ff7f0e' if t < 90 else '#d62728' for t in avg_time],
        text=[f'{t}s' for t in avg_time],
        textposition='auto'
    )])
    
    fig.update_layout(
        title="Average Processing Time by Step",
        xaxis_title="Pipeline Step",
        yaxis_title="Time (seconds)",
        height=400,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig

def create_profitability_chart():
    """Create profitability analysis chart"""
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug']
    revenue = [250000, 280000, 320000, 295000, 340000, 380000, 420000, 450000]
    expenses = [180000, 195000, 220000, 210000, 235000, 260000, 285000, 305000]
    profit = [r - e for r, e in zip(revenue, expenses)]
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name='Revenue',
        x=months,
        y=revenue,
        marker_color='#2ca02c'
    ))
    
    fig.add_trace(go.Bar(
        name='Expenses',
        x=months,
        y=expenses,
        marker_color='#d62728'
    ))
    
    fig.add_trace(go.Scatter(
        name='Profit',
        x=months,
        y=profit,
        mode='lines+markers',
        line=dict(color='#1f77b4', width=3),
        marker=dict(size=8),
        yaxis='y2'
    ))
    
    fig.update_layout(
        title="Revenue, Expenses & Profitability Analysis",
        xaxis_title="Month",
        yaxis=dict(title="Amount (₹)"),
        yaxis2=dict(title="Profit (₹)", overlaying="y", side="right"),
        height=400,
        barmode='group',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    return fig

def create_audit_activity_chart():
    """Create audit activity heatmap"""
    # Generate sample audit data
    hours = list(range(24))
    days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    
    # Create random activity data
    np.random.seed(42)
    activity_data = np.random.poisson(5, (len(days), len(hours)))
    
    fig = go.Figure(data=go.Heatmap(
        z=activity_data,
        x=hours,
        y=days,
        colorscale='Blues',
        showscale=True
    ))
    
    fig.update_layout(
        title="Audit Activity Heatmap (Last 7 Days)",
        xaxis_title="Hour of Day",
        yaxis_title="Day of Week",
        height=300
    )
    
    return fig

def create_status_chart(status_data=None):
    """Create system status overview chart"""
    if status_data is None:
        status_data = {
            'Online': 8,
            'Warning': 2,
            'Error': 1,
            'Maintenance': 1
        }
    
    colors = ['#2ca02c', '#ff7f0e', '#d62728', '#9467bd']
    
    fig = go.Figure(data=[go.Pie(
        labels=list(status_data.keys()),
        values=list(status_data.values()),
        hole=0.6,
        marker_colors=colors
    )])
    
    fig.update_layout(
        title="System Component Status",
        height=300,
        showlegend=True,
        annotations=[dict(text="12<br>Total", x=0.5, y=0.5, font_size=16, showarrow=False)]
    )
    
    return fig
