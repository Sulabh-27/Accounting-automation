"""
MIS & Analytics Page
Interactive charts, business intelligence, and performance metrics
"""

import streamlit as st
import sys
from pathlib import Path
import pandas as pd

# Add parent directory to path for imports
parent_dir = Path(__file__).parent.parent.parent
sys.path.append(str(parent_dir))

from streamlit_app.components.utils import setup_page_config, load_custom_css, format_currency
from streamlit_app.components.charts import (
    create_sales_trend_chart, create_gst_breakdown_chart, 
    create_state_wise_sales_chart, create_channel_performance_chart,
    create_profitability_chart
)
from streamlit_app.data.sample_data import get_mis_data, get_channel_data

def main():
    setup_page_config()
    load_custom_css()
    
    st.markdown("# 📈 MIS & Analytics Dashboard")
    st.markdown("Comprehensive business intelligence and performance analytics")
    
    # Filters section
    render_filters()
    
    st.markdown("---")
    
    # Key metrics overview
    render_key_metrics()
    
    st.markdown("---")
    
    # Charts section
    render_analytics_charts()
    
    st.markdown("---")
    
    # Detailed tables
    render_detailed_tables()

def render_filters():
    """Render filter controls"""
    st.markdown("## 🔍 Filters & Controls")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        date_range = st.date_input(
            "Date Range",
            value=[pd.to_datetime("2025-08-01"), pd.to_datetime("2025-08-31")],
            help="Select date range for analysis"
        )
    
    with col2:
        channels = st.multiselect(
            "Channels",
            ["Amazon MTR", "Amazon STR", "Flipkart", "Pepperfry"],
            default=["Amazon MTR", "Flipkart"],
            help="Select channels to analyze"
        )
    
    with col3:
        gstins = st.multiselect(
            "GSTINs",
            ["06ABGCS4796R1ZA", "07ABGCS4796R1Z8", "09ABGCS4796R1Z4", "24ABGCS4796R1ZC", "29ABGCS4796R1Z2"],
            default=["06ABGCS4796R1ZA"],
            help="Select company GSTINs"
        )
    
    with col4:
        states = st.multiselect(
            "States",
            ["Maharashtra", "Karnataka", "Delhi", "Tamil Nadu", "Gujarat", "Uttar Pradesh"],
            default=["Maharashtra", "Karnataka", "Delhi"],
            help="Select states for analysis"
        )
    
    # Export options
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col2:
        if st.button("📊 Export Charts", width='stretch'):
            st.success("📊 Charts exported to PDF")
    
    with col3:
        if st.button("📋 Export Data", width='stretch'):
            st.success("📋 Data exported to Excel")

def render_key_metrics():
    """Render key business metrics"""
    st.markdown("## 📊 Key Business Metrics")
    
    mis_data = get_mis_data()
    
    # Sales metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Sales",
            format_currency(mis_data['sales_metrics']['total_sales']),
            delta=format_currency(45000),
            help="Total sales amount for the period"
        )
    
    with col2:
        st.metric(
            "Net Sales", 
            format_currency(mis_data['sales_metrics']['net_sales']),
            delta=format_currency(38000),
            help="Sales after returns and adjustments"
        )
    
    with col3:
        st.metric(
            "Total Transactions",
            f"{mis_data['sales_metrics']['total_transactions']:,}",
            delta="+67",
            help="Number of transactions processed"
        )
    
    with col4:
        st.metric(
            "Average Order Value",
            format_currency(mis_data['sales_metrics']['average_order_value']),
            delta=format_currency(23.45),
            help="Average value per transaction"
        )
    
    # Profitability metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Gross Profit",
            format_currency(mis_data['profitability_metrics']['gross_profit']),
            delta=format_currency(28000),
            help="Revenue minus cost of goods sold"
        )
    
    with col2:
        st.metric(
            "Profit Margin",
            f"{mis_data['profitability_metrics']['profit_margin']:.1f}%",
            delta="+2.3%",
            help="Gross profit as percentage of revenue"
        )
    
    with col3:
        st.metric(
            "GST Liability",
            format_currency(mis_data['gst_metrics']['gst_liability']),
            delta=format_currency(5800),
            help="Net GST payable amount"
        )
    
    with col4:
        st.metric(
            "Data Quality Score",
            f"{mis_data['quality_metrics']['data_quality_score']:.1f}%",
            delta="+1.2%",
            help="Overall data quality and completeness"
        )

def render_analytics_charts():
    """Render interactive analytics charts"""
    st.markdown("## 📈 Analytics Charts")
    
    # First row of charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📈 Sales Trend Analysis")
        fig_sales = create_sales_trend_chart()
        st.plotly_chart(fig_sales, width='stretch')
    
    with col2:
        st.markdown("### 🥧 GST Breakdown")
        fig_gst = create_gst_breakdown_chart()
        st.plotly_chart(fig_gst, width='stretch')
    
    # Second row of charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🗺️ State-wise Sales Distribution")
        fig_states = create_state_wise_sales_chart()
        st.plotly_chart(fig_states, width='stretch')
    
    with col2:
        st.markdown("### 🏪 Channel Performance")
        fig_channels = create_channel_performance_chart()
        st.plotly_chart(fig_channels, width='stretch')
    
    # Full width profitability chart
    st.markdown("### 💰 Revenue, Expenses & Profitability")
    fig_profit = create_profitability_chart()
    st.plotly_chart(fig_profit, width='stretch')

def render_detailed_tables():
    """Render detailed data tables"""
    st.markdown("## 📋 Detailed Analytics")
    
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Sales Summary", "💸 Expense Breakdown", "🏛️ GST Analysis", "🏪 Channel Performance"])
    
    with tab1:
        render_sales_summary_table()
    
    with tab2:
        render_expense_breakdown_table()
    
    with tab3:
        render_gst_analysis_table()
    
    with tab4:
        render_channel_performance_table()

def render_sales_summary_table():
    """Render sales summary table"""
    st.markdown("### 📊 Sales Summary by State")
    
    # Sample sales data
    sales_data = pd.DataFrame([
        {"State": "Maharashtra", "Transactions": 185, "Sales": 125000, "Returns": 3500, "Net Sales": 121500, "Avg Order": 675.68},
        {"State": "Karnataka", "Transactions": 142, "Sales": 98000, "Returns": 2800, "Net Sales": 95200, "Avg Order": 670.42},
        {"State": "Delhi", "Transactions": 128, "Sales": 87000, "Returns": 2200, "Net Sales": 84800, "Avg Order": 662.50},
        {"State": "Tamil Nadu", "Transactions": 95, "Sales": 65000, "Returns": 1800, "Net Sales": 63200, "Avg Order": 665.26},
        {"State": "Gujarat", "Transactions": 78, "Sales": 52000, "Returns": 1500, "Net Sales": 50500, "Avg Order": 647.44},
        {"State": "Uttar Pradesh", "Transactions": 70, "Sales": 48000, "Returns": 1200, "Net Sales": 46800, "Avg Order": 668.57}
    ])
    
    # Format currency columns
    for col in ["Sales", "Returns", "Net Sales", "Avg Order"]:
        sales_data[col] = sales_data[col].apply(lambda x: f"₹{x:,.2f}")
    
    st.dataframe(
        sales_data,
        width='stretch',
        hide_index=True
    )
    
    # Summary metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total States", "6", help="Number of states with sales")
    with col2:
        st.metric("Top State", "Maharashtra", help="Highest sales volume")
    with col3:
        st.metric("Growth Rate", "+12.5%", help="Month-over-month growth")

def render_expense_breakdown_table():
    """Render expense breakdown table"""
    st.markdown("### 💸 Expense Analysis by Category")
    
    expense_data = pd.DataFrame([
        {"Category": "Commission", "Amount": 45000, "Percentage": 36.0, "Channel": "Amazon", "Tax Deductible": "Yes"},
        {"Category": "Shipping", "Amount": 35000, "Percentage": 28.0, "Channel": "Multiple", "Tax Deductible": "Yes"},
        {"Category": "Fulfillment", "Amount": 25000, "Percentage": 20.0, "Channel": "Amazon", "Tax Deductible": "Yes"},
        {"Category": "Advertising", "Amount": 20000, "Percentage": 16.0, "Channel": "Multiple", "Tax Deductible": "Yes"}
    ])
    
    # Format currency
    expense_data["Amount"] = expense_data["Amount"].apply(lambda x: f"₹{x:,.2f}")
    expense_data["Percentage"] = expense_data["Percentage"].apply(lambda x: f"{x:.1f}%")
    
    st.dataframe(
        expense_data,
        width='stretch',
        hide_index=True
    )
    
    # Expense insights
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Expenses", "₹1,25,000", delta="-₹5,000")
    with col2:
        st.metric("Expense Ratio", "27.8%", delta="-1.2%", delta_color="inverse")
    with col3:
        st.metric("Tax Savings", "₹22,500", delta="+₹2,100")

def render_gst_analysis_table():
    """Render GST analysis table"""
    st.markdown("### 🏛️ GST Analysis by Rate")
    
    gst_data = pd.DataFrame([
        {"GST Rate": "0%", "Taxable Value": 85000, "CGST": 0, "SGST": 0, "IGST": 0, "Total Tax": 0},
        {"GST Rate": "5%", "Taxable Value": 0, "CGST": 0, "SGST": 0, "IGST": 0, "Total Tax": 0},
        {"GST Rate": "12%", "Taxable Value": 0, "CGST": 0, "SGST": 0, "IGST": 0, "Total Tax": 0},
        {"GST Rate": "18%", "Taxable Value": 365000, "CGST": 25000, "SGST": 25000, "IGST": 28300, "Total Tax": 78300},
        {"GST Rate": "28%", "Taxable Value": 0, "CGST": 0, "SGST": 0, "IGST": 0, "Total Tax": 0}
    ])
    
    # Format currency columns
    for col in ["Taxable Value", "CGST", "SGST", "IGST", "Total Tax"]:
        gst_data[col] = gst_data[col].apply(lambda x: f"₹{x:,.2f}")
    
    st.dataframe(
        gst_data,
        width='stretch',
        hide_index=True
    )
    
    # GST summary
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Output GST", "₹78,300", help="Total GST collected")
    with col2:
        st.metric("Input GST", "₹22,500", help="GST paid on expenses")
    with col3:
        st.metric("Net Liability", "₹55,800", help="GST payable to government")

def render_channel_performance_table():
    """Render channel performance table"""
    st.markdown("### 🏪 Channel Performance Comparison")
    
    channel_data = get_channel_data()
    
    # Convert to DataFrame
    df_channels = pd.DataFrame([
        {
            "Channel": channel.replace("_", " ").title(),
            "Transactions": data["transactions"],
            "Success Rate": f"{data['success_rate']:.1f}%",
            "Avg Processing Time": f"{data['avg_processing_time']:.1f}s",
            "Total Value": f"₹{data['total_value']:,.2f}",
            "Exceptions": data["exceptions"],
            "Performance": "🟢 Excellent" if data['success_rate'] >= 98 else "🟡 Good" if data['success_rate'] >= 96 else "🔴 Needs Attention"
        }
        for channel, data in channel_data.items()
    ])
    
    st.dataframe(
        df_channels,
        width='stretch',
        hide_index=True
    )
    
    # Channel insights
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Best Performer", "Amazon MTR", help="Highest success rate")
    with col2:
        st.metric("Fastest Processing", "Amazon STR", help="Lowest avg processing time")
    with col3:
        st.metric("Total Channels", "4", help="Active processing channels")

if __name__ == "__main__":
    main()
