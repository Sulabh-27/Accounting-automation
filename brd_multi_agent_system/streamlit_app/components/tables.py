"""
Table components for Streamlit dashboard
Advanced data tables with filtering, sorting, and actions
"""

import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

def create_advanced_table(data, key="table", height=400, selection_mode="single"):
    """Create an advanced data table with AgGrid"""
    
    # Configure grid options
    gb = GridOptionsBuilder.from_dataframe(data)
    gb.configure_pagination(paginationAutoPageSize=True)
    gb.configure_side_bar()
    gb.configure_selection(selection_mode, use_checkbox=True)
    gb.configure_default_column(
        groupable=True,
        value=True,
        enableRowGroup=True,
        aggFunc="sum",
        editable=True
    )
    
    grid_options = gb.build()
    
    # Display grid
    grid_response = AgGrid(
        data,
        gridOptions=grid_options,
        data_return_mode=DataReturnMode.AS_INPUT,
        update_mode=GridUpdateMode.MODEL_CHANGED,
        fit_columns_on_grid_load=False,
        theme='streamlit',
        enable_enterprise_modules=True,
        height=height,
        width='100%',
        reload_data=False,
        key=key
    )
    
    return grid_response

def create_simple_table(data, columns=None, show_index=False):
    """Create a simple formatted table"""
    
    if columns:
        data = data[columns]
    
    st.dataframe(
        data,
        width='stretch',
        hide_index=not show_index
    )

def create_metric_table(metrics_dict, title="Metrics"):
    """Create a metrics table from dictionary"""
    
    st.markdown(f"#### {title}")
    
    for key, value in metrics_dict.items():
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown(f"**{key}:**")
        
        with col2:
            st.markdown(f"`{value}`")

def create_status_table(data, status_column="status"):
    """Create a table with status indicators"""
    
    # Add status colors
    def get_status_color(status):
        colors = {
            "active": "🟢",
            "inactive": "🔴", 
            "pending": "🟡",
            "success": "🟢",
            "failed": "🔴",
            "warning": "🟡",
            "completed": "🟢",
            "in_progress": "🟡",
            "error": "🔴"
        }
        return colors.get(status.lower(), "⚪")
    
    # Apply status colors
    if status_column in data.columns:
        data[status_column] = data[status_column].apply(
            lambda x: f"{get_status_color(x)} {x}"
        )
    
    st.dataframe(data, width='stretch', hide_index=True)

def create_expandable_table(data, expand_column, detail_columns):
    """Create a table with expandable rows"""
    
    for i, row in data.iterrows():
        with st.expander(f"📋 {row[expand_column]}", expanded=False):
            
            col1, col2 = st.columns(2)
            
            mid_point = len(detail_columns) // 2
            
            with col1:
                for col in detail_columns[:mid_point]:
                    if col in row:
                        st.markdown(f"**{col}:** {row[col]}")
            
            with col2:
                for col in detail_columns[mid_point:]:
                    if col in row:
                        st.markdown(f"**{col}:** {row[col]}")

def create_action_table(data, actions=None):
    """Create a table with action buttons"""
    
    if actions is None:
        actions = ["Edit", "Delete", "View"]
    
    for i, row in data.iterrows():
        
        # Display row data
        cols = st.columns(len(data.columns) + 1)
        
        for j, (col_name, value) in enumerate(row.items()):
            with cols[j]:
                st.markdown(f"**{col_name}:** {value}")
        
        # Action buttons
        with cols[-1]:
            action_cols = st.columns(len(actions))
            
            for k, action in enumerate(actions):
                with action_cols[k]:
                    if st.button(action, key=f"{action}_{i}"):
                        st.success(f"{action} clicked for row {i}")

def create_comparison_table(data1, data2, labels=["Before", "After"]):
    """Create a side-by-side comparison table"""
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"#### {labels[0]}")
        st.dataframe(data1, width='stretch', hide_index=True)
    
    with col2:
        st.markdown(f"#### {labels[1]}")
        st.dataframe(data2, width='stretch', hide_index=True)

def create_summary_table(data, group_by, agg_functions):
    """Create a summary table with aggregations"""
    
    if group_by in data.columns:
        summary = data.groupby(group_by).agg(agg_functions).reset_index()
        
        st.markdown(f"#### Summary by {group_by}")
        st.dataframe(summary, width='stretch', hide_index=True)
        
        return summary
    else:
        st.error(f"Column '{group_by}' not found in data")
        return None

def create_pivot_table(data, index, columns, values, aggfunc='sum'):
    """Create a pivot table"""
    
    try:
        pivot = pd.pivot_table(
            data, 
            index=index, 
            columns=columns, 
            values=values, 
            aggfunc=aggfunc,
            fill_value=0
        )
        
        st.markdown(f"#### Pivot Table: {values} by {index} and {columns}")
        st.dataframe(pivot, width='stretch')
        
        return pivot
        
    except Exception as e:
        st.error(f"Error creating pivot table: {e}")
        return None

def create_filtered_table(data, filters=None):
    """Create a table with interactive filters"""
    
    if filters is None:
        filters = {}
    
    # Create filter controls
    st.markdown("#### Filters")
    
    filter_cols = st.columns(len(filters) if filters else 3)
    
    applied_filters = {}
    
    for i, (col_name, filter_type) in enumerate(filters.items()):
        
        if i < len(filter_cols):
            with filter_cols[i]:
                
                if filter_type == "selectbox":
                    unique_values = ["All"] + list(data[col_name].unique())
                    selected = st.selectbox(f"Filter by {col_name}", unique_values)
                    if selected != "All":
                        applied_filters[col_name] = selected
                
                elif filter_type == "multiselect":
                    unique_values = list(data[col_name].unique())
                    selected = st.multiselect(f"Filter by {col_name}", unique_values)
                    if selected:
                        applied_filters[col_name] = selected
                
                elif filter_type == "slider":
                    min_val = float(data[col_name].min())
                    max_val = float(data[col_name].max())
                    selected = st.slider(
                        f"Filter by {col_name}", 
                        min_val, 
                        max_val, 
                        (min_val, max_val)
                    )
                    applied_filters[col_name] = selected
    
    # Apply filters
    filtered_data = data.copy()
    
    for col_name, filter_value in applied_filters.items():
        
        if isinstance(filter_value, list):
            filtered_data = filtered_data[filtered_data[col_name].isin(filter_value)]
        
        elif isinstance(filter_value, tuple):
            filtered_data = filtered_data[
                (filtered_data[col_name] >= filter_value[0]) & 
                (filtered_data[col_name] <= filter_value[1])
            ]
        
        else:
            filtered_data = filtered_data[filtered_data[col_name] == filter_value]
    
    # Display filtered table
    st.markdown(f"#### Filtered Results ({len(filtered_data)} rows)")
    st.dataframe(filtered_data, width='stretch', hide_index=True)
    
    return filtered_data
