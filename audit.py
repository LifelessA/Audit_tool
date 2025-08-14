import streamlit as st
import pandas as pd
import numpy as np
import json
import base64
import re
from io import BytesIO
import ast

# Set page config
st.set_page_config(
    page_title="Data Inspector",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Session state initialization
if 'primary_data' not in st.session_state:
    st.session_state.primary_data = None
if 'lookup_data' not in st.session_state:
    st.session_state.lookup_data = None
if 'flattened_data' not in st.session_state:
    st.session_state.flattened_data = None
if 'filters' not in st.session_state:
    st.session_state.filters = {}
if 'derived_columns' not in st.session_state:
    st.session_state.derived_columns = {}
if 'comparison_result' not in st.session_state:
    st.session_state.comparison_result = None
if 'global_search' not in st.session_state:
    st.session_state.global_search = ""

# Helper functions
def flatten_json(data):
    """Flatten nested JSON objects"""
    if not data:
        return []
    
    # If data is a single object, convert to list
    if not isinstance(data, list):
        data = [data]
    
    # Recursive function to flatten an object
    def flatten_obj(obj, parent_key='', sep='.'):
        items = {}
        for k, v in obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            if isinstance(v, dict):
                items.update(flatten_obj(v, new_key, sep=sep))
            elif isinstance(v, list):
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        items.update(flatten_obj(item, f"{new_key}[{i}]", sep=sep))
                    else:
                        items[f"{new_key}[{i}]"] = item
            else:
                items[new_key] = v
        return items
    
    # Flatten each object in the list
    flattened = []
    for obj in data:
        flat_obj = flatten_obj(obj)
        flattened.append(flat_obj)
    
    return flattened

def process_uploaded_file(file):
    """Process uploaded file based on file type"""
    if file is None:
        return None
    
    file_type = file.name.split('.')[-1].lower()
    
    if file_type == 'json':
        try:
            data = json.load(file)
            return data
        except json.JSONDecodeError as e:
            st.error(f"Error parsing JSON file: {e}")
            return None
            
    elif file_type in ['csv', 'txt']:
        try:
            df = pd.read_csv(file)
            return df.to_dict('records')
        except Exception as e:
            st.error(f"Error processing CSV file: {e}")
            return None
            
    elif file_type in ['xls', 'xlsx']:
        try:
            df = pd.read_excel(file)
            return df.to_dict('records')
        except Exception as e:
            st.error(f"Error processing Excel file: {e}")
            return None
            
    else:
        st.error(f"Unsupported file type: {file_type}")
        return None

def apply_filters(data):
    """Apply filters to data"""
    filtered_data = data.copy()
    
    # Apply global search
    if st.session_state.global_search:
        search_term = st.session_state.global_search.lower()
        filtered_data = [row for row in filtered_data 
                         if any(str(val).lower().find(search_term) != -1 
                         for val in row.values()]
    
    # Apply column filters
    for col, filter_info in st.session_state.filters.items():
        filter_type = filter_info['type']
        filter_value = filter_info['value']
        
        if filter_type == 'contains':
            filtered_data = [row for row in filtered_data 
                             if col in row and 
                             str(row[col]).lower().find(filter_value.lower()) != -1]
        elif filter_type == 'not_contains':
            filtered_data = [row for row in filtered_data 
                             if col in row and 
                             str(row[col]).lower().find(filter_value.lower()) == -1]
        elif filter_type == 'eq':
            filtered_data = [row for row in filtered_data 
                             if col in row and 
                             str(row[col]).lower() == filter_value.lower()]
        elif filter_type == 'neq':
            filtered_data = [row for row in filtered_data 
                             if col in row and 
                             str(row[col]).lower() != filter_value.lower()]
        elif filter_type == 'gt':
            try:
                filter_value_num = float(filter_value)
                filtered_data = [row for row in filtered_data 
                                 if col in row and 
                                 pd.notnull(row[col]) and 
                                 float(row[col]) > filter_value_num]
            except:
                pass
        elif filter_type == 'lt':
            try:
                filter_value_num = float(filter_value)
                filtered_data = [row for row in filtered_data 
                                 if col in row and 
                                 pd.notnull(row[col]) and 
                                 float(row[col]) < filter_value_num]
            except:
                pass
        elif filter_type == 'gte':
            try:
                filter_value_num = float(filter_value)
                filtered_data = [row for row in filtered_data 
                                 if col in row and 
                                 pd.notnull(row[col]) and 
                                 float(row[col]) >= filter_value_num]
            except:
                pass
        elif filter_type == 'lte':
            try:
                filter_value_num = float(filter_value)
                filtered_data = [row for row in filtered_data 
                                 if col in row and 
                                 pd.notnull(row[col]) and 
                                 float(row[col]) <= filter_value_num]
            except:
                pass
        elif filter_type == 'is_null':
            filtered_data = [row for row in filtered_data 
                             if col not in row or 
                             row[col] is None or 
                             row[col] == '']
        elif filter_type == 'is_not_null':
            filtered_data = [row for row in filtered_data 
                             if col in row and 
                             row[col] is not None and 
                             row[col] != '']
    
    return filtered_data

def add_derived_column(base_column, new_column_name, keywords):
    """Add a derived column based on keyword matching"""
    if not st.session_state.flattened_data:
        return
    
    keyword_list = [kw.strip().lower() for kw in keywords.split(',') if kw.strip()]
    
    for row in st.session_state.flattened_data:
        if base_column in row:
            cell_value = str(row[base_column]).lower()
            found_keyword = next((kw for kw in keyword_list if kw in cell_value), None)
            row[new_column_name] = found_keyword if found_keyword else ""
        else:
            row[new_column_name] = ""
    
    st.session_state.derived_columns[new_column_name] = {
        'base_column': base_column,
        'keywords': keywords
    }

def compare_columns(primary_column, lookup_column):
    """Compare primary and lookup columns"""
    if not st.session_state.flattened_data or not st.session_state.lookup_data:
        return
    
    matches = 0
    mismatches = 0
    
    for i, row in enumerate(st.session_state.flattened_data):
        if i < len(st.session_state.lookup_data):
            lookup_row = st.session_state.lookup_data[i]
            
            if primary_column in row and lookup_column in lookup_row:
                primary_val = str(row[primary_column])
                lookup_val = str(lookup_row[lookup_column])
                
                if primary_val == lookup_val:
                    row[f"{primary_column}_vs_{lookup_column}"] = f"✓ {primary_val}"
                    matches += 1
                else:
                    row[f"{primary_column}_vs_{lookup_column}"] = f"⚠ {primary_val} ≠ {lookup_val}"
                    mismatches += 1
            else:
                row[f"{primary_column}_vs_{lookup_column}"] = "Column missing"
        else:
            row[f"{primary_column}_vs_{lookup_column}"] = "No lookup row"
    
    st.session_state.comparison_result = {
        'primary_column': primary_column,
        'lookup_column': lookup_column,
        'matches': matches,
        'mismatches': mismatches
    }

def reset_all():
    """Reset all application state"""
    st.session_state.primary_data = None
    st.session_state.lookup_data = None
    st.session_state.flattened_data = None
    st.session_state.filters = {}
    st.session_state.derived_columns = {}
    st.session_state.comparison_result = None
    st.session_state.global_search = ""

def export_data(data, format_type):
    """Export data in specified format"""
    if not data:
        st.warning("No data to export")
        return
    
    df = pd.DataFrame(data)
    
    if format_type == 'csv':
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name='data_inspector_export.csv',
            mime='text/csv'
        )
        
    elif format_type == 'json':
        json_data = df.to_json(orient='records', indent=2)
        st.download_button(
            label="Download JSON",
            data=json_data,
            file_name='data_inspector_export.json',
            mime='application/json'
        )
        
    elif format_type == 'excel':
        excel_file = BytesIO()
        with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Data')
        excel_file.seek(0)
        st.download_button(
            label="Download Excel",
            data=excel_file,
            file_name='data_inspector_export.xlsx',
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

# UI Components
def render_header():
    """Render the app header"""
    col1, col2 = st.columns([1, 2])
    with col1:
        st.title("🔍 Data Inspector")
    with col2:
        st.session_state.global_search = st.text_input(
            "Search all columns...", 
            value=st.session_state.global_search,
            key="global_search_input"
        )
    
    st.divider()

def render_data_input():
    """Render data input section"""
    st.subheader("Data Input")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # JSON Editor
        json_input = st.text_area(
            "Paste JSON here or upload a file...", 
            height=200,
            key="json_editor"
        )
        
        if json_input:
            try:
                data = json.loads(json_input)
                st.session_state.primary_data = data
                st.session_state.flattened_data = flatten_json(data)
                st.success("JSON parsed successfully!")
            except json.JSONDecodeError as e:
                st.error(f"Invalid JSON: {e}")
    
    with col2:
        # File Upload
        uploaded_file = st.file_uploader(
            "Or upload a file (JSON, CSV, Excel)", 
            type=['json', 'csv', 'xlsx', 'xls'],
            key="primary_file_uploader"
        )
        
        if uploaded_file:
            data = process_uploaded_file(uploaded_file)
            if data:
                st.session_state.primary_data = data
                st.session_state.flattened_data = flatten_json(data)
                st.success(f"File processed: {uploaded_file.name}")
        
        # Lookup Data
        lookup_file = st.file_uploader(
            "Upload lookup data (optional)", 
            type=['json', 'csv', 'xlsx', 'xls'],
            key="lookup_file_uploader"
        )
        
        if lookup_file:
            data = process_uploaded_file(lookup_file)
            if data:
                st.session_state.lookup_data = data
                st.success(f"Lookup file processed: {lookup_file.name}")

def render_filters():
    """Render filter controls"""
    st.sidebar.subheader("Column Filters")
    
    if not st.session_state.flattened_data:
        st.sidebar.info("Load data to enable filters")
        return
    
    # Get all columns
    all_columns = set()
    for row in st.session_state.flattened_data:
        all_columns.update(row.keys())
    all_columns = sorted(all_columns)
    
    # Add new filter
    with st.sidebar.expander("Add Filter"):
        col1, col2 = st.columns(2)
        with col1:
            filter_column = st.selectbox("Column", all_columns, key="filter_column_select")
        with col2:
            filter_operator = st.selectbox(
                "Operator", 
                ["contains", "not_contains", "eq", "neq", "gt", "lt", "gte", "lte", "is_null", "is_not_null"],
                key="filter_operator_select"
            )
        
        filter_value = ""
        if filter_operator not in ["is_null", "is_not_null"]:
            filter_value = st.text_input("Value", key="filter_value_input")
        
        if st.button("Apply Filter", key="apply_filter_btn"):
            st.session_state.filters[filter_column] = {
                'type': filter_operator,
                'value': filter_value
            }
    
    # Active filters
    if st.session_state.filters:
        st.sidebar.subheader("Active Filters")
        for col, filter_info in st.session_state.filters.items():
            st.sidebar.write(f"**{col}** {filter_info['type']} `{filter_info['value']}`")
            
            if st.sidebar.button(f"Remove {col} filter", key=f"remove_{col}"):
                del st.session_state.filters[col]
                st.rerun()
                
        if st.sidebar.button("Clear All Filters", key="clear_all_filters"):
            st.session_state.filters = {}
            st.rerun()

def render_derived_columns():
    """Render derived column controls"""
    st.sidebar.subheader("Derived Columns")
    
    if not st.session_state.flattened_data:
        st.sidebar.info("Load data to create derived columns")
        return
    
    with st.sidebar.expander("Create New Column"):
        # Get all columns
        all_columns = set()
        for row in st.session_state.flattened_data:
            all_columns.update(row.keys())
        all_columns = sorted(all_columns)
        
        base_column = st.selectbox("Base Column", all_columns, key="derived_base_column")
        new_column = st.text_input("New Column Name", key="derived_new_column")
        keywords = st.text_input("Keywords (comma-separated)", key="derived_keywords")
        
        if st.button("Create Column", key="create_derived_column"):
            if base_column and new_column and keywords:
                add_derived_column(base_column, new_column, keywords)
                st.success(f"Derived column '{new_column}' created!")
            else:
                st.warning("Please fill all fields")
    
    # Active derived columns
    if st.session_state.derived_columns:
        st.sidebar.subheader("Active Derived Columns")
        for col, info in st.session_state.derived_columns.items():
            st.sidebar.write(f"**{col}** (from {info['base_column']})")
            
            if st.sidebar.button(f"Remove {col}", key=f"remove_derived_{col}"):
                # Remove derived column from data
                for row in st.session_state.flattened_data:
                    if col in row:
                        del row[col]
                del st.session_state.derived_columns[col]
                st.rerun()

def render_comparison():
    """Render column comparison controls"""
    st.sidebar.subheader("Data Comparison")
    
    if not st.session_state.flattened_data or not st.session_state.lookup_data:
        st.sidebar.info("Load both primary and lookup data to compare")
        return
    
    # Get primary columns
    primary_columns = set()
    for row in st.session_state.flattened_data:
        primary_columns.update(row.keys())
    primary_columns = sorted(primary_columns)
    
    # Get lookup columns
    lookup_columns = set()
    for row in st.session_state.lookup_data:
        lookup_columns.update(row.keys())
    lookup_columns = sorted(lookup_columns)
    
    with st.sidebar.expander("Compare Columns"):
        primary_col = st.selectbox("Primary Column", primary_columns, key="primary_col_select")
        lookup_col = st.selectbox("Lookup Column", lookup_columns, key="lookup_col_select")
        
        if st.button("Compare Columns", key="compare_columns_btn"):
            compare_columns(primary_col, lookup_col)
    
    # Show comparison results if available
    if st.session_state.comparison_result:
        result = st.session_state.comparison_result
        st.sidebar.success(f"Comparison complete: {result['matches']} matches, {result['mismatches']} mismatches")
        
        if st.sidebar.button("Clear Comparison", key="clear_comparison"):
            col_name = f"{result['primary_column']}_vs_{result['lookup_column']}"
            for row in st.session_state.flattened_data:
                if col_name in row:
                    del row[col_name]
            st.session_state.comparison_result = None
            st.rerun()

def render_data_table():
    """Render the data table"""
    st.subheader("Data Preview")
    
    if not st.session_state.flattened_data:
        st.info("Load data to begin inspection")
        return
    
    # Apply filters
    filtered_data = apply_filters(st.session_state.flattened_data)
    
    if not filtered_data:
        st.warning("No data matches the current filters")
        return
    
    # Convert to DataFrame for display
    df = pd.DataFrame(filtered_data)
    
    # Highlight search results
    def highlight_search(text):
        search_term = st.session_state.global_search.lower()
        if search_term and isinstance(text, str) and search_term in text.lower():
            return f'<span style="background-color: #FFFF00">{text}</span>'
        return text
    
    # Apply highlighting
    styled_df = df.copy()
    for col in styled_df.columns:
        if styled_df[col].dtype == 'object':
            styled_df[col] = styled_df[col].apply(highlight_search)
    
    # Display the table
    st.dataframe(
        styled_df,
        height=600,
        use_container_width=True,
        hide_index=True
    )
    
    # Show summary stats
    st.caption(f"Showing {len(filtered_data)} of {len(st.session_state.flattened_data)} rows")

def render_export():
    """Render export controls"""
    st.subheader("Export Data")
    
    if not st.session_state.flattened_data:
        st.warning("No data to export")
        return
    
    # Apply filters to get current view
    filtered_data = apply_filters(st.session_state.flattened_data) or st.session_state.flattened_data
    
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("Export as CSV", key="export_csv_btn"):
            export_data(filtered_data, 'csv')
    with col2:
        if st.button("Export as JSON", key="export_json_btn"):
            export_data(filtered_data, 'json')
    with col3:
        if st.button("Export as Excel", key="export_excel_btn"):
            export_data(filtered_data, 'excel')

def render_reset():
    """Render reset button"""
    if st.button("Reset All", key="reset_all_btn", use_container_width=True):
        reset_all()
        st.rerun()

# Main App
def main():
    render_header()
    
    # Layout
    col1, col2 = st.columns([3, 1])
    
    with col1:
        render_data_input()
        render_data_table()
        render_export()
        render_reset()
    
    with col2:
        render_filters()
        render_derived_columns()
        render_comparison()

if __name__ == "__main__":
    main()
