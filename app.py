import streamlit as st
import pandas as pd
import json
import os
import re
from datetime import datetime
from modules.validate_files_module import check_tree_integrity_optimized, check_tree_integrity, value_counts_for_each_distinct_value, distinct_value_counts, distinct_values_with_counts, validate_file, log_validation, distinct_asc_values_each_column
from modules.database_utils import composed_site_id_tree, get_db_connection, load_data_with_copy_command, move_data_to_tree, update_unique_plot_id
from modules.dataframe_actions import determine_configs
from modules.logs import write_and_log
import logging
    
logging.basicConfig(
    filename='logs.log',  # Specify your log file name
    level=logging.INFO,  # Set the logging level to INFO or WARNING
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the format of log messages
)
logging.info('Logging setup is complete.')

# Streamlit App
st.title("Data Validation and Copy to Database")

# FILE UPLOAD
uploaded_file = st.file_uploader("Choose a file", type=["csv", "txt"])
if uploaded_file:
    # Ensure the temporary directory exists
    temp_dir = "temp_dir"
    os.makedirs(temp_dir, exist_ok=True)

    # Save the uploaded file temporarily
    uploaded_file_path = os.path.join("temp_dir", uploaded_file.name)
    with open(uploaded_file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
        logging.info(f'uploaded_file: {uploaded_file}')

    # Load the file into a DataFrame
    df = pd.read_csv(uploaded_file, delimiter='\t')
    st.write("Data Preview:", df.head())

    #GET CONFIGS
    # Load configuration based on file name
    table_name, ordered_core_attributes, core_columns_string, config, core_and_alternative_columns = determine_configs(uploaded_file.name, df.columns)
    # Determine the extra columns that are not part of the ordered_core_attributes
    extra_columns = [col for col in df.columns if col not in core_and_alternative_columns]
    
    # Extract expected column names (main attributes, not alternatives)
    write_and_log(f"Core columns found: {ordered_core_attributes}")
    #write_and_log(f"DF columns found: {df.columns}")

    # Display the extra columns
    if extra_columns:
        write_and_log(f"Extra columns found: {extra_columns}")
    else:
        write_and_log("No extra columns found.")

    # VALIDATION
    # Button to run validation
    if st.button("CHECK PRESENCE OF KEY COLUMNS AND DATA FORMAT RESTRICTIONS"):
        write_and_log(f'validating file: {uploaded_file}')
        validation_results = validate_file(df, config, uploaded_file.name)
        
    # Dropdown menu to select a validation function to run
    function_choice = st.selectbox("EXPLORE DATA IN THE UPLOADED FILE",
        ("Show ASCENDING Distinct Values in Each Column", "Show Distinct Values in Each Column with their Counts", 
        "Show Count of Unique Values in Each Column", "Show Counts for all Distinct Values for Each Column"))

    if st.button("Run"):
        write_and_log(f'Running function: {function_choice} on file: {uploaded_file}')
        if function_choice == "Show ASCENDING Distinct Values in Each Column":
            distinct_asc_values_each_column(df)
        elif function_choice == "Show Distinct Values in Each Column with their Counts":
            distinct_values_with_counts(df)
        elif function_choice == "Show Count of Unique Values in Each Column":
            distinct_value_counts(df)
        elif function_choice == "Show Counts for all Distinct Values for Each Column":
            value_counts_for_each_distinct_value(df)
    
    if st.button("CHECK DATA FOR INTEGRITY"):
        integrity_issues = check_tree_integrity_optimized(df)
        write_and_log(f'Running Integrity issues: {integrity_issues}')
    #DATABASE UPLOADS
    # Button to copy data to the database
    if st.button("Copy Data to Database"):
        write_and_log(f'attempting to upload: {uploaded_file}')
        load_data_with_copy_command(df, uploaded_file_path, table_name, ordered_core_attributes, extra_columns)
        write_and_log("Data successfully copied to the database.")

# Dropdown menu to select a validation function to run
helper_operation = st.selectbox("CHOSE A HELPER OPERATION",
("Move Data to Tree Table", "Update unique_plot_id in tree_staging", "Update composed_site_id in tree table"))

if st.button("Run that helper operation"):
    write_and_log(f'Running function: {helper_operation}')
    if helper_operation == "Move Data to Tree Table":
        move_data_to_tree()
        write_and_log("Data successfully moved from tree_staging to tree.")
    elif helper_operation == "Update unique_plot_id in tree_staging":
        update_unique_plot_id()
        write_and_log("Unique_plot_id in tree_staging successfully updated")
    elif helper_operation == "Update composed_site_id in tree table":
        composed_site_id_tree()
        write_and_log("composed_site_id in tree successfully updated")
