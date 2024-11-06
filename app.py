import streamlit as st
import pandas as pd
import json
import os
import re
import concurrent.futures
from datetime import datetime
from modules.validate_files_module import query_check, run_tests_in_background, send_email, value_counts_for_each_distinct_value, distinct_value_counts, distinct_values_with_counts, validate_file, log_validation, distinct_asc_values_each_column, plausibility_test
from modules.database_utils import site_password, get_db_connection, load_data_with_copy_command, do_query, composed_site_id_sites, move_data_to_tree, update_unique_plot_id_3stg, get_wildcard_db_id, composed_site_id_to_all
from modules.dataframe_actions import determine_configs, dataframe_for_tree_integrity
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

    #GET CONFIGS AND COLUMNS based on file name
    table_name, ordered_core_attributes, core_columns_string, config, core_and_alternative_columns = determine_configs(uploaded_file.name, df.columns)
    # Determine the extra columns that are not part of the ordered_core_attributes
    extra_columns = [col for col in df.columns if col not in core_and_alternative_columns]
    
    # Extract expected column names (main attributes, not alternatives)
    write_and_log(f"Core columns found: {ordered_core_attributes}")
    if extra_columns:
        write_and_log(f"Extra columns found: {extra_columns}")
    else:
        write_and_log("No extra columns found.")

    # VALIDATION
    # PRESENCE OF KEY COLUMNS AND DATA (FORMAT) RESTRICTIONS
    if st.button("CHECK PRESENCE OF KEY COLUMNS AND DATA FORMAT RESTRICTIONS"):
        validation_results = validate_file(df, config, uploaded_file.name)

    # DATA (range) VALIDATION
    explore_functions = {
        "Show ASCENDING Distinct Values in Each Column": distinct_asc_values_each_column,
        "Show Distinct Values in Each Column with their Counts": distinct_values_with_counts,
        "Show Count of Unique Values in Each Column": distinct_value_counts,
        "Show Counts for all Distinct Values for Each Column": value_counts_for_each_distinct_value,
    }

    # Dropdown menu to select a function + Button to trigger it
    function_choice = st.selectbox("EXPLORE DATA IN THE UPLOADED FILE", list(explore_functions.keys()))
    if st.button("Run"):
        write_and_log(f'Running function: {function_choice} on file: {uploaded_file.name}')
        explore_functions[function_choice](df)  # Call the selected function
    
    # PLAUSIBILITY
    if table_name == "tree_staging": 
        df_integrity_lpi_id, df_integrity_spi_id = dataframe_for_tree_integrity(df)
        # Get user email input
        email = st.text_input("Enter your email to receive the results:", key="email")

        if st.button("Run All Plausibility Tests"):
            if email:
                # Run tests in background
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(run_tests_in_background, df_integrity_lpi_id, email, df, xpi = 'lpi_id')
                    executor.submit(run_tests_in_background, df_integrity_spi_id, email, df, xpi = 'spi_id')
                write_and_log(f'Tests are running in the background. Results will be sent to your email: {email}.')
            else:
                st.write("Please enter a valid email.")
        
#DATABASE ACTIONS
# Create a password input field
user_password = st.text_input("To upload to database enter password", type="password")
PASSWORD = st.secrets["general"]["site_password"]

# Check if the password entered by the user is correct
if user_password == PASSWORD:
    st.success("Password is correct. You can now proceed.")
    # COPY TO DATABASE
    if st.button("Copy Data to Database"):
        write_and_log(f'attempting to upload: {uploaded_file}')
        load_data_with_copy_command(df, uploaded_file_path, table_name, ordered_core_attributes, extra_columns)
        write_and_log("Data successfully copied to the database.")

    # HELPER FUNCTIONS (set the record_id values, moving to tree)
    helper_operations = {
        "Move Data to Tree Table": move_data_to_tree, 
        "Update unique_plot_id in tree_staging": update_unique_plot_id_3stg, 
        "Update composed_site_id in sites table": composed_site_id_sites,
        "Propagate composed_site_id from sites table to all tables":composed_site_id_to_all,
        "get_wildcard_db_id": get_wildcard_db_id}

    helper_operation_key = st.selectbox("CHOOSE A HELPER OPERATION", list(helper_operations.keys()))
    selected_function = helper_operations[helper_operation_key]

    if st.button("Run that helper operation"):
        if helper_operation_key == "Propagate composed_site_id from sites table to all tables":
            composed_site_id_to_all()
        else:    
            do_query(selected_function)
        write_and_log(f"{selected_function} successfully completed")

    #FILE COMPARISON ("QUERy TESTING")
    # Initialize session state for files if not already done
    if "file_1" not in st.session_state:
        st.session_state["file_1"] = None
    if "file_2" not in st.session_state:
        st.session_state["file_2"] = None

    # File uploaders for two CSV files with state saving
    st.session_state["file_1"] = st.file_uploader("Upload first CSV file", type="csv")
    st.session_state["file_2"] = st.file_uploader("Upload second CSV file", type="csv")

    if st.button("Compare DB output to provider output") and st.session_state["file_1"] and st.session_state["file_2"]:
        # Proceed only if both files are uploaded
        file_1 = st.session_state["file_1"]
        file_2 = st.session_state["file_2"]

        merged_df = query_check(st.session_state["file_1"], st.session_state["file_2"])
        write_and_log("Merged DataFrame with Differences:")
        write_and_log(merged_df)