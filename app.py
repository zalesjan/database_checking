import streamlit as st
import pandas as pd
import json
import os
import re
from datetime import datetime
from modules.validate_files_module import check_tree_integrity_optimized, value_counts_for_each_distinct_value, distinct_value_counts, distinct_values_with_counts, validate_file, log_validation, distinct_asc_values_each_column
from modules.database_utils import composed_site_id_tree, get_db_connection, load_data_with_copy_command, move_data_to_tree, update_unique_plot_id
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
    # PRESENCE OF KEY COLUMNS AND DATA (FORMAT) RESTRICTIONS
    if st.button("CHECK PRESENCE OF KEY COLUMNS AND DATA FORMAT RESTRICTIONS"):
        validation_results = validate_file(df, config, uploaded_file.name)

    # Data (range) validation
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
        
    # CHECK DATA FOR INTEGRITY
    grouped = dataframe_for_tree_integrity(df)

    # Initialize Streamlit session state to keep track of which tests have been run
    if 'current_test' not in st.session_state:
        st.session_state.current_test = 0  # This will control which test to run
    if 'integrity_issues' not in st.session_state:
        st.session_state.integrity_issues = {}  # Store results for each test

    # Select the columns needed for the integrity checks
    columns_to_check = ['tree_id', 'wildcard_id', 'dbh', 'position', 'life', 'integrity', 'full_scientific']
    df_for_integrity_checks = df[columns_to_check]

    # List of tests and corresponding functions
    test_functions = {
        "dbh_reduction": check_dbh_reduction,
        "position_reversal": check_position_change,
        "species_change": check_species_change,
        "life_status_reversal": check_life_status_change,
        "geometry_shift": check_geometry_shift,  
        "missing_in_census": check_missing_census,
        "integrity_reversal": check_integrity_change
    }

    # List of test names (same order as above)
    test_names = list(test_functions.keys())

    # Display a sequential button for each test
    if st.session_state.current_test < len(test_names):
        current_test_name = test_names[st.session_state.current_test]
        if st.button(f"Run {current_test_name} test"):
            # Run the corresponding test function
            test_function = test_functions[current_test_name]
            result = test_function(grouped)

            # Store the result in the session state
            st.session_state.integrity_issues[current_test_name] = result

            # Display the result
            st.write(f"Results for {current_test_name} test:")
            st.dataframe(result)  # Display the resulting dataframe

            # Increment the current test index to show the next test button
            st.session_state.current_test += 1

    # Display final results once all tests are run
    if st.session_state.current_test >= len(test_names):
        st.write("All tests completed. Here are the results for each test:")
        for test, result in st.session_state.integrity_issues.items():
            st.write(f"### {test.capitalize()} Test Results:")
            st.dataframe(result)

    # Optional button to reset the tests
    if st.button("Reset Tests"):
        st.session_state.current_test = 0
        st.session_state.integrity_issues = {}
        st.write("All tests have been reset.")
    
    # CHECK DATA FOR INTEGRITY
    if st.button("CHECK DATA FOR INTEGRITY"):
        integrity_issues = check_tree_integrity_optimized(df)
        write_and_log(f'Running Integrity issues: {integrity_issues}')
    
    #DATABASE UPLOADS

    musi tam prijit heslo
    # Button to copy data to the database
    if st.button("Copy Data to Database"):
        write_and_log(f'attempting to upload: {uploaded_file}')
        load_data_with_copy_command(df, uploaded_file_path, table_name, ordered_core_attributes, extra_columns)
        write_and_log("Data successfully copied to the database.")

# HELPER FUNCTIONS (set the record values, moving to tree)
helper_operations = {
    "Move Data to Tree Table": move_data_to_tree, 
    "Update unique_plot_id in tree_staging": update_unique_plot_id, 
    "Update composed_site_id in tree table": composed_site_id_tree,}
helper_operation = st.selectbox("CHOOSE A HELPER OPERATION", list(helper_operations.keys()))

# Button to trigger the selected operation
if st.button("Run that helper operation"):
    write_and_log(f'Running function: {helper_operation}')
    helper_operations[helper_operation]()
    write_and_log(f"{helper_operation} successfully completed")

