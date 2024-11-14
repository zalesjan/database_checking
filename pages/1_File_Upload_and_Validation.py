import streamlit as st
from modules.validate_files_module import value_counts_for_each_distinct_value, distinct_value_counts, distinct_values_with_counts, validate_file, log_validation, distinct_asc_values_each_column
from modules.dataframe_actions import determine_configs, df_from_uploaded_file, extra_columns
from modules.logs import write_and_log
import logging
    
logging.basicConfig(
    filename='logs.log',  # Specify your log file name
    level=logging.INFO,  # Set the logging level to INFO or WARNING
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the format of log messages
)
logging.info('Logging setup is complete.')

# Page Name
st.title("File Upload and Validation")
st.markdown("Here you can check your files and validate the data (columns, intervals, allowed values).\nTo start, upload the file that you want to check.")


# FILE UPLOAD
uploaded_file = st.file_uploader("Upload a file to be checked", type=["csv", "txt"])
if uploaded_file:
    df, uploaded_file_path = df_from_uploaded_file(uploaded_file)

    #GET CONFIGS AND COLUMNS based on file name and extra columns that are not part of the ordered_core_attributes, st.write core and extra ones
    table_name, ordered_core_attributes, core_columns_string, config, core_and_alternative_columns = determine_configs(uploaded_file.name, df.columns)
    extra_columns = extra_columns(df, core_and_alternative_columns, ordered_core_attributes)

    # VALIDATION
    # PRESENCE OF KEY COLUMNS AND DATA (FORMAT) RESTRICTIONS
    if st.button("CHECK PRESENCE OF KEY COLUMNS AND DATA FORMAT RESTRICTIONS"):
        validation_results = validate_file(df, config, uploaded_file.name)

    # A CHOICE OF DATA (range) VALIDATION FUNCTIONS TO RUN
    explore_functions = {
        "Show ASCENDING Distinct Values in Each Column": distinct_asc_values_each_column,
        "Show Distinct Values in Each Column with their Counts": distinct_values_with_counts,
        "Show Count of Unique Values in Each Column": distinct_value_counts,
        "Show Counts for all Distinct Values for Each Column": value_counts_for_each_distinct_value}

    # Dropdown menu to select a function + Button to trigger it
    function_choice = st.selectbox("EXPLORE & vALIDATE DATA IN THE UPLOADED FILE", list(explore_functions.keys()))
    if st.button("Run"):
        write_and_log(f'Running function: {function_choice} on file: {uploaded_file.name}')
        explore_functions[function_choice](df)  # Call the selected function
    