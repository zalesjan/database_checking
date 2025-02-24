import streamlit as st
from modules.validate_files_module import value_counts_for_each_distinct_value, distinct_values_with_counts, validate_file
from modules.dataframe_actions import etl_process_df, df_from_uploaded_file
from modules.logs import write_and_log
import logging
    
logging.basicConfig(
    filename='logs.log',  # Specify your log file name
    level=logging.INFO,  # Set the logging level to INFO or WARNING
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the format of log messages
)
logging.info('Logging setup is complete.')

# Page Name
st.title("Data Consistency and Validation")
st.markdown("Here you can check your files and validate the data (columns, intervals, allowed values).\nTo start, upload the file that you want to check.")

# FILE UPLOAD and ETL
uploaded_file = st.file_uploader("Upload a file to be checked", type=["csv", "txt"])
if uploaded_file:
    df, uploaded_file_path = df_from_uploaded_file(uploaded_file)

    table_name, ordered_core_attributes, extra_columns, ignored_columns, config, column_mapping, table_mapping = etl_process_df(uploaded_file.name, df.columns, df)
    
    # VALIDATION
    # PRESENCE OF KEY COLUMNS AND DATA (FORMAT) VALIDATION
    if st.button(f'**CHECK PRESENCE OF KEY COLUMNS AND DATA FORMAT RESTRICTIONS**'):
        validation_results, columns_for_exploration = validate_file(df, config, uploaded_file.name)

        # Store columns_for_exploration in session state
        st.session_state['columns_for_exploration'] = columns_for_exploration

        # Display results
        if columns_for_exploration:
            st.warning("These columns did not pass the test and need further exploration:")
            st.write(f"{columns_for_exploration}")
        else:
            st.success("No columns need further exploration.")

# DATA EXPLORATION ON FAILED COLUMNS
if 'columns_for_exploration' in st.session_state and st.session_state['columns_for_exploration']:
    explore_functions = {
        "Show Distinct Values in Each Column with their Counts": distinct_values_with_counts,
        "Show Counts for all Distinct Values for Each Column": value_counts_for_each_distinct_value,
    }
    function_choice = st.selectbox("Choose an exploration function:", list(explore_functions.keys()))
    if st.button("Run Exploration on those Columns"):
        explore_functions[function_choice](df, st.session_state['columns_for_exploration'])
else:
    st.info("Run validation first to explore columns.")
  
        


        