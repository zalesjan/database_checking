import streamlit as st
import concurrent.futures
from modules.logs import write_and_log
from modules.validate_files_module import run_tests_in_background, value_counts_for_each_distinct_value, distinct_values_with_counts, validate_file
from modules.dataframe_actions import etl_process_df, df_from_uploaded_file, dataframe_for_tree_integrity
import logging
    
logging.basicConfig(
    filename='logs.log',  # Specify your log file name
    level=logging.INFO,  # Set the logging level to INFO or WARNING
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the format of log messages
)
logging.info('Logging setup is complete.')

# Page Name
st.title("4_File_Comparison")

#ALL FILES FROM INSTITUTE COMPARISON ("QUERy TESTING")

# Multi-file uploader
uploaded_files = st.file_uploader("Upload CSV files", type=["csv", "txt"], accept_multiple_files=True)

if st.button("Run the validation") and uploaded_files:
    for file in uploaded_files:
        df, uploaded_file_path = df_from_uploaded_file(file)
        table_name, ordered_core_attributes, extra_columns, ignored_columns, config = etl_process_df(file.name, df.columns, df)

        validation_results, columns_for_exploration = validate_file(df, config, file.name)

        #COLUMNS FOR EXPLORATION 
        # Store columns_for_exploration in session state
        st.session_state['columns_for_exploration'] = columns_for_exploration

        # Display results
        if columns_for_exploration:
            st.warning("These columns did not pass the test and need further exploration:")
            write_and_log(f"{columns_for_exploration}")
        else:
            st.success("No columns need further exploration.")

        # DATA EXPLORATION ON FAILED COLUMNS
        if 'columns_for_exploration' in st.session_state and st.session_state['columns_for_exploration']:
            distinct_values_with_counts(df, st.session_state['columns_for_exploration'])
            value_counts_for_each_distinct_value(df, st.session_state['columns_for_exploration'])
        else:
            st.info("Upload files first to explore your data.")
        
        # PLAUSIBILITY TEST
        if table_name == "tree_staging": 
            df_integrity_lpi_id, df_integrity_spi_id = dataframe_for_tree_integrity(df)
            email = None

            # Run tests in background
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.submit(run_tests_in_background, df_integrity_lpi_id, email, df, xpi = 'lpi_id', page = 'onepager')
                executor.submit(run_tests_in_background, df_integrity_spi_id, email, df, xpi = 'spi_id', page = 'onepager')
            write_and_log(f'Tests were run in the background. Results will be saved in JSON file.')
        