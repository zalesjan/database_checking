import streamlit as st
import concurrent.futures
from modules.validate_files_module import plausibility_test, run_tests_in_background, send_email
from modules.dataframe_actions import determine_configs, dataframe_for_tree_integrity, df_from_uploaded_file
from modules.logs import write_and_log
import logging
    
logging.basicConfig(
    filename='logs.log',  # Specify your log file name
    level=logging.INFO,  # Set the logging level to INFO or WARNING
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the format of log messages
)
logging.info('Logging setup is complete.')

# Page Name
st.title("2_Plausibility_Tests")

# FILE UPLOAD
uploaded_file = st.file_uploader("Choose a file", type=["csv", "txt"])
if uploaded_file:
    df, uploaded_file_path = df_from_uploaded_file(uploaded_file)

    #GET CONFIGS
    table_name, ordered_core_attributes, core_columns_string, config, core_and_alternative_columns = determine_configs(uploaded_file.name, df.columns)

    # PLAUSIBILITY TEST
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