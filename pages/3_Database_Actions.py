import streamlit as st
import pandas as pd
import json
import os
import re
from modules.database_utils import site_password, get_db_connection, load_data_with_copy_command, do_query, composed_site_id_sites, move_data_to_tree, update_unique_plot_id_3stg, get_wildcard_db_id, composed_site_id_to_all
from modules.dataframe_actions import determine_configs, dataframe_for_tree_integrity, df_from_uploaded_file, extra_columns
from modules.logs import write_and_log
import logging
    
logging.basicConfig(
    filename='logs.log',  # Specify your log file name
    level=logging.INFO,  # Set the logging level to INFO or WARNING
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the format of log messages
)
logging.info('Logging setup is complete.')

# Page Name
st.title("3_Database_Actions")

# Password input field, check the password entered by the user is correct
user_password = st.text_input("This section is reserved for manupulation with the database. To proceed, enter password", type="password")
PASSWORD = st.secrets["general"]["site_password"]
if user_password == PASSWORD:
    st.success("Password is correct. You can now proceed.")

    # FILE UPLOAD
    uploaded_file = st.file_uploader("Choose a file", type=["csv", "txt"])
    if uploaded_file:
        df, uploaded_file_path = df_from_uploaded_file(uploaded_file)

        #GET CONFIGS AND COLUMNS based on file name and extra columns that are not part of the ordered_core_attributes, st.write core and extra ones
        table_name, ordered_core_attributes, core_columns_string, config, core_and_alternative_columns = determine_configs(uploaded_file.name, df.columns)
        extra_columns = extra_columns(df, core_and_alternative_columns, ordered_core_attributes)
        
        # COPY TO DATABASE
        if st.button("Copy Data to Database"):
            write_and_log(f'attempting to upload: {uploaded_file}')
            load_data_with_copy_command(df, uploaded_file_path, table_name, ordered_core_attributes, extra_columns)
            write_and_log("Data copy to the database is at its end.")

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
        write_and_log(f"{helper_operation_key}  is at its end.")