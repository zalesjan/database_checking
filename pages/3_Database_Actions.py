import streamlit as st
from modules.database_utils import truncate_all_tables, password_check, select_role, load_data_with_copy_command, do_query, move_data_to_tree, tree_staging_id
from modules.dataframe_actions import etl_process_df, df_from_uploaded_file
from modules.logs import write_and_log, do_action_after_role_check
    
# Page Name
st.title("3_Database_Actions")

if password_check():
    role = select_role()

    # FILE UPLOAD and ETL
    # Multi-file uploader
    # Multi-file uploader
    if "uploaded_files" not in st.session_state:
        st.session_state["uploaded_files"] = []    
    uploaded_files = st.file_uploader(
        "Upload files (CSV, TXT, or Excel)",
        type=["csv", "txt", "xls", "xlsx"],
        accept_multiple_files=True)
        
    if uploaded_files:
        for file in uploaded_files:
            df, uploaded_file_path = df_from_uploaded_file(file)
            table_name, ordered_core_attributes, extra_columns, ignored_columns, config, column_mapping, table_mapping = etl_process_df(file.name, df.columns, df)

            # COPY TO DATABASE
            if st.button("Copy Data to Database"):
                write_and_log(f'attempting to upload: {file.name}')
                load_data_with_copy_command(df, uploaded_file_path, table_name, ordered_core_attributes, extra_columns, ignored_columns, column_mapping, role)
                write_and_log("Data copy to the database is at its end.")
            # COPY ecological attributes to sites 
            #if st.button("Copy ecological attributes to sites tbl of Database"):
            #    write_and_log(f'attempting to upload: {file}')
            #    load_ecological_data_with_copy_command(df, uploaded_file_path, table_name, ordered_core_attributes, extra_columns, ignored_columns)
            #    write_and_log("Data copy to the database is at its end.")
    
    # HELPER FUNCTIONS (set the record_id values, moving to tree)
    helper_operations = {
        "Move Data to Tree Table": move_data_to_tree, 
        "Update unique_plot_id in tree_staging": tree_staging_id, 
        #"Update composed_site_id in sites table": composed_site_id_sites,
        #"Propagate composed_site_id from sites table to all tables":composed_site_id_to_all,
        #"get_wildcard_db_id": get_wildcard_db_id
        }

    helper_operation_key = st.selectbox("CHOOSE A HELPER OPERATION", list(helper_operations.keys()))
    selected_function = helper_operations[helper_operation_key]

    if st.button("Run that helper operation"):  
        do_query(selected_function, role)
        write_and_log(f"{helper_operation_key}  is at its end.")
    
    #  Trigger the truncate action
    if st.button("Truncate all tables and restart numbering"):

        do_action_after_role_check(role, truncate_all_tables, role)
           