import streamlit as st
from modules.database_utils import truncate_all_tables, load_data_with_copy_command, move_data_to_tree, tree_staging_id
from modules.database_utils import do_query, password_check, select_role
from modules.dataframe_actions import determine_order, etl_process_df, extract_file_name, df_from_uploaded_file, do_action_after_role_check
from modules.logs import write_and_log
    
# Page Name
st.title("Database Actions")

if password_check():
    role = select_role()

    # FILE UPLOAD and ETL
    # Multi-file uploader
    if "uploaded_files" not in st.session_state:
        st.session_state["uploaded_files"] = []    
    uploaded_files = st.file_uploader(
        "Upload files (CSV, TXT, or Excel)",
        type=["csv", "txt", "xls", "xlsx"],
        accept_multiple_files=True)
      
    if uploaded_files:
        file_order = []  # initialize list to hold tuples (file, order)

        # Determine processing order and Sort files by order 
        for file in uploaded_files:
            file_path = file 

            file_name = extract_file_name(file)
            file_name= file_name.lower()

            order = determine_order(file_name)
            file_order.append((file_name, file_path, order))  # collect the pair

        file_order.sort(key=lambda x: x[2][1])  # sort by order

        sorted_files = [(f_name, f_path, f_order) for f_name, f_path, f_order in file_order]
        
        for name, file_object, _ in sorted_files:

            df, uploaded_file_path = df_from_uploaded_file(file_object, header_line_idx= None)
            table_name, ordered_core_attributes, extra_columns, ignored_columns, config, column_mapping, input_mapping, header_line_idx = etl_process_df( name, df.columns, df)

            # COPY TO DATABASE
            if st.button("Copy Data to Database"):
                write_and_log(f'attempting to upload: {file.name}')

                schema = "public"
                # COPY DATA TO DATABASE
                load_data_with_copy_command(df, schema, table_name, column_mapping, ordered_core_attributes, extra_columns, ignored_columns, role)
                write_and_log(f"Data copy of {file.name} to the database is complete.")
                            
                write_and_log("Data copy to the database is at its end.")
    
    # HELPER FUNCTIONS (set the record_id values, moving to tree)
    helper_operations = {
        "Move Data to Tree Table": move_data_to_tree, 
        "Update plot_record_id in tree_staging": tree_staging_id
        }

    helper_operation_key = st.selectbox("CHOOSE A HELPER OPERATION", list(helper_operations.keys()))
    selected_function = helper_operations[helper_operation_key]

    if st.button("Run that helper operation"):  
        do_query(selected_function, role)
        write_and_log(f"{helper_operation_key}  is at its end.")
    
    if role == "role_superuser_DB_development":
        #  Trigger the truncate action
        if st.button("Truncate all tables and restart numbering"):

            do_action_after_role_check(role, truncate_all_tables, role)
    