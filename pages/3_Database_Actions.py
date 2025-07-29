import streamlit as st
from modules.database_utils import truncate_all_tables, load_data_with_copy_command, move_data_to_tree, tree_staging_id
from modules.database_utils import do_query, password_check, select_role
from modules.dataframe_actions import determine_order, etl_process_df, extract_file_name, df_from_uploaded_file
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
        file_order = []  # initialize list to hold tuples (file, order)

        # Determine processing order and Sort files by order 
        for file in uploaded_files:
            file_path = file 
            #print(f"this is file path: {file_path}")

            file_name = extract_file_name(file)
            file_name= file_name.lower()
            #print(f"this is file_name: {file_name}")

            order = determine_order(file_name)
            file_order.append((file_name, file_path, order))  # collect the pair
            #print(f"this is file_order: {file_order}")

        file_order.sort(key=lambda x: x[2][1])  # sort by order

        sorted_files = [(f_name, f_path, f_order) for f_name, f_path, f_order in file_order]
        
        for name, file_object, _ in sorted_files:

            df, uploaded_file_path = df_from_uploaded_file(file_object, header_line_idx= None)
            table_name, ordered_core_attributes, extra_columns, ignored_columns, config, column_mapping, table_mapping, header_line_idx = etl_process_df(role, name, uploaded_file_path, df.columns, df)

            # COPY TO DATABASE
            if st.button("Copy Data to Database"):
                write_and_log(f'attempting to upload: {file.name}')
                if role in ["role_superuser_DB_VUK-raw_data", "VUK-stage"]:
                    schema = table_name
                    table_name = (file.name).lower().replace("-", "_").replace(".", "_").replace(" ", "_")
                    
                    # Create table dynamically in a schema (e.g. biodiversity_raw)
                    create_stmt = f'CREATE SCHEMA IF NOT EXISTS {schema};'
                    do_query(create_stmt, role)

                    # Create table with all columns as VARCHAR
                    column_defs = ",\n".join([f'"{col}" VARCHAR' for col in df.columns])
                    create_table_query = f'''
                        CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" (
                            {column_defs}
                        );
                    '''
                    print(create_table_query)
                    do_query(create_table_query, role)
                    write_and_log(f"✅ Table `{schema}.{table_name}` created or already exists.")

                    # Load data directly to the raw schema
                    load_data_with_copy_command(df, schema,
                                                uploaded_file_path, table_name, column_mapping,
                                                ordered_core_attributes=list(df.columns),
                                                extra_columns=[],
                                                ignored_columns=[],
                                                role=role)
                    write_and_log(f"✅ Raw data from `{file.name}` loaded to {schema}.{table_name}`")

                else:
                    schema = "public"
                    # COPY DATA TO DATABASE
                    load_data_with_copy_command(df, schema, uploaded_file_path, table_name, column_mapping, ordered_core_attributes, extra_columns, ignored_columns, role)
                    write_and_log(f"Data copy of {file.name} to the database is complete.")
                            
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
    
    if role == "role_superuser_DB_development":
        #  Trigger the truncate action
        if st.button("Truncate all tables and restart numbering"):

            do_action_after_role_check(role, truncate_all_tables, role)
    
           
    if st.button("Redo the RLS policy"):
                # Define your users and their corresponding institute filters
        user_institute_map = {
            "magda": "__"
            }
        
        """
            "FVA_BW": "FVA-BW",
            "NWFVA": "NWFVA", 
            "VUK": "VUK",
            "UNIUD": "UNIUD",
            "INBO": "INBO",
            "DISAFA_UNITO": "UNITO",
            "IBER_BAS": "IBER-BAS",
            "USV": "USV",
            "WR": "WR",
            "ANF": "ANF",
            "WSL": "WSL",
            "TUZVO": "TUZVO",
            "CULS": "CULS",
            "UL": "UL",
            "WULS": "WULS",
            "BFNP": "BFNP",
            "ICIFOR": "ICIFOR",
            "INCDS": "INCDS",
            "UniTBv": "UniTBv",
            "BGD-NP": "BGD-NP",
            "INRAE": "INRAE",
            "UCPH": "UCPH",
            "NPS": "NPS",
            "iDIV": "iDIV",
            "WuH_NRW": "WuH-NRW",
            "URK": "URK",
            "UEF": "UEF",
            "LWF": "LWF",
            "AlberITF": "AlberITF",
            "LFRLP_FAWF": "LFRLP-FAWF"
        """
        

        # List of tables to apply the RLS to
        target_tables = ["sites", "site_design", "plots", "trees", "metadata", "cwd", "biodiversity", "tree_staging"]

        for sanitized_user, institute_filter in user_institute_map.items():
            for table_name in target_tables:
                sanitized_user = sanitized_user.replace(" ", "_").replace("-", "_").lower()
                
                # 1. Enable and force RLS
                #enable_rls = f"""ALTER TABLE {table_name} ENABLE ROW LEVEL SECURITY;"""
                #force_rls = f"""ALTER TABLE {table_name} FORCE ROW LEVEL SECURITY;"""
                #do_query(enable_rls, role)
                #do_query(force_rls, role)
                if role == "role_superuser_DB_VUK-raw_data":
                    for table_name in target_tables:
                        grant_select = f"""GRANT SELECT ON all TABLES IN SCHEMA {table_name} TO {sanitized_user};"""
                        do_query(grant_select, role)
                else:
                    # 2. Grant SELECT to user
                    grant_select = f"""GRANT SELECT ON TABLE {table_name} TO {sanitized_user};"""
                    do_query(grant_select, role)

                    # 3. Create or replace policy
                    policy_name = f"{sanitized_user}_policy"

                    drop_policy = f"""DROP POLICY IF EXISTS {policy_name} ON {table_name};"""
                    # Special case: give full access to jan_holik
                    if sanitized_user == "tomas_privetivy":
                        create_policy = f"""
                            CREATE POLICY {policy_name}
                                ON {table_name}
                                FOR SELECT
                                TO {sanitized_user}
                                USING (true);
                        """
                    else:
                        like_pattern = f"%{institute_filter}%"
                        create_policy = f"""
                            CREATE POLICY {policy_name}
                            ON {table_name}
                            FOR SELECT
                            TO {sanitized_user}
                            USING (composed_site_id LIKE '{like_pattern}');
                    """
                        
                    do_query(drop_policy, role)
                    do_query(create_policy, role)

                    write_and_log(f"✅ RLS set up for user {sanitized_user} on table {table_name}.")
        