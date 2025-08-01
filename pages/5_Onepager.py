import pandas as pd
import streamlit as st
from modules.logs import write_and_log, do_action_after_role_check
from modules.validate_files_module import find_previous_record_id_columns_from_mapping, run_parallel_plausibility_tests, value_counts_for_each_distinct_value, distinct_values_with_counts, validate_file, tree_smaller_than_threshold
from modules.dataframe_actions import determine_order, etl_process_df, extract_file_name, df_from_uploaded_file, dataframe_for_tree_integrity
from modules.database_utils import load_data_with_copy_command, password_check, select_role, do_query, foreign_key_mismatch, setup_logins, sanitize_institute_name
from modules.database_utils import truncate_tree_staging, move_data_to_tree, tree_staging_id, plots_id, site_design_id, cwd_id, show_counts_of_all
from modules.database_utils import basic_query_calc_basal_area, basic_query_main_query, basic_query_no_plots_per_year, truncate_no_plots_per_year, truncate_calc_basal_area
from modules.database_utils import basic_query_lying, basic_query_standing, truncate_lying, truncate_standing

from psycopg2 import sql
import os

def process_copy_all_files(sorted_files, role, institute = None):
    previous_table_name = None  # Keep track of the last uploaded table
    previous_table_count = None  # Store the count of unique record_IDs in the previous table
    
    for name, file_object, _ in sorted_files:

        unwanted_keywords = ["template", "query", "with", "docx"]
        if not any(keyword in name.lower() for keyword in unwanted_keywords):            
            # ETL
            df, uploaded_file_path = df_from_uploaded_file(file_object, header_line_idx = None)
            df_columns = {str(col).lower(): col for col in df.columns}
            table_name, ordered_core_attributes, extra_columns, ignored_columns, config, column_mapping, table_mapping, header_line_idx = etl_process_df(role, name, uploaded_file_path, df_columns, df)

            # Dynamically handle raw data role
            if role in ["role_superuser_DB_VUK-raw_data", "VUK-stage"]:
                df, uploaded_file_path = df_from_uploaded_file(file_object, header_line_idx)
                df_columns = {str(col).lower(): col for col in df.columns}

                schema = table_name
                table_name = (
                    name.lower()
                    .replace("-", "_")
                    .replace(".", "_")
                    .replace(" ", "_"))
                
                # Create table dynamically in a schema (e.g. biodiversity_raw)
                create_stmt = f'CREATE SCHEMA IF NOT EXISTS {schema};'
                do_query(create_stmt, role)

                # Create table with all columns as VARCHAR
                column_defs = ",\n".join([f'"{col}" VARCHAR' for col in df_columns])
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
                                            ordered_core_attributes = [col.lower() for col in df.columns],
                                            extra_columns=[],
                                            ignored_columns=[],
                                            role=role)
                write_and_log(f"✅ Raw data from `{file_name}` loaded to {schema}.{table_name}`")

            else:
                schema = "public"
                sites_updated_rows = 0 

                # COPY DATA TO DATABASE
                load_data_with_copy_command(df, schema, uploaded_file_path, table_name, column_mapping, ordered_core_attributes, extra_columns, ignored_columns, role)
                write_and_log(f"Data copy of {file_name} to the database is complete.")
                
                # Store Institute if it's "sites"
                if table_name == "sites":
                    institute = df["institute"].iloc[0]
                    sites_updated_rows = len(df["institute"])
                    previous_table_count = len(df["institute"])
                    
                    sanitized_institute = sanitize_institute_name(institute)
                    create_role = f"""CREATE ROLE {sanitized_institute} WITH LOGIN PASSWORD %s;"""
                    do_query(create_role, role, (f"%{sanitized_institute}%",))

                    setup_logins(institute, sanitized_institute, table_name, role)
                    

                if table_name == "site_design":
                    design_updated_rows, _ = do_query(site_design_id, role, (f"%{institute}%",) )
                    write_and_log(f"✅ Updated {design_updated_rows} rows in site_design with {previous_table_count} unique site values, and {sites_updated_rows} rows in sites.")
                    if role == "moje":
                        setup_logins(institute, sanitized_institute, table_name, role)

                if table_name == "plots":
                    plots_updated_rows, _ = do_query(plots_id, role, (f"%{institute}%",))
                    write_and_log(f"✅ Updated {plots_updated_rows} rows in plots and {design_updated_rows} rows in site_design.")
                    if role == "moje":
                        setup_logins(institute, sanitized_institute, table_name, role)
                else:
                    plots_updated_rows = False

                if table_name == "cwd":
                    cwd_updated_rows, _=do_query(cwd_id, role, (f"%{institute}%",))
                    write_and_log(f"✅ Updated {cwd_updated_rows} rows in cwd and {plots_updated_rows} rows in plots.")
                    if role == "moje":
                        setup_logins(institute, sanitized_institute, table_name, role)

                if table_name == "tree_staging":
                    tree_staging_updated_rows, _ =do_query(tree_staging_id, role, (f"%{institute}%",))
                    write_and_log(f"✅ Updated {tree_staging_updated_rows} rows in tree_staging and {plots_updated_rows} rows in plots.")
                    
                    moved_data_to_tree_from_staging, _ = do_query(move_data_to_tree, role)
                    do_query(truncate_tree_staging, role)
                    write_and_log(f"✅ {moved_data_to_tree_from_staging} rows moved from tree_staging to tree, PS: tree_staging was truncated and is therefore ready for next tree table")

                    tree_smaller_than_threshold(institute, role) 
                    write_and_log(f"Help functions of {file_name} is complete.")
                    if role == "moje":
                        setup_logins(institute, sanitized_institute, table_name, role)
                    
    st.success("All files copied to the database successfully.")    

    if role not in ["role_superuser_DB_VUK-raw_data", "VUK-stage"]:
        do_query(truncate_calc_basal_area, role)
        basic_query_calc_basal_area_df, _ = do_query(basic_query_calc_basal_area, role, (f"%{institute}%",))
        do_query(truncate_no_plots_per_year, role)
        basic_query_no_plots_per_year_df, _ = do_query(basic_query_no_plots_per_year, role, (f"%{institute}%",))
        do_query(truncate_standing, role)
        do_query(truncate_lying, role)
        _, basic_query_standing_df = do_query(basic_query_standing, role)
        _, basic_query_lying_df = do_query(basic_query_lying, role)
        _, basic_query_main_query_df = do_query(basic_query_main_query, role)
        if basic_query_main_query_df is not None:
            # Define the filename
            basic_query_file = f"temp_dir/basic_query_file_{institute}.csv"
            # Save the DataFrame as a CSV file
            basic_query_main_query_df.to_csv(basic_query_file, index=False)
            print(f"✅ Data saved successfully to {basic_query_file}")
            write_and_log("Test of basic queries: (numTrees and Basal area/Ha); DBH min/max/mean was saved to Temp Dir ")
            st.dataframe(basic_query_main_query_df)  # Display the result as a DataFrame
        else:
            write_and_log("⚠️ No data was returned from the basic query:\n{basic_query_main_query}.")
            
# Page Name
st.title("5_Onepager")

if password_check():
    role = select_role()
    
    # Multi-file uploader
    if "uploaded_files" not in st.session_state:
        st.session_state["uploaded_files"] = []    
    uploaded_files = st.file_uploader(
        "Upload files (CSV, TXT, or Excel)",
        type=["csv", "txt", "xls", "xlsx"],
        accept_multiple_files=True
    )
    
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
        
        #for name, path, item_order in sorted_files:
            #print(f"{name} -> {path} order> {item_order}")     
        
        if st.button("Run the validation") and uploaded_files:
            previous_table_name = None  # Keep track of the last uploaded table
            previous_table_count = None  # Store the count of unique record_IDs in the previous table
            
            for name, file_object, _ in sorted_files:

                df, uploaded_file_path = df_from_uploaded_file(file_object, header_line_idx= None)      # Create DF (dataframe_actions)

                table_name, ordered_core_attributes, extra_columns, ignored_columns, config, column_mapping, table_mapping, header_line_idx = etl_process_df(role, name, uploaded_file_path, df.columns, df) # ETL (dataframe_actions)

                # Store Institute if it's "sites"
                if table_name == "sites":
                    institute = df["institute"].iloc[0]
                    sites_updated_rows = len(df["institute"])
                    previous_table_count = len(df["institute"])

                # COUNTING OF PRIMARY AND FOREIGN KEYS
                # This is to keep track that the amount of primary keys fit to their prevoius table: e.g. if I upload 3 sites in sites table, then in design, 3 sites should be referenced
                previous_record_id_columns = find_previous_record_id_columns_from_mapping(table_mapping, table_name)
                
                # Ensure we handle multiple columns correctly
                if previous_record_id_columns and previous_record_id_columns is not None:
                    if isinstance(previous_record_id_columns, str):
                        previous_record_id_columns = [previous_record_id_columns]  # Convert single column name to list
                    print(f"Previous record ID columns for {table_name}: {previous_record_id_columns}")

                    # Ensure all required columns exist in df
                    valid_columns = [col for col in previous_record_id_columns if col in df.columns]

                    if valid_columns:  # Ensure we have at least one valid column to check
                        # Convert multiple columns into tuples (composite keys)
                        unique_current_FK_value = df[valid_columns].drop_duplicates().apply(tuple, axis=1).nunique()
                        print(f"🔹 Unique Composite FKs for {table_name}: {unique_current_FK_value}")
                    else:
                        unique_current_FK_value = 0  # Default to 0 if none of the columns exist
                        print(f"⚠️ Warning: No valid columns found in `{table_name}` to determine unique FKs.")

                    # Get the current table's order from table_mapping, Ensure `table_name` is correctly retrieved from table_mapping
                    found_table_name = None
                    for key, values in table_mapping.items():
                        if values[0] == table_name:  # Match the table_name against the stored table name
                            found_table_name = key
                            break

                    if found_table_name is None:
                        print(f"⚠️ Warning: `{table_name}` not found in `table_mapping` keys!")
                    else:
                        # Retrieve current table order
                        current_table_order = table_mapping[found_table_name][2]  # Get order                    
                        #print(f"🔹 Current Table Order for {table_name}: {current_table_order}")
                        
                        previous_table_name = None  # Default in case there's no previous table
                        for key, (true_table_name, _, order, _, _) in table_mapping.items():
                            if order == current_table_order - 1:  # Find the previous table (order - 1)
                                previous_table_name = true_table_name
                                # FOR DEBUGGING print(f"🔹 Previous Table Found: {previous_table_name}")
                                break  # Stop after finding the previous table


                                        # Foreign Key Validation: Ensure the uploaded table has valid parent references
                    if previous_table_name:
                        # 🔹 Fetch the correct primary key count for the previous table before comparison
                        get_fk_count = f"SELECT COUNT(DISTINCT record_id) FROM {previous_table_name} WHERE composed_site_id like %s"
                        _, previous_table_count_df = do_query(get_fk_count, role, (f"%{institute}%", ))
                        print(f"🔍 Debugging `previous_table_count_df`:\n{previous_table_count_df}")

                        print(f"{previous_table_name}: {previous_table_count}: {previous_table_count_df}")
                        if previous_table_count_df is not None and not previous_table_count_df.empty:
                            previous_table_count = previous_table_count_df.iloc[0, 0]  # Extract the correct primary key count
                            print(f"🟢 Corrected previous_table_count for `{previous_table_name}`: {previous_table_count}")

                    # Comparing the number of Foreign keys of this table to the number of Primary keys of the previous table.
                    if foreign_key_mismatch(table_name, unique_current_FK_value, previous_table_name, previous_table_count):
                        st.warning(f"⚠️ Foreign Key Validation Failed for `{table_name}`. "
                                f"Expected {previous_table_count} references from `{previous_table_name}`, "
                                f"but found {unique_current_FK_value}.")
                    else:
                        print(f"✅ Foreign Key Validation Passed for `{table_name}`")

                    # ✅ Save `unique_current_PK_value` as the new `previous_table_count` for the next iteration
                    previous_table_count = unique_current_FK_value

                # DATA VALIDATION
                validation_results, columns_for_exploration = validate_file(df, config, file.name) # (validate_files_module)
                
                # Exclude certain columns from exploration
                excluded_columns = {'tree_id', 'height', 'dbh'}
                columns_for_exploration = [col for col in columns_for_exploration if col not in excluded_columns]

                st.session_state['columns_for_exploration'] = columns_for_exploration
                if columns_for_exploration:
                    st.warning(f"These {len(columns_for_exploration)} columns did not pass the test and need further exploration:")
                    write_and_log(f"{columns_for_exploration}")
                else:
                    st.success("No columns need further exploration.")

                # DATA VERIFICATION ON FAILED COLUMNS
                if 'columns_for_exploration' in st.session_state and st.session_state['columns_for_exploration']:
                    distinct_values_with_counts(df, st.session_state['columns_for_exploration'])
                    value_counts_for_each_distinct_value(df, st.session_state['columns_for_exploration'])
                else:
                    st.info("Upload files first to explore your data.")
                
                # PLAUSIBILITY TEST
                if table_name == "tree_staging": 
                    df_integrity_lpi_id, df_integrity_spi_id = dataframe_for_tree_integrity(df)
                    run_parallel_plausibility_tests(df_integrity_lpi_id, df_integrity_spi_id, df, file)

        
        # 🔹 Single Button to Copy All Files to Database at the End
        if st.button("Copy all files to Database"):
            if role == "role_superuser_DB_PROD":  # This role requires confirmation before proceeding
                st.session_state['upload_confirmation_needed'] = True
                st.rerun()

            else:
                # ✅ Role is safe, immediately proceed
                process_copy_all_files(sorted_files, role, institute = None)

        # 🔹 If confirmation is needed (for sensitive role like "vukoz")
        if st.session_state.get('upload_confirmation_needed', False):
            st.warning("⚠️ You are using the production role 'vukoz'. Please confirm before uploading data!")
            confirm = st.checkbox("Yes, I confirm I want to proceed with uploading to PROD", key="confirm_prod_upload")

            if confirm:
                st.success("Confirmation received. Now you can start uploading.")
                if st.button("🚀 FINAL CONFIRM: Really copy all files to Database"):
                    process_copy_all_files(sorted_files, role, institute = None)
                    st.session_state['upload_confirmation_needed'] = False  # Reset after success
            
        if st.button("Crap, this upload went terribly wrong, I want to delete all data from this institute"):
            for name, file_object, _ in sorted_files:
                lower_file_name = name.lower()  # FIXED: Call `.lower()`
                if "sites" in lower_file_name:
                    df, _ = df_from_uploaded_file(file)  # Load DF to extract institute
                    institute = df["institute"].iloc[0]

            for file in reversed(sorted_files):
                # ETL
                df, uploaded_file_path = df_from_uploaded_file(file, header_line_idx)
                table_name, ordered_core_attributes, extra_columns, ignored_columns, config, column_mapping, table_mapping, header_line_idx = etl_process_df(role, name, uploaded_file_path, df.columns, df)

                table_to_delete = table_mapping.get(file.name, (None, None, None, None))[0]
                
                # FIXED: Remove `CASCADE` (not valid in DELETE, only for DROP TABLE)
                truncate_all_tables = f"DELETE FROM {table_to_delete} WHERE composed_site_id LIKE %s;"
                
                do_query(truncate_all_tables, role, (f"%{institute}%",)) 
                print(f"Deleting data from table: {table_to_delete}")

        if st.button("show how many sites, plots and trees we have"):           
            _, show_counts_of_all_df = do_query(show_counts_of_all, role)
            if show_counts_of_all_df is not None:
                # Define the filename
                show_counts_of_all_file = f"temp_dir/show_counts_of_all_file.csv"
                # Save the DataFrame as a CSV file
                show_counts_of_all_df.to_csv(show_counts_of_all_file, index=False)
                write_and_log("This many sites, plots and trees we have:")
                st.dataframe(show_counts_of_all_df)  # Display the result as a DataFrame

    if st.button("Synchronize the raw data from sharepoint"):  
        institute = "WR"
        sanitized_institute = sanitize_institute_name(institute)
        
        role = "role_superuser_DB_VUK-raw_data"
        sharepoint_local_path = r"C:\Users\zalesak\vukoz.cz\WILDCARD_EUFORIA_DATA - EUFoRIa Data Upload\WR"
        #"C:\Users\zalesak\vukoz.cz\WILDCARD_EUFORIA_DATA - EUFoRIa Data Upload\VUKOZ"
        #"C:\Users\zalesak\Downloads\Databáze vývoj\data\ver2.0\almost approved"
        #"C:\Users\zalesak\vukoz.cz\WILDCARD_EUFORIA_DATA - EUFoRIa Data Upload\WR"
        # Define accepted file extensions
        accepted_extensions = ('.csv', '.txt', '.xls', '.xlsx')
        
            
        file_order = []  # holds tuples: (file_name, full_path, order_value)
        for root, _, files in os.walk(sharepoint_local_path):
            for file in files:
                if file.lower().endswith(accepted_extensions):
                    full_path = os.path.join(root, file)
                    file_name = extract_file_name(file).lower()
                    
                    # ✅ Safe fallback for missing order
                    order_result = determine_order(file_name)
                    if order_result is None:
                        print(f"⚠️ No order for {file_name}, using default 999")
                        order_number = 999
                    elif isinstance(order_result, tuple):
                        # if determine_order returns something like ("wr_test.txt", 4)
                        order_number = order_result[1]
                    else:
                        order_number = order_result

                    file_order.append((file_name, full_path, order_number))

        # ✅ Sort by order number
        file_order.sort(key=lambda x: x[2])  # x[2] = order_number (int)

        sorted_files = file_order

        print(f"🗂️ Found {len(sorted_files)} files.")
        for name, path, order in sorted_files:
            print(f"  ✅ {name} | {path} | order: {order}")

        process_copy_all_files(sorted_files, role, institute)
        write_and_log(f"synchronize_raw_data is at its end.")
          