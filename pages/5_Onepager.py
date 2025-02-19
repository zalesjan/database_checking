import streamlit as st
from modules.logs import write_and_log
from modules.validate_files_module import run_parallel_plausibility_tests, value_counts_for_each_distinct_value, distinct_values_with_counts, validate_file, tree_smaller_than_threshold
from modules.dataframe_actions import determine_order, etl_process_df, df_from_uploaded_file, dataframe_for_tree_integrity
from modules.database_utils import load_data_with_copy_command, password_check, do_query, move_data_to_tree, tree_staging_id, plots_id, site_design_id

# Page Name
st.title("5_Onepager")

if password_check():
# Multi-file uploader
    if "uploaded_files" not in st.session_state:
        st.session_state["uploaded_files"] = []
        
    uploaded_files = st.file_uploader("Upload CSV files", type=["csv", "txt"], accept_multiple_files=True)
    
    if uploaded_files:
        # Determine processing order
        file_order = [determine_order(file) for file in uploaded_files]

        # Sort files by order (lower number = higher priority)
        file_order.sort(key=lambda x: x[1])  # Sort by order value
        # Extract sorted file list
        sorted_files = [file_tuple[0] for file_tuple in file_order]

        if st.button("Run the validation") and uploaded_files:
            for file in sorted_files:
                df, uploaded_file_path = df_from_uploaded_file(file)
                table_name, ordered_core_attributes, extra_columns, ignored_columns, config, column_mapping = etl_process_df(file.name, df.columns, df)

                # DATA VALIDATION
                validation_results, columns_for_exploration = validate_file(df, config, file.name)
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

                # COPY TO DATABASE 
                if st.button(f"Copy {file.name} to Database"):
                    load_data_with_copy_command(df, uploaded_file_path, table_name, ordered_core_attributes, extra_columns, ignored_columns, column_mapping)
                    write_and_log(f"Data copy of {file} the database is at its end.")
        
        # 🔹 Single Button to Copy All Files to Database at the End
        if st.button("Copy all files to Database"):
            for file in sorted_files:
                df, uploaded_file_path = df_from_uploaded_file(file)
                table_name, ordered_core_attributes, extra_columns, ignored_columns, config, column_mapping = etl_process_df(file.name, df.columns, df)
                get_unique_ids = f"SELECT COUNT(DISTINCT record_id) FROM {table_name}"

                load_data_with_copy_command(df, uploaded_file_path, table_name, ordered_core_attributes, extra_columns, ignored_columns, column_mapping)
                write_and_log(f"Data copy of {file.name} to the database is complete.")
                
                if table_name == "sites":
                    institute = df["institute"].iloc[0]
                    sites_updated_rows = len(df["institute"])
                    write_and_log(f"✅ Updated {sites_updated_rows} rows in sites.")
                    print(f"✅ Updated {sites_updated_rows} rows in sites.")
                    
            
                if table_name == "site_design":
                    design_updated_rows, _ = do_query(site_design_id, (f"%{institute}%",) )
                    write_and_log(f"✅ Updated {design_updated_rows} rows in site_design, and {sites_updated_rows} rows in sites.")
                    print(f"✅ Updated {design_updated_rows} rows in site_design and {sites_updated_rows} rows in sites.")

                    _, unique_designs_own = do_query(get_unique_ids, (table_name,))
                    unique_designs_own_value = unique_designs_own.iloc[0, 0]  # Extract first row, first column
                    write_and_log(f"✅ There is {unique_designs_own_value} unique_designs_own.")
                    print(f"✅ There is {unique_designs_own_value} unique_designs_own.")
                    
                    #parent_id_control() True 
                    #   st.warning

                if table_name == "plots":
                    plots_updated_rows=do_query(plots_id, institute)
                    write_and_log(f"✅ Updated {plots_updated_rows} rows in plots and {design_updated_rows} rows in site_design.")
                    print(f"✅ Updated {plots_updated_rows} rows in plots and {design_updated_rows} rows in site_design.")   
                if table_name == "tree_staging":
                    tree_staging_updated_rows=do_query(tree_staging_id, institute)
                    write_and_log(f"✅ Updated {tree_staging_updated_rows} rows in tree_staging and {plots_updated_rows} rows in plots.")
                    print(f"✅ Updated {tree_staging_updated_rows} rows in tree_staging {plots_updated_rows} rows in plots.")
                    
                    do_query(move_data_to_tree)
                    tree_smaller_than_threshold() 
                    write_and_log(f"Help functions of {file.name} is complete.")

            st.success("All files copied to the database successfully.")    

            