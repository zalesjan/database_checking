import streamlit as st
from modules.validate_files_module import run_parallel_plausibility_tests
from modules.dataframe_actions import determine_configs, dataframe_for_tree_integrity, df_from_uploaded_file

# Page Name
st.title("Plausibility Tests")

# FILE UPLOAD
uploaded_file = st.file_uploader("Choose a file", type=["csv", "txt"])
if uploaded_file:
    df, uploaded_file_path = df_from_uploaded_file(uploaded_file)

    #GET CONFIGS
    table_name, ordered_core_attributes, core_columns_string, config, core_and_alternative_columns, column_mapping, input_mapping = determine_configs(uploaded_file.name, df.columns)
    
    # PLAUSIBILITY TEST
    if table_name == "tree_staging": 
        df_integrity_plot_id = dataframe_for_tree_integrity(df)
        run_parallel_plausibility_tests(df_integrity_plot_id, df, uploaded_file)