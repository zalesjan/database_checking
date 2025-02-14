import json
import os
import pandas as pd
import streamlit as st
from modules.logs import write_and_log

def df_from_uploaded_file(uploaded_file):
    # Ensure the temporary directory exists
    temp_dir = "temp_dir"
    os.makedirs(temp_dir, exist_ok=True)

    # Save the uploaded file temporarily
    uploaded_file_path = os.path.join("temp_dir", uploaded_file.name)
    with open(uploaded_file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    # Load the file into a DataFrame
    df = pd.read_csv(uploaded_file, delimiter='\t')
    st.header(f'{uploaded_file.name}')
    st.write("Data Preview:", df.head())
    return df, uploaded_file_path

def df_from_detected_file(file_path):
    # Ensure the temporary directory exists
    temp_dir = "temp_dir"
    os.makedirs(temp_dir, exist_ok=True)

    # Load the file into a DataFrame
    df = pd.read_csv(file_path, delimiter='\t')
    return df

def etl_process_df(uploaded_file_name, df_columns, df):
    unique_key_prefix = f"{uploaded_file_name}_"

    #GET CONFIGS AND COLUMNS based on file name and extra columns that are not part of the ordered_core_attributes, st.write core and extra ones
    table_name, ordered_core_attributes, core_columns_string, config, core_and_alternative_columns, column_mapping = determine_configs(uploaded_file_name, df_columns)
    extra_columns = find_extra_columns(df, core_and_alternative_columns, ordered_core_attributes)
    
    # Option to ignore columns
    ignore_columns_option = st.checkbox("Do you want to ignore some columns?", key=f"{unique_key_prefix}ignore_columns")
    ignored_columns = []
    if ignore_columns_option:
        # Dynamically generate checkboxes for each column
        ignored_columns = st.multiselect("Columns to ignore", options=df.columns,  key=f"{unique_key_prefix}ignore_columns")

    # Display the ignored columns (for confirmation)
    if ignored_columns:
        st.write("You chose to ignore these columns:", ignored_columns, key=f"{unique_key_prefix}ignore_columns")

    return table_name, ordered_core_attributes, extra_columns, ignored_columns, config, column_mapping

def find_extra_columns(df, core_and_alternative_columns, ordered_core_attributes):
    # Extract expected column names (main attributes, not alternatives)
    extra_columns = [col for col in df.columns if col not in core_and_alternative_columns]
    st.warning(f"These {len(ordered_core_attributes)} columns for basic (mandatory) attributes were found:")
    st.write(f"{ordered_core_attributes}")
    if extra_columns:
        st.warning(f"These {len(extra_columns)} extended attributes columns were found")
        st.write(f"{extra_columns}")
    else:
        st.write("No extra columns found.")
    return extra_columns if extra_columns else []

def prepare_dataframe_for_copy(df, ordered_core_attributes, extra_columns, column_mapping, ignored_columns=None):
    """
    Prepares the DataFrame to merge core data with a JSONB column `extended_attributes` for COPY command.
    
    Args:
        df (pd.DataFrame): Original DataFrame with core and extra columns.
        core_attributes (list): List of core attributes to retain.
        extra_columns (list): List of extra columns to combine into JSONB.

    Returns:
        pd.DataFrame: Modified DataFrame ready for COPY command.
    """
    # Step 1: Create a copy of the DataFrame to avoid slice issues
    df_for_copy = df.copy()
    st.write("Data Preview:", df_for_copy.head())
    # Step 4: Rename columns to their primary names using `column_mapping`
    df_for_copy = df_for_copy.rename(columns=column_mapping)
    st.write("Data Preview:", df_for_copy.head())
    
    #Step 2: Remove columns to ignore from both core and extra columns
    if ignored_columns:
        ordered_core_attributes = [col for col in ordered_core_attributes if col not in ignored_columns]
        extra_columns = [col for col in extra_columns if col not in ignored_columns]

    # Step 2: Keep only the necessary columns (both core and extra)
    df_for_copy = df_for_copy[ordered_core_attributes].copy()
    st.write("Data Preview:", df_for_copy.head())
    # Create a new column `extended_attributes` by combining extra columns into a JSON string
    if extra_columns:
        df_for_copy.loc[:, 'extended_attributes'] = df.loc[:, extra_columns].apply(lambda row: json.dumps(row.dropna().to_dict()), axis=1)
    
    df_for_copy = df_for_copy.copy()

    return df_for_copy


def determine_copy_command_for_ecology_with_ignore(file_path, df_columns, extra_columns, table_name, ignored_columns=None):
    """
    Determines the core attributes and constructs the COPY command including extended attributes.
    
    Args:
        file_path (str): Path to the file being processed.
        df_columns (list): List of columns in the DataFrame.
        extra_columns (list): List of extra columns to be stored as JSONB.
        table_name (str): Name of the table to insert data into.
        ignored_columns (list, optional): List of columns to be ignored from the DataFrame.
    
    Returns:
        copy_command (str): The COPY command for inserting data.
    """
    if ignored_columns is None:
        ignored_columns = []

    # Filter out ignored columns
    filtered_columns = [col for col in df_columns if col not in ignored_columns]

    # Map the filtered DataFrame columns to core attributes
    core_attributes = [col for col in filtered_columns if col not in extra_columns]
    
    if extra_columns:
        # Join core columns into a comma-separated string
        columns_string = ", ".join(core_attributes + ["extended_attributes"])
    else:
        columns_string = ", ".join(core_attributes)

    # Create the COPY command to include core columns and JSONB `extended_attributes`
    copy_command = f"""
    UPDATE public.{table_name} 
    ({columns_string}) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) NULL '\\N';"""
    
    write_and_log(f'copy_command: {copy_command}')
    return copy_command

def determine_copy_command_with_ignore(file_path, df_columns, extra_columns, table_name, ignored_columns=None):
    """
    Determines the core attributes and constructs the COPY command including extended attributes.
    
    Args:
        file_path (str): Path to the file being processed.
        df_columns (list): List of columns in the DataFrame.
        extra_columns (list): List of extra columns to be stored as JSONB.
        table_name (str): Name of the table to insert data into.
        ignored_columns (list, optional): List of columns to be ignored from the DataFrame.
    
    Returns:
        copy_command (str): The COPY command for inserting data.
    """
    if ignored_columns is None:
        ignored_columns = []

    # Filter out ignored columns
    filtered_columns = [col for col in df_columns if col not in ignored_columns]

    # Map the filtered DataFrame columns to core attributes
    core_attributes = [col for col in filtered_columns if col not in extra_columns]
    
    if extra_columns:
        # Join core columns into a comma-separated string
        columns_string = ", ".join(core_attributes + ["extended_attributes"])
    else:
        columns_string = ", ".join(core_attributes)

    # Create the COPY command to include core columns and JSONB `extended_attributes`
    copy_command = f"""
    COPY public.{table_name} 
    ({columns_string}) 
    FROM STDIN WITH DELIMITER E'\t' CSV HEADER NULL '\\N';"""
    
    write_and_log(f'copy_command: {copy_command}')
    return copy_command

table_mapping = {
        "sites": ("sites", "expectations/expe_sites.json", 1),
        "design": ("site_design", "expectations/expe_site_design.json", 2),
        "plots": ("plots", "expectations/expe_plots.json", 3),
        "standing": ("tree_staging", "expectations/expe_standing.json", 4),
        "lying": ("tree_staging", "expectations/expe_lying.json", 5),
        "cwd": ("cwd", "expectations/expe_cwd.json", 6),
        "metadata": ("metadata", "expectations/expe_metadata.json", 6)
    }

def determine_configs(file_path, df_columns):
    # Define the mapping of base filename content to table names
    
    base_filename = os.path.basename(file_path).lower()
    
    # Find the appropriate table name and config file based on the filename
    for key, (table_name, config_file, _) in table_mapping.items():
        if key in base_filename:
            # Load the config file
            with open(config_file, 'r') as f:
                config = json.load(f)

            # Create a mapping of column names (including alternatives) to primary core attribute names
            column_mapping = {}
            for core_attr, details in config["expected_columns"].items():
                column_mapping[core_attr] = core_attr  # Primary name maps to itself
                if isinstance(details, dict) and "alternatives" in details:
                    for alternative in details["alternatives"]:
                        column_mapping[alternative] = core_attr  # Map alternatives to the primary name
            
            # Gather all core attributes and their alternatives into a set
        
            core_and_alternative_columns = set(column_mapping.keys())

            # Filter and reorder core attributes based on their presence and order in the uploaded file's columns
            ordered_core_attributes = [column_mapping[col] for col in df_columns if col in column_mapping]
                        
            # Join the core_attributes list into a comma-separated string
            core_columns_string = ", ".join(ordered_core_attributes)
            #write_and_log(f"SQL command columns: {core_columns_string}")
            
            return table_name, ordered_core_attributes, core_columns_string, config, core_and_alternative_columns, column_mapping

    # If no match is found, return a high priority number so it is processed last
    return None, None, None, None, None, 99

def determine_order(file):
    # Define the mapping of base filename content to table names
    
    base_filename = file.name.lower()
    
    for key, (_, _, order) in table_mapping.items():
        if key in base_filename:
            return (file, order)  # Return file and assigned order

def dataframe_for_tree_integrity(df):
    # Define the columns needed for the integrity checks
    columns_to_check = ['site_id', 'wildcard_sub_id','composed_site_id', 'spi_id', 'lpi_id', 'tree_id', 'dbh', 
                        'position', 'life', 'integrity', 'full_scientific', 'inventory_year', 'decay']
    
    # Filter only existing columns in df
    existing_columns = [col for col in columns_to_check if col in df.columns]
    
    # Create a filtered DataFrame for integrity checks
    df_for_integrity_checks = df[existing_columns].copy()

    # Step 1: Sort the data to ensure correct chronological order within groups
    df_integrity_lpi_id = df_for_integrity_checks.sort_values(
        by=[col for col in ['site_id', 'wildcard_sub_id', 'composed_site_id', 'lpi_id', 'tree_id', 'inventory_year'] if col in df_for_integrity_checks.columns]
    )
    df_integrity_spi_id = df_for_integrity_checks.sort_values(
        by=[col for col in ['site_id', 'wildcard_sub_id', 'composed_site_id', 'spi_id', 'tree_id', 'inventory_year'] if col in df_for_integrity_checks.columns]
    )

    # Step 2: Use groupby (without inventory_year) and create previous values for each column
    grouped_lpi_id = df_integrity_lpi_id.groupby(
        [col for col in ['site_id', 'wildcard_sub_id', 'composed_site_id', 'lpi_id', 'tree_id'] if col in df_integrity_lpi_id.columns]
    )
    grouped_spi_id = df_integrity_spi_id.groupby(
        [col for col in ['site_id', 'wildcard_sub_id', 'composed_site_id', 'spi_id', 'tree_id'] if col in df_integrity_spi_id.columns]
    )

    # Create previous counterparts for each column and add them to the DataFrame
    for column in existing_columns:
        if column != 'inventory_year':  # Skip inventory_year for the shift
            df_integrity_lpi_id[f'previous_{column}'] = grouped_lpi_id[column].shift(1)
            df_integrity_spi_id[f'previous_{column}'] = grouped_spi_id[column].shift(1)

    # Handle missing values (optional: fill with NaN, None, or a default value)
    #df_integrity_lpi_id.fillna(value={'previous_' + col: None for col in existing_columns if col != 'inventory_year'}, inplace=True)
    #df_integrity_spi_id.fillna(value={'previous_' + col: None for col in existing_columns if col != 'inventory_year'}, inplace=True)

    # Debugging output
    print("DataFrame with current and previous values:")
    print(df_integrity_lpi_id.head())  # Print the first few rows for inspection
    print(df_integrity_spi_id.head())  # Print the first few rows for inspection

    return df_integrity_lpi_id, df_integrity_spi_id


