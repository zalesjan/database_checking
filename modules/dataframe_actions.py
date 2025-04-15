import json
import os
import pandas as pd
import numpy as np
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

    #GET CONFIGS AND COLUMNS based on file name and extra columns that are not part of the ordered_core_attributes, st.write core and extra ones
    table_name, ordered_core_attributes, core_columns_string, config, core_and_alternative_columns, column_mapping, table_mapping = determine_configs(uploaded_file_name, df_columns)
    
    extra_columns = find_extra_columns(df_columns, core_and_alternative_columns, ordered_core_attributes)
    ignored_columns = find_ignored_columns(uploaded_file_name, df, extra_columns)
        
    return table_name, ordered_core_attributes, extra_columns, ignored_columns, config, column_mapping, table_mapping

def find_ignored_columns(uploaded_file_name, df, extra_columns):
    
    unique_key_prefix = f"{uploaded_file_name}_"

    df_ignore = df.copy()  # Create a copy to avoid modifying df
    df_ignore.columns = df_ignore.columns.str.lower() #columns names converted to lowercase
    df_ignore.replace("\\N", np.nan, inplace=True)  # Replace \N only in the copy

    # Identify ignored columns automatically
    ignored_columns = [col for col in extra_columns if df_ignore[col].isna().all()]  # All values are NaN

    st.warning(f"These {len(ignored_columns)} ignored_columns were found")
    st.write(f"{ignored_columns}")
    # Display the ignored columns (for confirmation)
    if ignored_columns:
        st.write("You chose to ignore these columns:", ignored_columns, key=f"{unique_key_prefix}ignore_columns")

    return ignored_columns

def find_extra_columns(df_columns, core_and_alternative_columns, ordered_core_attributes):
    # Normalize the column names from the file to lowercase
    df_columns_lower = {col.lower(): col for col in df_columns}  # Mapping original to lowercase

    # Extract expected column names (main attributes, not alternatives)
    extra_columns = [col for col in df_columns_lower if col not in core_and_alternative_columns]
    st.warning(f"These {len(ordered_core_attributes)} columns for basic (mandatory) attributes were found:")
    st.write(f"{ordered_core_attributes}")
    if extra_columns:
        st.warning(f"These {len(extra_columns)} extended attributes columns were found")
        st.write(f"{extra_columns}")
    else:
        st.write("No extra columns found.")
    return extra_columns if extra_columns else []

def prepare_dataframe_for_copy(df, ordered_core_attributes, extra_columns, column_mapping, table_name, ignored_columns=None):
    """
    Prepares the DataFrame to merge core data with a JSONB column `extended_attributes` for COPY command.
    
    Args:
        df (pd.DataFrame): Original DataFrame with core and extra columns.
        core_attributes (list): List of core attributes to retain.
        extra_columns (list): List of extra columns to combine into JSONB.

    Returns:
        pd.DataFrame: Modified DataFrame ready for COPY command.
    """
    json_column_mapping = {}

    # Step 1: Lowercase and rename columns using column_mapping
    df_for_copy = df.copy()
    df_for_copy.columns = df_for_copy.columns.str.lower()
    df_for_copy.rename(columns=column_mapping, inplace=True)

    # Step 2: Remove ignored columns from core and extra
    if ignored_columns:
        ordered_core_attributes = [col for col in ordered_core_attributes if col not in ignored_columns]
        extra_columns = [col for col in extra_columns if col not in ignored_columns]

    # Step 3: Determine JSON field mapping
    # Find the matching key in table_mapping where the value[0] == table_name
    for key, value in table_mapping.items():
        if value[0] == table_name:
            json_column_mapping = value[4] if len(value) > 4 and isinstance(value[4], dict) else {}
            break

    # Step 4: Build and remove dedicated JSON fields
    used_in_json_fields = []
    if json_column_mapping:
        for json_field, source_columns in json_column_mapping.items():
            available_cols = [col for col in source_columns if col in df_for_copy.columns]
            if available_cols:
                df_for_copy[json_field] = df_for_copy[available_cols].apply(
                    lambda row: json.dumps({k: v for k, v in row.dropna().to_dict().items()}),
                    axis=1
                )
                df_for_copy.drop(columns=available_cols, inplace=True)
                used_in_json_fields.extend(available_cols)

    # Step 5: Create extended_attributes (excluding those used in JSON fields)
    final_extra_columns = [col for col in extra_columns if col not in used_in_json_fields]
    if final_extra_columns:
        df_for_copy['extended_attributes'] = df[final_extra_columns].apply(
            lambda row: json.dumps(row.dropna().to_dict()), axis=1
        )

    # Step 6: Select final output columns
    all_required_columns = ordered_core_attributes.copy()
    if json_column_mapping:
        all_required_columns += list(json_column_mapping.keys())
    if final_extra_columns:
        all_required_columns.append("extended_attributes")

    df_for_copy = df_for_copy[[col for col in all_required_columns if col in df_for_copy.columns]].copy()

    st.write("✅ Final DataFrame preview before COPY:")
    st.dataframe(df_for_copy.head())

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
    json_column_mapping = {}

    # Find the matching key in table_mapping where the value[0] == table_name
    for key, value in table_mapping.items():
        if value[0] == table_name:
            json_column_mapping = value[4] if len(value) > 4 and isinstance(value[4], dict) else {}
            break

    if ignored_columns is None:
        ignored_columns = []

    json_fields = []

    # Flatten all JSON-related columns into a set of columns to exclude
    json_mapped_columns = set()
    if json_column_mapping:
        for columns in json_column_mapping.values():
            json_mapped_columns.update(columns)  # Adds all columns from each json field
    
    # Filter out ignored columns
    filtered_columns = [col for col in df_columns if col not in ignored_columns]

    # Map the filtered DataFrame columns to core attributes
    core_attributes = [col for col in filtered_columns if col not in extra_columns]
    
    if json_column_mapping:
        # Add JSONB fields to be included in the COPY command
        json_fields = list(json_column_mapping.keys())
        columns_string = ", ".join(core_attributes + json_fields)
    print(core_attributes)
    print(extra_columns)
    print(json_fields)
    print(json_mapped_columns)


    # 2. Exclude those from extra_columns
    extra_columns = [col for col in extra_columns if col not in json_mapped_columns and col not in ignored_columns]

    print(extra_columns)

    if extra_columns:
        # Join core columns into a comma-separated string
        columns_string = ", ".join(core_attributes + json_fields + ["extended_attributes"])
    else:
        columns_string = ", ".join(core_attributes + json_fields)

    # Create the COPY command to include core columns and JSONB `extended_attributes`
    copy_command = f"""
    COPY public.{table_name} 
    ({columns_string}) 
    FROM STDIN WITH DELIMITER E'\t' CSV HEADER NULL '\\N';"""
    
    write_and_log(f'copy_command: {copy_command}')
    return copy_command

table_mapping = {
        "sites": ("sites", "expectations/expe_sites.json", 1, None, None),
        "design": ("site_design", "expectations/expe_site_design.json", 2, ["composed_site_id"], {"plots_list": ["plots_list"]}),
        "plots": ("plots", "expectations/expe_plots.json", 3, ["composed_site_id", "inventory_year", "inventory_id", "circle_radius"], None),
        "standing": ("tree_staging", "expectations/expe_standing.json", 4, ["composed_site_id", "inventory_year", "inventory_id", "lpi_id", "spi_id", "circle_no", "circle_radius"], None),
        "lying": ("tree_staging", "expectations/expe_lying.json", 5, ["composed_site_id", "inventory_year", "inventory_id", "lpi_id", "spi_id", "circle_no", "circle_radius"], None),
        "cwd": ("cwd", "expectations/expe_cwd.json", 6, [], None),
        "metadata": ("metadata", "expectations/expe_metadata.json", 7, [], None),
        "biodiversity": (
        "biodiversity", 
        "expectations/expe_biodiversity.json", 
        8, 
        [], 
        {
            "authors": ["author1", "author2"],
            "addional_taxonomy": ["order", "class", "phylum", "taxonomy_value"],
            "group_specific_tree": ["tree", "stem", "part", "prp", "group_specific_tree_species_count", "group_specific_tree_total_cover", "total_cover", "species_count", "note"]
        }
    )
}

def determine_configs(file_path, df_columns):
    
    # Normalize the column names from the file to lowercase
    df_columns_lower = {col.lower(): col for col in df_columns}  # Mapping original to lowercase
    
    # Define the mapping of base filename content to table names
    base_filename = os.path.basename(file_path).lower()
    
    # Find the appropriate table name and config file based on the filename
    for table_acronym, (table_name, config_file, _, _, _) in table_mapping.items():
        if table_acronym in base_filename:
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
            ordered_core_attributes = [column_mapping[col] for col in df_columns_lower if col in column_mapping]
                        
            # Join the core_attributes list into a comma-separated string
            core_columns_string = ", ".join(ordered_core_attributes)
            #write_and_log(f"SQL command columns: {core_columns_string}")
            
            return table_name, ordered_core_attributes, core_columns_string, config, core_and_alternative_columns, column_mapping, table_mapping

    # If no match is found, return a high priority number so it is processed last
    return None, None, None, None, None, 99

def determine_order(file):
    # Define the mapping of base filename content to table names
    
    base_filename = file.name.lower()
    
    for key, (_, _, order, _, _) in table_mapping.items():
        if key in base_filename:
            return (file, order)  # Return file and assigned order

import pandas as pd

def dataframe_for_tree_integrity(df):
    """
    Process the DataFrame for tree integrity checks, ensuring that empty filtered DataFrames 
    are returned only when applicable, while the other continues processing.
    """

    # Filter to only include rows where 'consistent_id' is "Y"
    df = df[df['consistent_id'] == "Y"].copy()
    
    # Define the columns needed for the integrity checks
    columns_to_check = ['site_id', 'wildcard_sub_id', 'composed_site_id', 'spi_id', 'lpi_id', 'tree_id', 'dbh', 
                        'position', 'life', 'integrity', 'full_scientific', 'inventory_year', 'decay', 'consistent_id']
    
    # Filter only existing columns in df
    existing_columns = [col for col in columns_to_check if col in df.columns]
    df_for_integrity_checks = df[existing_columns].copy()

    # Step 1: Sort the data to ensure correct chronological order within groups
    df_integrity_lpi_id = df_for_integrity_checks.sort_values(
        by=[col for col in ['site_id', 'wildcard_sub_id', 'composed_site_id', 'lpi_id', 'tree_id', 'inventory_year'] if col in df_for_integrity_checks.columns]
    )
    df_integrity_spi_id = df_for_integrity_checks.sort_values(
        by=[col for col in ['site_id', 'wildcard_sub_id', 'composed_site_id', 'spi_id', 'tree_id', 'inventory_year'] if col in df_for_integrity_checks.columns]
    )
    
    # Step 2: Remove rows where lpi_id or spi_id is missing or invalid
    df_filtered_lpi_id = df_integrity_lpi_id[~df_integrity_lpi_id['lpi_id'].isin(['\\N', None, ''])]
    df_filtered_spi_id = df_integrity_spi_id[~df_integrity_spi_id['spi_id'].isin(['\\N', None, ''])]

    # Step 3: If filtering results in empty DataFrames, return only the affected DataFrame as empty
    if df_filtered_lpi_id.empty:
        print("⚠️ Warning: No valid rows left after filtering lpi_id. Returning empty DataFrame.")
        df_filtered_lpi_id = pd.DataFrame(columns=existing_columns)  # Ensure the structure is maintained
    
    if df_filtered_spi_id.empty:
        print("⚠️ Warning: No valid rows left after filtering spi_id. Returning empty DataFrame.")
        df_filtered_spi_id = pd.DataFrame(columns=existing_columns)  # Ensure the structure is maintained

    # Step 4: Use groupby (without inventory_year) and create previous values for each column
    if not df_filtered_lpi_id.empty:
        grouped_lpi_id = df_filtered_lpi_id.groupby(
            [col for col in ['site_id', 'wildcard_sub_id', 'composed_site_id', 'lpi_id', 'tree_id'] if col in df_filtered_lpi_id.columns]
        )
        for column in existing_columns:
            if column != 'inventory_year':  # Skip inventory_year for the shift
                df_filtered_lpi_id[f'previous_{column}'] = grouped_lpi_id[column].shift(1)

    if not df_filtered_spi_id.empty:
        grouped_spi_id = df_filtered_spi_id.groupby(
            [col for col in ['site_id', 'wildcard_sub_id', 'composed_site_id', 'spi_id', 'tree_id'] if col in df_filtered_spi_id.columns]
        )
        for column in existing_columns:
            if column != 'inventory_year':  # Skip inventory_year for the shift
                df_filtered_spi_id[f'previous_{column}'] = grouped_spi_id[column].shift(1)

    # Debugging output
    print("✅ DataFrame with current and previous values:")
    print(df_filtered_lpi_id.head())  
    print(df_filtered_spi_id.head())  

    return df_filtered_lpi_id, df_filtered_spi_id


    # Handle missing values (optional: fill with NaN, None, or a default value)
    #df_integrity_lpi_id.fillna(value={'previous_' + col: None for col in existing_columns if col != 'inventory_year'}, inplace=True)
    #df_integrity_spi_id.fillna(value={'previous_' + col: None for col in existing_columns if col != 'inventory_year'}, inplace=True)



