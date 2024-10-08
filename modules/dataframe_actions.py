import json
import os
from modules.logs import write_and_log

def prepare_dataframe_for_copy(df, ordered_core_attributes, extra_columns):
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
    
    # Keep only core attributes and extra columns
    df_for_copy = df[ordered_core_attributes].copy()

    
    # Create a new column `extended_attributes` by combining extra columns into a JSON string
    if extra_columns:
        df_for_copy.loc[:, 'extended_attributes'] = df.loc[:, extra_columns].apply(lambda row: json.dumps(row.dropna().to_dict()), axis=1)
        df_for_copy = df_for_copy.copy()

    return df_for_copy

def determine_copy_command(file_path, df_columns, extra_columns, table_name):
    """
    Determines the core attributes and constructs the COPY command including extended attributes.
    
    Args:
        file_path (str): Path to the file being processed.
        df_columns (list): List of columns in the DataFrame.
        extra_columns (list): List of extra columns to be stored as JSONB.
        table_name (str): Name of the table to insert data into.

    Returns:
        copy_command (str): The COPY command for inserting data.
        core_attributes (list): List of core columns for the table.
    """
    # Map the DataFrame columns to core attributes based on expectations
    core_attributes = [col for col in df_columns if col not in extra_columns]
    
    # Join core columns into a comma-separated string
    columns_string = ", ".join(core_attributes + ["extended_attributes"])

    # Create the COPY command to include core columns and JSONB `extended_attributes`
    copy_command = f"""
    COPY public.{table_name} 
    ({columns_string}) 
    FROM STDIN WITH DELIMITER E'\t' CSV HEADER NULL '\\N';"""
    write_and_log(f'copy_command: {copy_command}')
    return copy_command

def determine_configs(file_path, df_columns):
    # Define the mapping of base filename content to table names
    table_mapping = {
        "lying": ("tree_staging", "expectations/expe_lying.json"),
        "standing": ("tree_staging", "expectations/expe_standing.json"),
        "design": ("site_design", "expectations/expe_site_design.json"),
        "plots": ("plot", "expectations/expe_plots.json")
    }
    
    base_filename = os.path.basename(file_path).lower()
    
    # Find the appropriate table name and config file based on the filename
    for key, (table_name, config_file) in table_mapping.items():
        if key in base_filename:
            # Load the config file
            with open(config_file, 'r') as f:
                config = json.load(f)

            # Extract core attributes from config
            core_attributes = list(config["expected_columns"].keys())

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
            
            # Return the copy command, table name, and core attributes
            return table_name, ordered_core_attributes, core_columns_string, config, core_and_alternative_columns

    # If no match is found in the table mapping
    raise ValueError("File name does not match any known configuration")
