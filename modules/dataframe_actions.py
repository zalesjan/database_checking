import json
import os
import pandas as pd
import numpy as np
import streamlit as st
from modules.logs import write_and_log

import pandas as pd
import os

def extract_file_name(file):
    """Handles both Streamlit UploadedFile and local string paths."""
    if hasattr(file, "name"):
        return file.name  # Streamlit upload
    return os.path.basename(file)  # Local file path

def df_from_uploaded_file(uploaded_file, header_line_idx):

    encodings_to_try = ["utf-8", "cp1250", "iso-8859-2", "latin1"]

    def try_read_csv(filepath, sep, skiprows):
        for enc in encodings_to_try:
            try:
                df = pd.read_csv(filepath, sep=sep, skiprows= skiprows, engine="python", encoding=enc)
                print(f"✅ Loaded with encoding: {enc}")
                return df
            except UnicodeDecodeError:
                print(f"❌ Failed to decode {filepath} with encoding: {enc}")
        raise UnicodeDecodeError(f"⚠️ Could not decode {filepath} with any known encoding.")
    
    # If it's a string, treat it as a file path
    if isinstance(uploaded_file, str):
        uploaded_file_path = uploaded_file
        print(uploaded_file)
        print(uploaded_file_path)
        if uploaded_file_path.endswith(".xlsx") or uploaded_file_path.endswith(".xls"):
            print(".xlsx")
            df = pd.read_excel(uploaded_file_path)
        elif uploaded_file_path.endswith(".csv"):
            print(".csv")
            df = try_read_csv(uploaded_file_path, sep=None, skiprows = header_line_idx)
        elif uploaded_file_path.endswith(".txt"):
            print(".txt")
            df = try_read_csv(uploaded_file_path, sep="\t", skiprows = header_line_idx)
        else:
            raise ValueError(f"Unsupported file type: {uploaded_file_path}")
        return df, uploaded_file_path

    # Else assume it's a Streamlit UploadedFile object
    temp_dir = "temp_dir"
    os.makedirs(temp_dir, exist_ok=True)
    uploaded_file_path = os.path.join(temp_dir, uploaded_file.name)

    with open(uploaded_file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    if uploaded_file.name.endswith(".xlsx") or uploaded_file.name.endswith(".xls"):
        df = pd.read_excel(uploaded_file)
    elif uploaded_file.name.endswith(".csv"):
        df = try_read_csv(uploaded_file, sep=None, skiprows = header_line_idx)
    elif uploaded_file.name.endswith(".txt"):
        df = try_read_csv(uploaded_file, sep="\t", skiprows = header_line_idx)
    else:
        raise ValueError(f"Unsupported file type: {uploaded_file.name}")
    return df, uploaded_file_path


def df_from_detected_file(file_path):
    # Ensure the temporary directory exists
    temp_dir = "temp_dir"
    os.makedirs(temp_dir, exist_ok=True)

    # Load the file into a DataFrame
    df = pd.read_csv(file_path, delimiter='\t')
    return df

def etl_process_df(role, file_name, uploaded_file_path, df_columns, df):

    # 🔧 Use file_name for table naming, logging, etc.
    table_name = os.path.splitext(file_name)[0].lower().replace(" ", "_")

    st.header(f"{file_name}")
    st.write(df)

    #GET CONFIGS AND COLUMNS based on file name and extra columns that are not part of the ordered_core_attributes, st.write core and extra ones
    table_name, ordered_core_attributes, core_columns_string, config, core_and_alternative_columns, column_mapping, table_mapping = determine_configs(file_name, df_columns)
   
    if role == "role_superuser_DB_VUK-raw_data": 
        ignored_columns = None
        extra_columns = None

        expected_cols = core_and_alternative_columns
        header_line_idx = find_header_line(uploaded_file_path, expected_cols, sep="\t", encoding="utf-8")

    if role != "role_superuser_DB_VUK-raw_data":
        extra_columns = find_extra_columns(df_columns, core_and_alternative_columns, ordered_core_attributes)
        ignored_columns = find_ignored_columns(file_name, df, extra_columns)
        header_line_idx = None
        
    return table_name, ordered_core_attributes, extra_columns, ignored_columns, config, column_mapping, table_mapping, header_line_idx

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
    df_columns_lower = {str(col).lower(): col for col in df_columns}  # Mapping original to lowercase

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

def find_header_line(filepath, expected_columns, sep="\t", encoding="utf-8"):
    #print(f"🔍 Expected columns: {expected_columns}")
    threshold = 6
    print(f"🔎 Using threshold: {threshold}")

    with open(filepath, encoding=encoding, errors="ignore") as f:
        for i, line in enumerate(f):
            if not line.strip():
                continue

            fields = [col.strip().lower() for col in line.strip().split(sep)]
            match_count = sum(1 for col in expected_columns if col.lower() in fields)

            print(f"Line {i}: match_count={match_count}")

            if match_count >= threshold:
                print(f"✅ Header found at line {i}: {fields}")
                return i

    raise ValueError("❌ Could not detect header line dynamically.")

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
 
     # Step 1: Create a copy of the DataFrame to avoid slice issues
     df_for_copy = df.copy()
     df_for_copy.columns = df_for_copy.columns.str.lower() #columns names converted to lowercase
 
     # Step 2: Rename columns to their primary names using `column_mapping`
     df_for_copy = df_for_copy.rename(columns=column_mapping)
 
     #Step 3: Remove columns to ignore from both core and extra columns
     if ignored_columns:
         ordered_core_attributes = [col for col in ordered_core_attributes if col not in ignored_columns]
         extra_columns = [col for col in extra_columns if col not in ignored_columns]
 
     # Step 4: Keep only the necessary columns (both core and extra)
     print(ordered_core_attributes)
     df_for_copy = df_for_copy[ordered_core_attributes].copy()
     st.write("Data Preview:", df_for_copy.head())
 
     # Create a new column `extended_attributes` by combining extra columns into a JSON string
     if extra_columns:
         df.columns = df.columns.str.lower() #columns names converted to lowercase
         df_for_copy.loc[:, 'extended_attributes'] = df.loc[:, extra_columns].apply(lambda row: json.dumps(row.dropna().to_dict()), axis=1)
 
     df_for_copy = df_for_copy.copy()
 
     return df_for_copy

def prepare_biodiversity_dataframe_for_copy(df, ordered_core_attributes, extra_columns, column_mapping, table_name, ignored_columns=None):
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
    print(df_for_copy)
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
            if not available_cols:
                continue

            # 🧠 Special case for authors: split on semicolons
            if json_field == "authors":
                def parse_authors(row):
                    authors = []
                    for col in available_cols:
                        val = row.get(col)
                        if pd.notna(val):
                            authors.extend([name.strip() for name in str(val).split(';') if name.strip()])
                    return json.dumps(authors)

                df_for_copy[json_field] = df_for_copy[available_cols].apply(parse_authors, axis=1)

            # 🧠 Special case for plots_list
            elif json_field == "plots_list":
                def parse_plots_list(row):
                    items = []
                    for col in available_cols:
                        val = row.get(col)
                        if pd.notna(val):
                            items.extend([item.strip() for item in str(val).split(';') if item.strip()])
                    return json.dumps(items)

                df_for_copy[json_field] = df_for_copy[available_cols].apply(parse_plots_list, axis=1)

            # ✅ Generic case
            else:
                df_for_copy[json_field] = df_for_copy[available_cols].apply(
                    lambda row: json.dumps({k: v for k, v in row.dropna().to_dict().items()}),
                    axis=1
                )

            # 🔁 Drop source columns (since JSON was created)
            if json_field not in ["plots_list", "authors"]:
                df_for_copy.drop(columns=available_cols, inplace=True)
            used_in_json_fields.extend(available_cols)

    # Step 5: Special handling for parentheses in full_scientific
    # 🧠 Special handling if "full_scientific" column has parentheses
    if "full_scientific" in df_for_copy.columns:
        def extract_brackets(val):
            if pd.notna(val) and '(' in val and ')' in val:
                extra = val[val.find('(')+1 : val.find(')')].strip()
                main_name = val[:val.find('(')].strip()
                return main_name, extra
            elif pd.notna(val):
                return val.strip(), None
            else:
                return None, None

        full_scientific_cleaned = []
        taxonomy_extras = []

        for _, row in df_for_copy.iterrows():
            main_name, extra = extract_brackets(row["full_scientific"])
            full_scientific_cleaned.append(main_name)
            taxonomy_extras.append(extra)

        df_for_copy["full_scientific"] = full_scientific_cleaned

        # Merge taxonomy_extras into existing or new additional_taxonomy
        # does not work well, it creates an extra column additional_taxonomy
        
        if "additional_taxonomy" in df_for_copy.columns:
            df_for_copy["additional_taxonomy"] = [
                json.dumps({**json.loads(e), "taxonomy_extras": [extra]} if extra else json.loads(e))
                for e, extra in zip(df_for_copy["additional_taxonomy"], taxonomy_extras)
            ]
        else:
            df_for_copy["additional_taxonomy"] = [
                json.dumps({"taxonomy_extras": [extra]} if extra else {})
                for extra in taxonomy_extras
            ]

                # Step 5: Create extended_attributes (excluding those used in JSON fields)
    final_extra_columns = [col for col in extra_columns if col not in used_in_json_fields]
   
    if final_extra_columns:
        print(df.columns)
        df_for_copy['extended_attributes'] = df[final_extra_columns].apply(
            lambda row: json.dumps(row.dropna().to_dict()), axis=1
        )
    
    st.write("✅ Final DataFrame preview before COPY:")
    st.dataframe(df_for_copy.head())
    # Step 6: Select final output columns
    all_required_columns = ordered_core_attributes.copy()
    print(all_required_columns)

    if json_column_mapping:
        all_required_columns += list(json_column_mapping.keys())
    if "additional_taxonomy" in df_for_copy.columns and "additional_taxonomy" not in all_required_columns:
        all_required_columns.append("additional_taxonomy")

    if final_extra_columns:
        all_required_columns.append("extended_attributes")
    print(all_required_columns)
    print(df_for_copy)

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

def determine_copy_command_with_ignore(file_path, df_columns, extra_columns, table_name, schema, ignored_columns=None):
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
     COPY {schema}.{table_name} 
     ({columns_string}) 
     FROM STDIN WITH DELIMITER E'\t' CSV HEADER NULL '\\N';"""
    write_and_log(f'copy_command: {copy_command}')
    return copy_command
 
def biodiversity_determine_copy_command_with_ignore(file_path, ordered_core_attributes, extra_columns, table_name, df_columns, schema, ignored_columns=None):
    
    json_column_mapping = {}

    # Find the matching key in table_mapping where the value[0] == table_name
    for key, value in table_mapping.items():
        if value[0] == schema or value[0] == table_name:
            json_column_mapping = value[4] if len(value) > 4 and isinstance(value[4], dict) else {}
            break

    if ignored_columns is None:
        ignored_columns = []


    # Flatten all JSON-related columns into a set of columns to exclude
    json_mapped_columns = set()
    valid_json_fields = []

    # Normalize DataFrame columns to lowercase for consistent matching
    df_columns_lower = {str(col).lower(): col for col in df_columns}

    # Filter out ignored columns
    filtered_columns = [col for col in ordered_core_attributes if col not in ignored_columns]

    # Map the filtered DataFrame columns to core attributes
    core_attributes = [col for col in filtered_columns if col not in extra_columns]
    
    if json_column_mapping:
        # Add JSONB fields to be included in the COPY command
        # Only include JSON fields if at least one of their source columns is in the DataFrame
        for json_field, source_cols in json_column_mapping.items():
            #print(f"ℹ️from copy command: json_column_mapping {json_column_mapping}")
            #print(f"ℹ️from copy command: json_field {json_field}")
            #print(f"ℹ️from copy command: df_columns_lower {df_columns_lower}")
            #print(f"ℹ️from copy command: source_cols {source_cols}")

            if any(col.lower() in df_columns_lower for col in source_cols):
                #print(source_cols)
                valid_json_fields.append(json_field)
                #print(f"ℹ️from copy command: valid_json_fields {valid_json_fields}")

                json_mapped_columns.update([col.lower() for col in source_cols])
                #print(f"ℹ️from copy command: json_mapped_columns {json_mapped_columns}")
                print(f"ℹ️from copy command: valid_json_fields {valid_json_fields}")
        
        if schema == "site_design":
            columns_string = ", ".join(f'"{col}"' for col in (core_attributes))
        else:
            columns_string = ", ".join(f'"{col}"' for col in (core_attributes + valid_json_fields))

        #columns_string = ", ".join(core_attributes + valid_json_fields)
        #print(f"ℹ️from copy command: core_attributes {core_attributes}")
        #print(f"ℹ️from copy command: columns_string {columns_string}")

    # 2. Exclude those from extra_columns
    extra_columns = [col for col in extra_columns if col not in json_mapped_columns and col not in ignored_columns]

    if extra_columns:
        # Join core columns into a comma-separated string
        columns_string = ", ".join(f'"{col}"' for col in (core_attributes + valid_json_fields + ["extended_attributes"]))
    else:
        if schema != "site_design":
            columns_string = ", ".join(f'"{col}"' for col in (core_attributes + valid_json_fields))

    #print(f"ℹ️from copy command: columns_string {columns_string}")

    # Create the COPY command to include core columns and JSONB `extended_attributes`
    copy_command = f"""
    COPY {schema}.{table_name} 
    ({columns_string}) 
    FROM STDIN WITH DELIMITER E'\t' CSV HEADER NULL '\\N';"""
    #print(f"ℹ️from copy command: columns_string {columns_string}")

    write_and_log(f'copy_command: {copy_command}')
    return copy_command

"""
def raw_data_determine_copy_command_with_ignore(file_path, df_columns, extra_columns, table_name, ignored_columns=None):
    
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
    print(f"ℹ️from copy command: json_column_mapping {json_column_mapping}")
    print(f"ℹ️from copy command: core_attributes {core_attributes}")
    print(f"ℹ️from copy command: extra_columns {extra_columns}")
    print(f"ℹ️from copy command: json_fields {json_fields}")
    print(f"ℹ️from copy command: json_mapped_columns {json_mapped_columns}")


    # 2. Exclude those from extra_columns
    extra_columns = [col for col in extra_columns if col not in json_mapped_columns and col not in ignored_columns]

    print(f"ℹ️from copy command: extra_columns {extra_columns}")

    if extra_columns:
        # Join core columns into a comma-separated string
        columns_string = ", ".join(core_attributes + json_fields + ["extended_attributes"])
    else:
        columns_string = ", ".join(core_attributes + json_fields)

    # Create the COPY command to include core columns and JSONB `extended_attributes`
    copy_command = f"""
"""
    COPY public.{table_name} 
    ({columns_string}) 
    FROM STDIN WITH DELIMITER E'\t' CSV HEADER NULL '\\N';"""
"""    
    write_and_log(f'copy_command: {copy_command}')
    return copy_command
"""

common_biodiversity_config = (
    "biodiversity", 
    "expectations/expe_biodiversity.json", 
    8, 
    [], 
    {
        "authors": ["author1", "author2", "authors"],
        "additional_taxonomy": ["order", "class", "phylum", "taxonomy_value", "tax_note", "taxon_author"],
        "group_specific_tree": [
            "tree", "stem", "part", "prp",
            "group_specific_tree_species_count", "group_specific_tree_total_cover", "group_specific_tree_note_tree", "group_specific_tree_janek_id",
            "total_cover", "species_count", "note",  "group_specific_tree_decay", 
            "group_specific_tree_soil_contact", "group_specific_tree_moss_coverage", "group_specific_tree_bark_coverage"
        ],
        "group_specific_species": ["group_specific_species_loc_in_plot", "group_specific_species_trap_type"],
        "group_specific_prp": []
    }
)

table_mapping = {
        "sites": ("sites", "expectations/expe_sites.json", 1, None, None),
        "design": ("site_design", "expectations/expe_site_design.json", 2, ["composed_site_id"], {"plots_list": ["plots_list"]}),
        "plots": ("plots", "expectations/expe_plots.json", 3, ["composed_site_id", "inventory_year", "inventory_id", "circle_radius"], None),
        "standing": ("tree_staging", "expectations/expe_standing.json", 4, ["composed_site_id", "inventory_year", "inventory_id", "lpi_id", "spi_id", "circle_no", "circle_radius"], None),
        "lying": ("tree_staging", "expectations/expe_lying.json", 5, ["composed_site_id", "inventory_year", "inventory_id", "lpi_id", "spi_id", "circle_no", "circle_radius"], None),
        "cwd": ("cwd", "expectations/expe_cwd.json", 6, [], None),
        "metadata": ("metadata", "expectations/expe_metadata.json", 7, [], None),
        # Multiple aliases for the same biodiversity configuration
        "biodiversity": common_biodiversity_config,
        "bryo": common_biodiversity_config,
        "malaco": common_biodiversity_config,
        "fyto": common_biodiversity_config,
        "ento": common_biodiversity_config,
        "lich": common_biodiversity_config,
        "myko": common_biodiversity_config,
}

def determine_configs(file_name, df_columns):
   
    # Normalize the column names from the file to lowercase
    df_columns_lower = {str(col).lower(): col for col in df_columns}  # Mapping original to lowercase
    
    # Find the appropriate table name and config file based on the filename
    for table_acronym, (table_name, config_file, _, _, _) in table_mapping.items():
        
        if table_acronym in file_name:

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
            #print(ordered_core_attributes)

            return table_name, ordered_core_attributes, core_columns_string, config, core_and_alternative_columns, column_mapping, table_mapping

    # If no match is found, return a high priority number so it is processed last
    return None, None, None, None, None, None, 99

def determine_order(file):
    # Define the mapping of base filename content to table names
    
    base_filename = file.lower()
    
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



