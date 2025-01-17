import yagmail
import json
import pandas as pd
import sys
import re
from datetime import datetime
import os
import logging
from modules.logs import write_and_log
from modules.database_utils import do_query

import streamlit as st

# Email configuration
EMAIL_USER = st.secrets['email']['EMAIL_USER']
EMAIL_PASSWORD = st.secrets['email']['EMAIL_PASSWORD']

def validate_file(df, config, file_name):
    write_and_log(f'validating file: {file_name}')
    expected_columns = config.get("expected_columns", {})
    validation_results = {}
    columns_for_exploration = set()
    existence_checks = {}

    # Convert all column names to lowercase
    df.columns = df.columns.str.lower()

    # Validate each column
    for standard_name, expectations in expected_columns.items():
        # Identify the actual column name from the alternatives
        actual_column_name = None
        for alt_name in expectations.get("alternatives", []):
            if alt_name in df.columns:
                actual_column_name = alt_name
                break

        # If no actual column name was found, record the absence
        if actual_column_name is None:
            validation_results[f"{standard_name}"] = "WARNING: column missing! Sure you don't have it? It's mandatory."
            existence_checks[f"{standard_name}"] = False
            continue
        else:
            existence_checks[f"{standard_name}"] = True

        # Implement specific validations
        if expectations.get("non_null", False):
            non_null_check = df[actual_column_name].notnull().all()
            validation_results[f"{standard_name} (non-null)"] = bool(non_null_check)
            if not non_null_check:
                columns_for_exploration.add(actual_column_name)
        
        if expectations.get("is_numeric", False):
            numeric_check = pd.to_numeric(df[actual_column_name], errors='coerce').notnull().all()
            validation_results[f"{standard_name} (numeric)"] = bool(numeric_check)
            if not numeric_check:
                columns_for_exploration.add(actual_column_name)

        if "range" in expectations:
            min_val, max_val = expectations["range"]
            if max_val == "current_year":
                max_val = datetime.now().year
            numeric_series = pd.to_numeric(df[actual_column_name], errors='coerce')
            range_check = numeric_series.between(min_val, max_val).all()
            validation_results[f"{standard_name} (range {min_val}-{max_val})"] = bool(range_check)
            columns_for_exploration.add(actual_column_name)

        if expectations.get("is_boolean", False):
            boolean_check = df[actual_column_name].isin([0, 1, True, False]).all()
            validation_results[f"{standard_name} (boolean)"] = bool(boolean_check)
            if not boolean_check:
                columns_for_exploration.add(actual_column_name)

        if "specific_characters" in expectations:
            pattern = expectations["specific_characters"]
            pattern_check = df[actual_column_name].apply(lambda x: bool(re.match(pattern, str(x)))).all()
            validation_results[f"{standard_name} (pattern {pattern})"] = bool(pattern_check)
            if not pattern_check:
                columns_for_exploration.add(actual_column_name)

        if "external_file" in expectations:
            external_file_path = expectations["external_file"]
            with open(external_file_path, 'r') as ext_file:
                allowed_values = [line.strip() for line in ext_file.readlines()]
            external_check = df[actual_column_name].isin(allowed_values).all()
            validation_results[f"{standard_name} (external file check)"] = bool(external_check)
            if not external_check:
                columns_for_exploration.add(actual_column_name)

        if "allowed_values" in expectations:
            allowed_values = expectations["allowed_values"]
            allowed_values_check = df[actual_column_name].isin(allowed_values).all()
            validation_results[f"{standard_name} (allowed values {allowed_values})"] = bool(allowed_values_check)
            if not allowed_values:
                columns_for_exploration.add(actual_column_name)

    # Check if validation_results is a dictionary
    if isinstance(validation_results, dict):
        write_and_log("Validation Results:")
        write_and_log(validation_results)  # display the dictionary as JSON
    else:
        write_and_log("Validation results are not in a valid format.")

    return validation_results, list(columns_for_exploration)


def distinct_values_with_counts(df, columns_for_exploration):
    distinct_values = {}
    unique_value_counts = {}
    for column in columns_for_exploration:
        # Get distinct values and sort them
        distinct_values[column] = sorted(df[column].dropna().unique().tolist())
        # Get the number of unique values
        unique_value_counts[column] = df[column].nunique(dropna=True)
    
    write_and_log("Distinct Values in Each Column:")
    for column, values in distinct_values.items():
        count = unique_value_counts[column]  # Retrieve the unique count for the current column
        write_and_log(f"**{column}**: {values}, that is {count} unique values")


def value_counts_for_each_distinct_value(df, columns_for_exploration):
    distinct_values_with_counts = {}
    for column in columns_for_exploration:
        value_counts = df[column].value_counts(dropna=True)

        # Sort numeric values in ascending order
        if pd.api.types.is_numeric_dtype(df[column]):
            sorted_values = value_counts.sort_index(ascending=True)
        # Sort string values alphabetically
        elif pd.api.types.is_string_dtype(df[column]):
            sorted_values = value_counts.sort_index(key=lambda x: x.str.lower())
        else:
            sorted_values = value_counts

        distinct_values_with_counts[column] = sorted_values
    
    st.write("Distinct Values and Counts in Each Column:")
    for column, values in distinct_values_with_counts.items():
        write_and_log(f"**{column}**:")
        for value, count in values.items():
            write_and_log(f" - {value}: {count}")

def plausibility_test(df):
    print("Before filtering or grouping, columns are:", df.head())
    dbh_reduction = None
    position_reversal = None
    decay_inconsistency = None
    integrity_reversal = None
    life_status_reversal = None

    # Initialize results dictionary for storing integrity issues
    integrity_issues = {
        'dbh_reduction': None,
        'position_reversal': None,
        'life_status_reversal': None,
        'integrity_reversal': None,
        'decay_inconsistency': None
    }
    #check_dbh_reduction(df): reduction by more than 2.5 cm or 10%
    integrity_issues = {}
    if 'dbh' in df.columns and 'previous_dbh' in df.columns:
        life_filter = df['life'] == "A"  
        dbh_criteria = (df['dbh'] < df['previous_dbh'] - 25) | (df['dbh'] < df['previous_dbh'] * 0.9)
        dbh_reduction = df[life_filter & dbh_criteria & df['previous_dbh'].notna()][['site_id', 'composed_site_id', 'spi_id', 'tree_id', 'inventory_year']]
        #integrity_issues['dbh_reduction'] = df[life_filter & dbh_criteria & df['previous_dbh'].notna()][['site_id', 'wildcard_id', 'spi_id', 'tree_id', 'inventory_year']]

        #check dbh for smaller than threshold
        dbh_smaller_than_threshold = """
        SELECT t.composed_site_id, t.record_id, t.dbh, d.standing_alive_threshold 
        FROM public.tree t
        JOIN public.plots p
            ON unique_plot_id = p.record_id
        JOIN site_design d
            ON unique_site_design_id = d.record_id
        where t.dbh < d.standing_alive_threshold
        """
        dbh_smaller_than_threshold = do_query(dbh_smaller_than_threshold)
        if dbh_smaller_than_threshold is not None:
            st.dataframe(dbh_smaller_than_threshold)  # Display the result as a DataFrame
        else:
            st.write("No results to display or an error occurred.")

    #def check_position_change(df): Position changes from L to S
    position_criteria = (df['previous_position'] == 'L') & (df['position'] == 'S')
    position_reversal = df[position_criteria][['site_id', 'composed_site_id', 'spi_id', 'tree_id', 'inventory_year']]
    #integrity_issues['position_reversal'] = df[position_criteria][['site_id', 'wildcard_id', 'spi_id', 'tree_id', 'inventory_year']]

    #def check_life_status_change(df): Life status changes from D to A
    life_criteria = (df['previous_life'] == 'D') & (df['life'] == 'A')
    life_status_reversal = df[life_criteria][['site_id', 'composed_site_id', 'spi_id', 'tree_id', 'inventory_year']]
    #integrity_issues['life_status_reversal'] = df[life_criteria][['site_id', 'wildcard_id', 'spi_id', 'tree_id', 'inventory_year']]

    #def check_integrity_change(df): Integrity changes from F to C
    death_filter = df['life'] == "D" 
    integrity_criteria = (df['previous_integrity'] == 'F') & (df['integrity'] == 'C')
    integrity_reversal = df[integrity_criteria & death_filter & df['previous_integrity'].notna()][['site_id', 'composed_site_id', 'spi_id', 'tree_id', 'inventory_year']]
    #integrity_issues['integrity_reversal'] = df[integrity_criteria & death_filter & df['previous_integrity'].notna()][['site_id', 'wildcard_id', 'spi_id', 'tree_id', 'inventory_year']]

    #def check_decay(df,): decay values either increase or stay the same, from 0 (no decay) to 5 (complete decay)
    decay_criteria = df['decay'] < df['previous_decay']
    decay_inconsistency = df[decay_criteria & df['previous_decay'].notna()][['site_id', 'composed_site_id', 'spi_id', 'tree_id', 'inventory_year']]
    #integrity_issues['decay_inconsistency'] = df[decay_criteria & df['previous_decay'].notna()][['site_id', 'wildcard_id', 'spi_id', 'tree_id', 'inventory_year']]
    
    #integrity_issues = dbh_reduction, position_reversal, life_status_reversal, integrity_reversal, decay_inconsistency
    #return integrity_issues
    
    return dbh_reduction, position_reversal, life_status_reversal, integrity_reversal, decay_inconsistency

def check_species_change(df, xpi):
    # Group by the relevant columns and check if the species changes across inventory years
    species_change = df.groupby(['site_id', 'composed_site_id', xpi, 'tree_id'])['full_scientific'].transform('nunique') > 1

    # Create a new column in the dataframe to store the result of the species change check
    df['species_change'] = species_change

    # Filter rows where there is a species change and create an integrity issue log
    species_integrity_issues = df[df['species_change'] == True][['site_id', 'composed_site_id', 'spi_id', 'tree_id', 'inventory_year', 'full_scientific']]
    print(f"{species_integrity_issues}")
    return species_integrity_issues

    #def check_geometry_shift(df, xpi):
    # Check 4: Geometry shifts more than 1 meter
    #df['previous_geometry_x'] = grouped['geometry_x'].shift(1)
    #df['previous_geometry_y'] = grouped['geometry_y'].shift(1)
    #geometry_criteria = ((df['geometry_x'] - df['previous_geometry_x']).pow(2) +
    #                     (df['geometry_y'] - df['previous_geometry_y']).pow(2)).pow(0.5) > 1
    #integrity_issues['geometry_shift'] = df[geometry_criteria & df['previous_geometry_x'].notna()]

def check_missing_in_census(df, xpi):
    # Step 1: Calculate the number of distinct census years per unique_plot_id
    plot_census_count = df.groupby(['site_id', 'composed_site_id', xpi])['inventory_year'].nunique().reset_index()
    plot_census_count.columns = ['site_id', 'composed_site_id', xpi, 'total_census_years']

    # Step 2: Merge this information back to the original dataframe to know how many censuses are expected for each plot
    df = df.merge(plot_census_count, on=['site_id', 'composed_site_id', xpi], how='left')

    # Step 3: Calculate how many distinct census years each tree_id appears in per unique_plot_id
    tree_census_count = df.groupby(['site_id', 'composed_site_id', xpi, 'tree_id'])['inventory_year'].nunique().reset_index()
    tree_census_count.columns = ['site_id', 'composed_site_id', xpi, 'tree_id', 'tree_census_years']

    # Step 4: Merge this tree-level census count back to the dataframe to check for missing census records
    df = df.merge(tree_census_count, on=['site_id', 'composed_site_id', xpi, 'tree_id'], how='left')

    # Step 5: Identify trees that are missing from any census (when tree_census_years is less than total_census_years)
    census_gap = df['tree_census_years'] < df['total_census_years']

    # Store the result of trees that are missing from any census for their unique_plot_id
    missing_in_census_integrity_issues = {}
    missing_in_census_integrity_issues['missing_in_census'] = df[census_gap]
    print(f"{missing_in_census_integrity_issues}")
    return missing_in_census_integrity_issues
    

# Function to send results via email
def send_email(results, email, xpi):
    yag = yagmail.SMTP(EMAIL_USER, EMAIL_PASSWORD)
    subject = f"Integrity Test Results for plots of type: {xpi}"
    body = "Please find the results attached."

    # Define the file path for saving the JSON file
    temp_dir = "temp_dir"
    os.makedirs(temp_dir, exist_ok=True)
    json_file = "temp_dir/integrity_test_results.json"  # Temporary file path

    # Process `results` to remove headers
    processed_results = {key: value.to_dict(orient='records') if hasattr(value, 'to_dict') else value for key, value in results.items()}

    # Convert the `processed_results` dictionary to JSON and save to file (without headers)
    with open(json_file, 'w') as f:
        json.dump(processed_results, f, default=str, indent=4)

    # Send email with the JSON file as an attachment
    yag.send(to=email, subject=subject, contents=[body, json_file])

# Run tests and send the results
def run_tests_in_background(df_integrity, email, df, xpi):
    results = {}
    print(f"Running tests in background")
    try:
        # results['results_plausibility_test'] = plausibility_test(df_integrity)
        results['dbh_reduction'], results['position_reversal'], results['life_status_reversal'], results['integrity_reversal'], results['decay_inconsistency'] = plausibility_test(df_integrity)
        results['species'] = check_species_change(df, xpi)
        results['missing_in_census'] = check_missing_in_census(df, xpi)
        print(f'Tests was run in the background. Results will be sent to your email.')
        send_email(results, email, xpi)  # Send results via email
    except Exception as e:
        print(f"Error running plausibility_test: {e}")
        
def file_comparison(file_1, file_2):
    # Load the files into DataFrames
    df1 = pd.read_csv(file_1)
    df2 = pd.read_csv(file_2)

    # Convert columns to lowercase
    df1.columns = df1.columns.str.lower()
    df2.columns = df2.columns.str.lower()
    
    # Define the join columns
    join_columns = ["site_id", "wildcard_id", "spi_id", "inventory_year","full_scientific", "life"]

    # Check if all join columns are present in both DataFrames
    missing_columns_df1 = [col for col in join_columns if col not in df1.columns]
    missing_columns_df2 = [col for col in join_columns if col not in df2.columns]

    if missing_columns_df1 or missing_columns_df2:
        if missing_columns_df1:
            st.error(f"File 1 is missing required columns: {missing_columns_df1}")
        if missing_columns_df2:
            st.error(f"File 2 is missing required columns: {missing_columns_df2}")
        return None  # Stop if required columns are missing

    # Merge DataFrames
    merged_df = pd.merge(df1, df2, on=join_columns, suffixes=('_file1', '_file2'))
    
    # Check for columns with the same name in both files and calculate differences if numerical
    for col in set(df1.columns).intersection(df2.columns) - set(join_columns):
        if pd.api.types.is_numeric_dtype(df1[col]) and pd.api.types.is_numeric_dtype(df2[col]):
            merged_df[f"{col}_diff"] = merged_df[f"{col}_file1"] - merged_df[f"{col}_file2"]

    # Display merged DataFrame
    return merged_df