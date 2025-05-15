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

logging.basicConfig(
    filename='logs.log',  # Specify your log file name
    level=logging.INFO,  # Set the logging level to INFO or WARNING
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the format of log messages
)
logging.info('Logging setup is complete.')

# Email configuration
EMAIL_USER = st.secrets['email']['EMAIL_USER']
EMAIL_PASSWORD = st.secrets['email']['EMAIL_PASSWORD']

def validate_file(df, config, file_name):
    st.header(f'validating file: {file_name}')
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

def set_base_columns(df, xpi):
# Define required columns dynamically based on availability
    base_columns = ['site_id', xpi, 'tree_id', 'inventory_year']

    # Dynamically add `composed_site_id` and `wildcard_sub_id` if they exist
    if 'composed_site_id' in df.columns:
        base_columns.append('composed_site_id')
    if 'wildcard_sub_id' in df.columns:
        base_columns.append('wildcard_sub_id')

    return base_columns

def plausibility_test(df, xpi, base_columns):
    

    # Initialize empty results
    dbh_reduction = None
    position_reversal = None
    decay_inconsistency = None
    integrity_reversal = None
    life_status_reversal = None

    # Check DBH Reduction: Reduction by more than 2.5 cm or 10%
    if {'dbh', 'previous_dbh', 'life'}.issubset(df.columns):
    
        # Drop NaN values
        # Ensure numeric types (force coercion for non-numeric values to NaN)
        df['dbh'] = pd.to_numeric(df['dbh'], errors='coerce')
        df['previous_dbh'] = pd.to_numeric(df['previous_dbh'], errors='coerce')

        # Drop rows with missing values in dbh or previous_dbh
        df.dropna(subset=['dbh', 'previous_dbh'], inplace=True)

        # Apply the filters
        consistent_id_filter = df['consistent_id'] == "Y"
        life_filter = df['life'] == "A"  
        dbh_criteria = (df['dbh'] < df['previous_dbh'] - 25) | (df['dbh'] < df['previous_dbh'] * 0.9)

        # Final filter
        dbh_reduction = df[consistent_id_filter & life_filter & dbh_criteria][base_columns]
    else:
        consistent_id_filter = df['consistent_id'] == "Y"
    # Check Position Change: Position changes from L to S
    if {'previous_position', 'position'}.issubset(df.columns):
        position_criteria = (df['previous_position'] == 'L') & (df['position'] == 'S')
        position_reversal = df[consistent_id_filter & position_criteria][base_columns]

    # Check Life Status Change: Life status changes from D to A
    if {'previous_life', 'life'}.issubset(df.columns):
        life_criteria = (df['previous_life'] == 'D') & (df['life'] == 'A')
        life_status_reversal = df[life_criteria & consistent_id_filter][base_columns]

    # Check Integrity Change: Integrity changes from F to C
    if {'previous_integrity', 'integrity', 'life'}.issubset(df.columns):
        death_filter = df['life'] == "D"
        integrity_criteria = (df['previous_integrity'] == 'F') & (df['integrity'] == 'C')
        integrity_reversal = df[consistent_id_filter & integrity_criteria & death_filter & df['previous_integrity'].notna()][base_columns]

    # Check Decay: Decay values should increase or stay the same (0 = no decay, 5 = complete decay)
    if {'decay', 'previous_decay'}.issubset(df.columns):
        decay_criteria = df['decay'] < df['previous_decay']
        decay_inconsistency = df[consistent_id_filter & decay_criteria & df['previous_decay'].notna()][base_columns]

    return dbh_reduction, position_reversal, life_status_reversal, integrity_reversal, decay_inconsistency


#check dbh for smaller than threshold
def tree_smaller_than_threshold(institute, role):
    dbh_smaller_than_threshold = f"""
    SELECT t.composed_site_id, t.record_id, t.dbh, d.standing_alive_threshold 
    FROM public.trees t
    JOIN public.plots p
        ON plot_record_id = p.record_id
    JOIN site_design d
        ON site_design_record_id = d.record_id
    where t.dbh < d.standing_alive_threshold
    and p.composed_site_id like %s;
    """
    _, dbh_smaller_than_threshold = do_query(dbh_smaller_than_threshold, role, (f"%{institute}%",))
    
    if dbh_smaller_than_threshold is not None:
        write_and_log("Data Preview: Test of DBH smaller than threshold")
        st.dataframe(dbh_smaller_than_threshold)  # Display the result as a DataFrame
    else:
        st.write("No tree_smaller_than_threshold or an error occurred.")

def check_species_change(df, base_columns):
    
    # Ensure 'full_scientific' exists before proceeding
    if 'full_scientific' not in df.columns:
        print("Warning: 'full_scientific' column is missing from DataFrame.")
        return None
    
    # Remove 'inventory_year' to ensure changes are checked across years
    grouping_columns = [col for col in base_columns if col != 'inventory_year']
    
    # Group by relevant columns and check if species changes across inventory years
    species_counts = df.groupby(grouping_columns)['full_scientific'].nunique().reset_index()
    
    # Filter groups where species count > 1 (i.e., changes occurred)
    species_changes = species_counts[species_counts['full_scientific'] > 1]

    # Merge back to the original DataFrame to filter relevant rows
    df = df.merge(species_changes[grouping_columns], on=grouping_columns, how='inner')

    # Filter rows where there is a species change
    species_integrity_issues = df[grouping_columns + ['inventory_year', 'full_scientific']]
    
    print(f"Species integrity issues detected:\n{species_integrity_issues}")
    
    return species_integrity_issues

    #def check_geometry_shift(df, xpi):
    # Check 4: Geometry shifts more than 1 meter
    #df['previous_geometry_x'] = grouped['geometry_x'].shift(1)
    #df['previous_geometry_y'] = grouped['geometry_y'].shift(1)
    #geometry_criteria = ((df['geometry_x'] - df['previous_geometry_x']).pow(2) +
    #                     (df['geometry_y'] - df['previous_geometry_y']).pow(2)).pow(0.5) > 1
    #integrity_issues['geometry_shift'] = df[geometry_criteria & df['previous_geometry_x'].notna()]

def check_missing_in_census(df, base_columns):
    # Ensure 'inventory_year' exists before proceeding
    if 'inventory_year' not in df.columns:
        print("Warning: 'inventory_year' column is missing from DataFrame.")
        return None

    # Remove 'inventory_year' and 'tree_id' from base_columns safely
    base_columns = [col for col in base_columns if col not in {'inventory_year', 'tree_id'}]

    # Step 1: Calculate the number of distinct census years per unique plot
    plot_census_count = df.groupby(base_columns)['inventory_year'].nunique().reset_index()
    plot_census_count.rename(columns={'inventory_year': 'total_census_years'}, inplace=True)

    # Step 2: Merge this information back into df
    df = df.merge(plot_census_count, on=base_columns, how='left')

    # Step 3: Calculate how many distinct census years each tree_id appears in per unique plot
    tree_census_count = df.groupby(base_columns + ['tree_id'])['inventory_year'].nunique().reset_index()
    tree_census_count.rename(columns={'inventory_year': 'tree_census_years'}, inplace=True)

    # Step 4: Merge tree-level census count into df
    df = df.merge(tree_census_count, on=base_columns + ['tree_id'], how='left')

    # Step 5: Identify missing census records, handling NaN values
    df['tree_census_years'].fillna(0, inplace=True)
    df['total_census_years'].fillna(0, inplace=True)
    
    census_gap = df['tree_census_years'] < df['total_census_years']

    # Store results
    missing_in_census_integrity_issues = df[census_gap][base_columns + ['tree_id', 'inventory_year']]

    print(missing_in_census_integrity_issues)
    return {"missing_in_census": missing_in_census_integrity_issues}

def find_previous_record_id_columns_from_mapping(table_mapping, table_name):
    for key, (mapped_table_name, _, _, previous_record_id_columns, _) in table_mapping.items():    # Find the correct key in table_mapping that corresponds to `table_name`
        if mapped_table_name == table_name:  # Check if the table_name matches
            break  # Stop looping when the correct match is found
    else:
        previous_record_id_columns = None  # If no match is found, set to None
    
    return previous_record_id_columns
    
# Function to send results via email
def send_email(results, statistics, file, email, xpi):
    yag = yagmail.SMTP(EMAIL_USER, EMAIL_PASSWORD)
    subject = f"Integrity Test Results for plots of type: {xpi}"
    body = "Please find the results attached."
   
    json_file = save_json (results, statistics, file)

    # Send email with the JSON file as an attachment
    yag.send(to=email, subject=subject, contents=[body, json_file])

# Function to save results as json
def save_json(results, statistics, file, xpi):

    # Define the file path for saving the JSON file
    temp_dir = "temp_dir"
    os.makedirs(temp_dir, exist_ok=True)
    json_file = f"temp_dir/integrity_test_results_{file}_{xpi}.json"  # Temporary file path

    # Process `results` to remove headers
    processed_results = {
        key: value.to_dict(orient='records') if isinstance(value, pd.DataFrame) else value 
        for key, value in results.items()
    }

    # Combine statistics and results into one JSON structure
    output_data = {
        "statistics": statistics,
        "results": processed_results
    }

    # Save to JSON file
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, default=str, indent=4)

    print(f"Results successfully saved to {json_file}")
    return output_data

# Run tests and send the results
def run_tests_in_background(df_integrity, df, file, xpi):
    # Check if df_integrity is empty and stop execution
    df_integrity = df_integrity
    print(df_integrity)
    if df_integrity.empty:
        print("⚠️ Warning: df_integrity is empty. Skipping plausibility tests.")
        return None 
    
    results = {}
    statistics = []
    base_columns = set_base_columns(df, xpi)
    print(f"Running tests in background")
    try:
        # results['results_plausibility_test'] = plausibility_test(df_integrity)
        results['dbh_reduction'], results['position_reversal'], results['life_status_reversal'], results['integrity_reversal'], results['decay_inconsistency'] = plausibility_test(df_integrity, xpi, base_columns)
        results['species'] = check_species_change(df_integrity, base_columns)
        results['missing_in_census'] = check_missing_in_census(df_integrity, base_columns)

        # Calculate statistics
        total_records = len(df_integrity)  # Total records being tested
        print("\n==== Plausibility Test Results Summary ====")

        for key, value in results.items():
            if isinstance(value, pd.DataFrame):  # If the value is a DataFrame, count rows
                count = value.shape[0]  # Get number of rows
            elif isinstance(value, (list, set, dict)):  # Count items in lists, sets, or dicts
                count = len(value)
            else:
                count = 0  # If it's something unexpected, set count to 0

            ratio = count / total_records if total_records > 0 else 0
            percentage = ratio * 100
            print(f"{key}: {count} issues found ({percentage:.2f}% of total {total_records})")
            statistics.append({"test": key, "issues found": count, "total": total_records, "that is this many percent": f"{percentage:.2f}"})

        output_data = save_json(results, statistics, file, xpi)
        return output_data
        
    except Exception as e:
        print(f"Error running plausibility_test: {e}")
        raise e
    
import concurrent.futures 

def run_parallel_plausibility_tests(df_integrity_lpi_id, df_integrity_spi_id, df, file):
    """Run tests in parallel for different datasets."""
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_lpi = executor.submit(run_tests_in_background, df_integrity_lpi_id, df, file.name, 'lpi_id')
        future_spi = executor.submit(run_tests_in_background, df_integrity_spi_id, df, file.name, 'spi_id')
        
        # Retrieve results when finished
        output_data_lpi = future_lpi.result()
        output_data_spi = future_spi.result()
        
        write_and_log("Tests were run in the background. Results will be saved in JSON file.")
        write_and_log(output_data_lpi)
        write_and_log(output_data_spi)

def file_comparison(file_1, file_2):
    # Load the files into DataFrames
    df1 = pd.read_csv(file_1)
    df2 = pd.read_csv(file_2)

    # Convert columns to lowercase
    df1.columns = df1.columns.str.lower()
    df2.columns = df2.columns.str.lower()
    
    # Define the join columns
    join_columns = ["wildcard_sub_id", "spi_id", "circle_no"]

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