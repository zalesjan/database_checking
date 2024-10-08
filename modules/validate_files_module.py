
import json
import pandas as pd
import sys
import re
from datetime import datetime
import os
import logging
from modules.logs import write_and_log

def validate_file(df, config, file_name):

    logging.basicConfig(
        filename='validation_logs.log',  # Specify your log file name
        level=logging.INFO,  # Set the logging level to INFO or WARNING
        format='%(asctime)s - %(levelname)s - %(message)s',  # Define the format of log messages
    )
    logging.info('Validation logging setup is complete.')

    expected_columns = config.get("expected_columns", {})
    validation_results = {}
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
            validation_results[f"{standard_name}"] = "Column not found, skipping checks."
            existence_checks[f"{standard_name}"] = False
            continue
        else:
            existence_checks[f"{standard_name}"] = True

        # Implement specific validations
        if expectations.get("non_null", False):
            non_null_check = df[actual_column_name].notnull().all()
            validation_results[f"{standard_name} (non-null)"] = bool(non_null_check)

        if expectations.get("is_numeric", False):
            numeric_check = pd.to_numeric(df[actual_column_name], errors='coerce').notnull().all()
            validation_results[f"{standard_name} (numeric)"] = bool(numeric_check)

        if "range" in expectations:
            min_val, max_val = expectations["range"]
            if max_val == "current_year":
                max_val = datetime.now().year
            numeric_series = pd.to_numeric(df[actual_column_name], errors='coerce')
            range_check = numeric_series.between(min_val, max_val).all()
            validation_results[f"{standard_name} (range {min_val}-{max_val})"] = bool(range_check)

        if expectations.get("is_boolean", False):
            boolean_check = df[actual_column_name].isin([0, 1, True, False]).all()
            validation_results[f"{standard_name} (boolean)"] = bool(boolean_check)

        if "specific_characters" in expectations:
            pattern = expectations["specific_characters"]
            pattern_check = df[actual_column_name].apply(lambda x: bool(re.match(pattern, str(x)))).all()
            validation_results[f"{standard_name} (pattern {pattern})"] = bool(pattern_check)

        if "external_file" in expectations:
            external_file_path = expectations["external_file"]
            with open(external_file_path, 'r') as ext_file:
                allowed_values = [line.strip() for line in ext_file.readlines()]
            external_check = df[actual_column_name].isin(allowed_values).all()
            validation_results[f"{standard_name} (external file check)"] = bool(external_check)

        if "allowed_values" in expectations:
            allowed_values = expectations["allowed_values"]
            allowed_values_check = df[actual_column_name].isin(allowed_values).all()
            validation_results[f"{standard_name} (allowed values {allowed_values})"] = bool(allowed_values_check)

    if all(validation_results.values()):
        logging.info(f"Validation passed for file {file_name}.")
    else:
        logging.warning(f"Validation failed for file {file_name} with issues: {validation_results}")

    # Check if validation_results is a dictionary
    if isinstance(validation_results, dict):
        write_and_log("Validation Results:")
        write_and_log(validation_results)  # display the dictionary as JSON
    else:
        write_and_log("Validation results are not in a valid format.")

    return validation_results

def log_validation(validation_results, file_name):
    if all(validation_results.values()):
        logging.info(f"Validation passed for file {file_name}.")
    else:
        logging.warning(f"Validation failed for file {file_name} with issues: {validation_results}")

def distinct_asc_values_each_column(df):
    distinct_values = {}
    for column in df.columns:
        values = df[column].dropna().unique()

        # Sort numeric values in ascending order
        if pd.api.types.is_numeric_dtype(df[column]):
            sorted_values = sorted(values)
        # Sort string values alphabetically
        elif pd.api.types.is_string_dtype(df[column]):
            sorted_values = sorted(values, key=str.lower)
        else:
            sorted_values = values.tolist()
        
        distinct_values[column] = sorted_values
    
    write_and_log("Distinct ASC Values in Each Column:")
    for column, values in distinct_values.items():
        write_and_log(f"**{column}**: {values}")

def distinct_values_with_counts(df):
    distinct_values = {}
    unique_value_counts = {}
    for column in df.columns:
        # Get distinct values and sort them
        distinct_values[column] = sorted(df[column].dropna().unique().tolist())
        # Get the number of unique values
        unique_value_counts[column] = df[column].nunique(dropna=True)
    
    write_and_log("Distinct Values in Each Column:")
    for column, values in distinct_values.items():
        count = unique_value_counts[column]  # Retrieve the unique count for the current column
        write_and_log(f"**{column}**: {values}, that is {count} unique values")

def distinct_value_counts(df):
    unique_value_counts = {}
    for column in df.columns:
        unique_count = df[column].nunique(dropna=True)  # Get the number of unique values
        unique_value_counts[column] = unique_count
    
    write_and_log("Number of Unique Values in Each Column:")
    for column, count in unique_value_counts.items():
        write_and_log(f"**{column}**: {count} unique values")

def value_counts_for_each_distinct_value(df):
    distinct_values_with_counts = {}
    for column in df.columns:
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
    
    write_and_log("Distinct Values and Counts in Each Column:")
    for column, values in distinct_values_with_counts.items():
        write_and_log(f"**{column}**:")
        for value, count in values.items():
            write_and_log(f" - {value}: {count}")

