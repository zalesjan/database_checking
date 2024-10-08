#OLD
def handle_extended_attributes(df, ordered_core_attributes, table_name):
    """
    OLD CODE WHERE WE USED TO PUT THE JSON DATA IN THE DB SEPARATELY

    Determines the extra columns (extended attributes) in the DataFrame and updates the database.

    Args:
        df: DataFrame containing the uploaded data.
        ordered_core_attributes: List of core attributes.
        table_name: The name of the table where data needs to be updated.

    Returns:
        A list of extra columns (extended attributes).
    """
    # Determine the extra columns that are not part of the ordered_core_attributes
    extra_columns = [col for col in df.columns if col not in ordered_core_attributes]

    # If extra columns are found, insert them into the JSONB field of the specified table
    if extra_columns:
        write_and_log(f"Extra columns found: {extra_columns}")
        
        # Create a DataFrame to handle only the 'record_id' and extra columns
        extra_data_df = df[['record_id'] + extra_columns]
        
        # Iterate over the rows to convert extra data into JSONB and update the database
        for index, row in extra_data_df.iterrows():
            # Convert non-NaN extra columns to a dictionary for each record
            json_data = row[extra_columns].dropna().to_dict()
            if json_data:
                insert_jsonb_data(table_name, record_id=row['record_id'], json_data=json_data)
                
        write_and_log("Extra columns successfully inserted into JSONB field.")
    else:
        write_and_log("No extra columns found.")

    return extra_columns

#OLD
def insert_jsonb_data(table_name, reocrd_id, json_data):
    """
    Insert JSONB data into the specified table and record.
    """
    conn = get_db_connection()
    if conn is None:
        return  # If connection fails, do nothing
    try:
        cur = conn.cursor()
        # Insert the JSON data into the tree table (assuming you have a JSONB field called `attributes`)
        update_query = f"""
            UPDATE public.{table_name}
            SET extended_attributes = COALESCE(attributes, '{{}}') || %s::jsonb
            WHERE record_id = %s;
        """
        
        cur.execute(update_query, [json.dumps(json_data), record_id])
        conn.commit()

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        cur.close()
        conn.close()

#OLD
def unfolded_determine_copy_command(file_path):
    base_filename = os.path.basename(file_path).lower()
    
    if "lying" in base_filename:
        copy_command = """
        "COPY public.tree_staging 
        (site_id, lpi_id, spi_id, tree_id, stem_id, piece_id, inventory_year, consistent_id, life, position, integrity, date, SPECIES_CODE, full_scientific, diameter_1, diameter_2, length, decay, geom)
        FROM STDIN WITH DELIMITER E'\t' CSV HEADER NULL '\\N';"""
    elif "standing" in base_filename:
        copy_command = """
        COPY public.tree_staging 
        (site_id, lpi_id, spi_id, tree_id, stem_id, inventory_year, consistent_id, life, position, integrity, height, date, SPECIES_CODE, full_scientific, dbh, decay, geom, HOM, BREAK_TYPE, CAVITIES, FUNGI, INSECT, BARK_DAMAGE, FORK, DECAY_TEXT)
        FROM STDIN WITH DELIMITER E'\t' CSV HEADER NULL '\\N';"""
    elif "design" in base_filename:
        copy_command = """
        copy public.site_design 
        (site_id, institute, site_name, inventory_year, inventory_type, no_plots, circle_no, circle_radius, circle_azimuth, circle_distance, pom_mark, standing_alive_threshold, standing_dead_threshold, lying_alive_threshold, lying_dead_threshold, species_pool, lis_cwd, geom, stem_coord_ref_point, epsg_code, other) 
        FROM STDIN WITH DELIMITER E'\t' CSV HEADER NULL '\\N';"""
    elif "plots" in base_filename:
        copy_command = """COPY public.plot
        (site_id, inventory_year, lpi_id, spi_id, circle_no, circle_radius, plot_sampled, sampled_area, inventory_year, consistent_id_standing, consistent_id_lying, other, geom) 
        FROM STDIN WITH DELIMITER E'\t' CSV HEADER NULL '\\N';"""
    else:
        raise ValueError("File name does not match any known configuration")

    return copy_command

#OLD
def execute_copy_command(core_attributes, file_path):
    # Establish a database connection
    conn = get_db_connection()
    if conn is None:
        return  # If connection fails, do nothing
    
    try:
        cur = conn.cursor()
        # Determine the appropriate COPY command
        copy_command = determine_copy_command(core_attributes, file_path)
    
        with open(file_path, 'r') as f:
            cur.copy_expert(sql=copy_command, file=f)
    
        # Commit the transaction and Log the success message
        conn.commit()
        logging.info(f"Successfully copied data from {file_path} using command: {copy_command}")

    except Exception as e:
        # Log any error that occurs
        #logging.error(f"Error copying data from {file_path}: {e}")
        print("An error occurred while connecting to the database:", str(e))
    finally:
        cur.close()
        conn.close()

#OLD
def load_config(file_path):
    base_filename = os.path.basename(file_path).lower()

    if "lying" in base_filename:
        config_file = "expectations/expe_lying.json"
    elif "standing" in base_filename:
        config_file = "expectations/expe_standing.json"
    elif "design" in base_filename:
        config_file = "expectations/expe_site_design.json"
    elif "plots" in base_filename:
        config_file = "expectations/expe_plots.json"
    else:
        raise ValueError("File name does not match any known configuration")

    with open(config_file, 'r') as f:
        config = json.load(f)

    return config