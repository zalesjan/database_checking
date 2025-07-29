import streamlit as st
import psycopg2
import os
import io
import logging
from datetime import datetime
import pandas as pd
import re
from modules.logs import write_and_log
from modules.dataframe_actions import determine_copy_command_for_ecology_with_ignore, biodiversity_determine_copy_command_with_ignore, determine_copy_command_with_ignore, prepare_biodiversity_dataframe_for_copy,  prepare_dataframe_for_copy, table_mapping

#queries used in helper operations
get_wildcard_db_id = "SELECT composed_site_id, record_id FROM public.sites"

truncate_calc_basal_area = f"""TRUNCATE TABLE calc_basal_area;"""
truncate_no_plots_per_year = f"""TRUNCATE TABLE no_plots_per_year;"""
truncate_lying = f"""TRUNCATE TABLE basic_query_standing;"""
truncate_standing = f"""TRUNCATE TABLE basic_query_lying;"""

basic_query_no_plots_per_year = f"""
        INSERT INTO no_plots_per_year
        SELECT  
            COUNT(p.record_id) as p_num_plots, 
            s.record_id as sd_record_id,
            s.inventory_year,
            s.composed_site_id 
        FROM public.plots p
        JOIN
            public.site_design s ON p.site_design_record_id = s.record_id
        WHERE
            p.composed_site_id like %s
        GROUP BY sd_record_id, s.composed_site_id, s.inventory_year
        ORDER BY s.composed_site_id ASC;
        """

basic_query_calc_basal_area = f"""
        INSERT INTO calc_basal_area
		SELECT 
            t.*, 
            (pi() *power(dbh/20, 2) ) AS basal_area
        FROM public.trees t
        JOIN
            public.plots ON t.plot_record_id = plots.record_id
        WHERE
            t.composed_site_id like %s;
        """

basic_query_standing = f"""
        INSERT INTO basic_query_standing
            SELECT 
			 	plots.composed_site_id,
				site_design.inventory_type,
				plots.inventory_year,
				p.p_num_plots,
                COUNT(calc_basal_area.record_id)/((plots.sampled_area/10000)*p.p_num_plots) AS ntrees_ha_standing,
                SUM(calc_basal_area.basal_area)/((plots.sampled_area)*p.p_num_plots) AS ba_hectare_standing,
                MAX(calc_basal_area.dbh)/10 AS dbh_max_standing,
                MIN(calc_basal_area.dbh)/10 AS dbh_min_standing,
                AVG(calc_basal_area.dbh)/10 AS dbh_mean_standing
            FROM
                public.site_design
        	JOIN
            	public.plots ON site_design.record_id = plots.site_design_record_id
            JOIN
                calc_basal_area ON plots.record_id = calc_basal_area.unique_plot_id
            JOIN
                no_plots_per_year p ON plots.site_design_record_id = p.sd_record_id
            WHERE
                calc_basal_area.position = 'S'
            GROUP BY
                plots.composed_site_id, plots.inventory_year, plots.sampled_area, p.p_num_plots, site_design.inventory_type
            order by plots.composed_site_id;
            """

basic_query_lying = f"""                
        INSERT INTO basic_query_lying
            SELECT 
				plots.composed_site_id,
				site_design.inventory_type,
				plots.inventory_year,
				p.p_num_plots,
                COUNT(calc_basal_area.record_id)/((plots.sampled_area/10000)*p.p_num_plots) AS ntrees_ha_lying,
				SUM(calc_basal_area.volume)/((plots.sampled_area)*p.p_num_plots) as volume,
                MAX(calc_basal_area.diameter_1)/10 AS max_d1,
                MIN(calc_basal_area.diameter_1)/10 AS min_d1,
                AVG(calc_basal_area.diameter_1)/10 AS mean_d1,
                MAX(calc_basal_area.diameter_2)/10 AS max_d2,
                MIN(calc_basal_area.diameter_2)/10 AS min_d2,
                AVG(calc_basal_area.diameter_2)/10 AS mean_d2
            FROM
                public.site_design
            JOIN
                public.plots ON site_design.record_id = plots.site_design_record_id
            JOIN
                calc_basal_area ON plots.record_id = calc_basal_area.unique_plot_id
            JOIN
                no_plots_per_year p ON plots.site_design_record_id = p.sd_record_id
            WHERE
                calc_basal_area.position = 'L'
			GROUP BY
            	plots.composed_site_id, plots.inventory_year, plots.sampled_area, p.p_num_plots, site_design.inventory_type
            order by 
				plots.composed_site_id;
            """

basic_query_main_query = f""" 
    SELECT 
        standing.composed_site_id,
        standing.inventory_type,
        standing.inventory_year,
        standing.ntrees_ha_standing,
        standing.ba_hectare_standing,
        standing.dbh_max_standing,
        standing.dbh_min_standing,
        standing.dbh_mean_standing,
		lying.volume,
		lying.ntrees_ha_lying,
		lying.max_d1,
        lying.min_d1,
        lying.mean_d1,
		lying.max_d2,
        lying.min_d2,
        lying.mean_d2
    FROM
        basic_query_standing standing
    LEFT JOIN
        basic_query_lying lying ON 
        standing.composed_site_id = lying.composed_site_id
        AND standing.inventory_type = lying.inventory_type
        AND standing.inventory_year = lying.inventory_year
    order by standing.composed_site_id;    
        """

tree_staging_id =f"""
        UPDATE tree_staging t
        SET plot_record_id = p.record_id
        FROM plots p
        WHERE 
            t.composed_site_id = p.composed_site_id
            AND t.inventory_year = p.inventory_year
            AND t.inventory_id = p.inventory_id
            AND t.circle_no IS NOT DISTINCT FROM p.circle_no
            AND (t.lpi_id = p.lpi_id OR t.spi_id = p.spi_id)
            and p.composed_site_id like %s;
        """

cwd_id =f"""
        UPDATE cwd t
        SET unique_plot_id = p.record_id
        FROM plots p
        WHERE 
            t.composed_site_id = p.composed_site_id
            AND t.inventory_year = p.inventory_year
            AND t.inventory_id = p.inventory_id
            AND (t.lpi_id = p.lpi_id OR t.spi_id = p.spi_id)
            and p.composed_site_id like %s;
        """

plots_id =f"""
        UPDATE plots p
        SET site_design_record_id = d.record_id
        FROM site_design d
        WHERE 
            p.composed_site_id = d.composed_site_id
            AND p.inventory_id = d.inventory_id
            AND p.inventory_year = d.inventory_year
            AND d.circle_radius IS NOT DISTINCT FROM p.circle_radius
            AND d.circle_no IS NOT DISTINCT FROM p.circle_no
			and d.composed_site_id like %s;
        """

site_design_id =f"""
        UPDATE site_design d
		SET site_record_id = s.record_id
		FROM sites s
		WHERE 
			d.composed_site_id = s.composed_site_id
            and s.composed_site_id like %s;
        """
move_data_to_tree = """
        INSERT INTO public.trees (composed_site_id, plot_record_id, tree_id, stem_id, piece_id, inventory_year, consistent_id, life, position, integrity, height, date, full_scientific, dbh, decay, diameter_1, diameter_2, length, geom, extended_attributes, circle_no, inventory_id, volume, epsg_code, diameter_130, udt)
        SELECT 
            composed_site_id, plot_record_id, tree_id, stem_id, piece_id, inventory_year, consistent_id, life, position, integrity, height, date, full_scientific, dbh, decay, diameter_1, diameter_2, length, geom, extended_attributes, circle_no, inventory_id, volume, epsg_code, diameter_130, now()
        FROM
            public.tree_staging;
        """
truncate_tree_staging = """truncate tree_staging"""

show_counts_of_all = f"""
        SELECT
            COUNT(DISTINCT sites.institute)AS institutes,
            COUNT (DISTINCT sites.record_id) AS count_sites,
            COUNT(DISTINCT site_design.record_id) AS count_site_designs,
            COUNT(DISTINCT plots.record_id)AS count_plots,
            COUNT(DISTINCT trees.record_id)AS count_trees 
        FROM
            public.sites
        JOIN
            public.site_design ON sites.record_id = site_design.site_record_id
        JOIN
            public.plots ON site_design.record_id = plots.site_design_record_id
        JOIN
            public.trees ON plots.record_id = trees.plot_record_id    
        """

# Set up logging
log_folder = 'logs'
os.makedirs(log_folder, exist_ok=True)
log_file = os.path.join(log_folder, 'data_copy.log')

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

def select_role():
    # Define available roles
    available_roles = ["role_superuser_DB_PROD", "role_vukoz_DB_PROD", "role_superuser_DB_old", "role_superuser_DB_development", "role_superuser_DB_VUK-raw_data"]

    # Create a select box for role selection
    selected_role = st.selectbox("Select PostgreSQL Role:", available_roles, index=3)
    return selected_role

def password_check():
    # Password input field, check the password entered by the user is correct
    #if not st.session_state["authenticated"]:
    user_password = st.text_input("DB upload is reserved for VUK. To proceed, enter password", type="password")
    PASSWORD = st.secrets["general"]["site_password"]
    if user_password == PASSWORD:
        #st.session_state["authenticated"] = True
        st.success("Password is correct. You can now proceed.")
        return True
    else:
        st.warning("Please enter the correct password to proceed.")

def get_db_connection(role):
    try:
        conn = psycopg2.connect(
            host=st.secrets[role]["DB_HOST"],
            port=st.secrets[role]["DB_PORT"],
            dbname=st.secrets[role]["DB_NAME"],
            user=st.secrets[role]["DB_USER"],
            password=st.secrets[role]["DB_PASSWORD"]
        )
        print("Connection to the database was successful!")
        return conn
    except Exception as e:
        print("An error occurred while connecting to the database:", str(e))
        return None

def do_query(query, role, placeholders=None):
    """
    Executes a SQL query with optional placeholders.
    
    Args:
        query (str): The SQL query string.
        placeholders (tuple, optional): Tuple containing one or two placeholder values.

    Returns:
        tuple: (affected_rows, result_df) - the latter for SELECTs, the former for other than SELECT (UPDATE, INSERT, etc.)
    """
    conn = get_db_connection(role)
    if conn is None:
        st.error("Database connection failed.")
        return None, None  # Ensure function always returns a tuple

    try:
        cur = conn.cursor()

        # Handling multiple placeholders
        if placeholders:
            cur.execute(query, placeholders)
            print(f"Executing: {query} with placeholders {placeholders}")
        else:
            cur.execute(query)

        # Case 1: If query is SELECT and returns rows
        if query.strip().upper().startswith("SELECT"):
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]  # Get column names
            result_df = pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame()
            conn.commit()
            return None, result_df  # Ensure it returns a tuple with None for affected_rows
        
        # Case 2: If query is UPDATE/DELETE and modifies rows
        affected_rows = cur.rowcount  # Get number of affected rows
        conn.commit()
        return affected_rows, None  # Ensure it returns a tuple with None for result_df

    except Exception as e:
        st.error(f"An error occurred: {e}")
        return None, None  # Ensure function always returns two values

def load_data_with_copy_command(df, schema, file_path, table_name, column_mapping, ordered_core_attributes, extra_columns, ignored_columns, role):
    """
    Load data using the constructed COPY command, including JSONB data.
    
    Args:
        df (pd.DataFrame): DataFrame with core and extra attributes.
        file_path (str): Path to the file being processed.
        table_name (str): Name of the target database table.
        extra_columns (list): List of columns considered as extra attributes.

    Returns:
        None
    """
    if schema in ["biodiversity", "site_design"] or table_name in ["site_design", "biodiversity"]:
        copy_command = biodiversity_determine_copy_command_with_ignore(file_path, ordered_core_attributes, extra_columns, table_name, df.columns, schema, ignored_columns)
    #elif role == "VUKOZ-raw_data":
        #copy_command = raw_data_determine_copy_command_with_ignore(file_path, ordered_core_attributes, extra_columns, table_name, ignored_columns)
    else:
        copy_command = determine_copy_command_with_ignore(file_path, ordered_core_attributes, extra_columns, table_name, schema, ignored_columns)

    # ✅ Convert problematic columns BEFORE preparing DataFrame
    numeric_columns = ['year_reserve', 'year_abandonment', "inventory_year", "prp_id", "tree_id", "stem_id", "abundance_value", "epsg_code"]  # Add other columns if needed
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert to numeric (NaN for errors)
            df[col] = df[col].fillna(0).astype(int)  # Fill NaN and convert to int
    
    if schema in ["biodiversity", "site_design"] or table_name in ["biodiversity", "site_design"]:
        df_ready = prepare_biodiversity_dataframe_for_copy(df, ordered_core_attributes, extra_columns, column_mapping, table_name, ignored_columns)
    #elif role == "VUKOZ-raw_data":
        #df_ready = prepare_raw_data_dataframe_for_copy(df, ordered_core_attributes, extra_columns, column_mapping, table_name, ignored_columns)
        #create_raw_data_table(file.name, df.columns, df)
    else:
        # Prepare the DataFrame to include `extended_attributes`
        df_ready = prepare_dataframe_for_copy(df, ordered_core_attributes, extra_columns, column_mapping, table_name, ignored_columns)
    
    # Connect to the database and execute the COPY command
    conn = get_db_connection(role)
    if conn is None:
        return

    try:
        cur = conn.cursor()
        
        if role != "role_superuser_DB_VUK-raw_data":
            # Count rows before insertion
            cur.execute(f"SELECT COUNT(*) FROM public.{table_name};")
            initial_row_count = cur.fetchone()[0]
            print(f"🔹 Rows in `{table_name}` before insertion: {initial_row_count}")
            st.write(f"🔹 Rows in `{table_name}` before insertion: {initial_row_count}")

        # Convert DataFrame to a CSV-like object for COPY
        copy_file_like_object = io.StringIO(df_ready.to_csv(index=False, sep='\t', header=True, na_rep='\\N'))

        # Execute COPY command
        cur.copy_expert(copy_command, copy_file_like_object)

        if role != "role_superuser_DB_VUK-raw_data":
        # Count rows after insertion
            cur.execute(f"SELECT COUNT(*) FROM public.{table_name};")
            final_row_count = cur.fetchone()[0]
            print(f"🔹 Rows in `{table_name}` after insertion: {final_row_count}")
            st.write(f"🔹 Rows in `{table_name}` after insertion: {final_row_count}")

            # Calculate number of rows inserted
            rows_inserted = final_row_count - initial_row_count

            if rows_inserted == len(df_ready):
                success_message = f"✅ Successfully loaded {rows_inserted} rows into `{table_name}`"
            elif rows_inserted > 0:
                success_message = f"⚠️ Only {rows_inserted} out of {len(df_ready)} rows were inserted into `{table_name}`"
            else:
                success_message = f"❌ No rows were inserted into `{table_name}`"

            print(success_message)
            st.write(success_message)

        # Commit transaction
        conn.commit()
        print(f"Data successfully loaded into {table_name}")

    except Exception as e:
        conn.rollback()
        print(f"Error inserting data: {str(e)}")
    
    finally:
        cur.close()
        conn.close()

def truncate_all_tables(role):
    for table in table_mapping:
        table_to_delete = table_mapping.get(table, (None, None, None, None))[0]
        truncate_all_tables = f"""truncate {table_to_delete} CASCADE"""
        restart_numbering = f"""ALTER SEQUENCE {table_to_delete}_record_id_seq RESTART WITH 1;"""
        print(table_to_delete)
        do_query(truncate_all_tables, role, (table_to_delete,))
        do_query(restart_numbering, role, (table_to_delete,))
    truncate_trees = f"""truncate trees"""
    do_query(truncate_trees, role)
    restart_numbering_trees = f"""ALTER SEQUENCE trees_record_id_seq RESTART WITH 1;"""
    do_query(restart_numbering_trees, role)
            
def load_ecological_data_with_copy_command(df, file_path, table_name, ordered_core_attributes, extra_columns, ignored_columns, role):
    """
    Load data using the constructed COPY command, including JSONB data.
    
    Args:
        df (pd.DataFrame): DataFrame with core and extra attributes.
        file_path (str): Path to the file being processed.
        table_name (str): Name of the target database table.
        extra_columns (list): List of columns considered as extra attributes.

    Returns:
        None
    """
    copy_command = determine_copy_command_for_ecology_with_ignore(file_path, ordered_core_attributes, extra_columns, table_name, ignored_columns)
    
    # Prepare the DataFrame to include `extended_attributes`
    df_ready = prepare_dataframe_for_copy(df, ordered_core_attributes, extra_columns, ignored_columns)
    st.write(f'DF to upload:', df_ready.head())
    
    # Connect to the database and execute the COPY command
    conn = get_db_connection(role)
    if conn is None:
        return

    try:
        cur = conn.cursor()
        
        # Use COPY command to insert the data
        copy_file_like_object = io.StringIO(df_ready.to_csv(index=False, sep='\t', header=True, na_rep='\\N'))
        with open('output.csv', 'w', encoding='utf-8') as file:
            file.write(copy_file_like_object.getvalue())
        """    
        #cur.copy_expert(copy_command, copy_file_like_object)
        cur.execute(copy_command, df_ready)
        
        conn.commit()
        print(f"Data successfully loaded into {table_name}")

    except Exception as e:
        conn.rollback()
        print(f"Error inserting data: {str(e)}")
        """
    finally:
        cur.close()
        conn.close()


def foreign_key_mismatch(table_name, unique_current_FK_value, previous_table_name, previous_table_count):    
    # Compare foreign key counts against the primary key count in the previous table
    if previous_table_count != unique_current_FK_value:
        st.warning(
            f"⚠️ Foreign key validation failed: {table_name} has {unique_current_FK_value} unique foreign keys, "
            f"but {previous_table_name} has {previous_table_count} primary keys."
        )
        write_and_log(
            f"⚠️ Foreign key validation failed: {table_name} has {unique_current_FK_value} unique foreign keys, "
            f"but {previous_table_name} has {previous_table_count} primary keys."
        )
        return True
    else:
        st.success(
            f"⚠️ Foreign key validation passed: {table_name} has {unique_current_FK_value} unique foreign keys, "
            f"and {previous_table_name} has also {previous_table_count} primary keys."
        )

def composed_site_id_to_all(role):
    tables_for_composed_site_id_to_all = ["tree_staging", "site_design", "plots"]
    for table_name in tables_for_composed_site_id_to_all:
        composed_site_id_update_in_all_from_sites = f"""
            UPDATE {table_name} t
            SET composed_site_id = s.composed_site_id
            FROM sites s
            WHERE 
                t.site_name = s.reserve_name
                t.wildcard_id = s.wildcard_id; 
                """
        do_query(composed_site_id_update_in_all_from_sites, role)

def sanitize_institute_name(institute):
    # Replace all spaces and hyphens (or multiple spaces) with a single underscore
    sanitized = re.sub(r"[\s\-]+", "_", institute.strip().lower())
    return sanitized

def setup_logins(institute, sanitized_institute, table_name, role):

    force_rls = f"""ALTER TABLE IF EXISTS {table_name} FORCE ROW LEVEL SECURITY;"""
    do_query(force_rls, role)

    grant_select = f"""GRANT SELECT ON TABLE {table_name} TO {sanitized_institute};"""
    do_query(grant_select, role)

    create_rls_policy = f"""
            CREATE POLICY {sanitized_institute}_policy
                ON {table_name}
                FOR SELECT
                TO {sanitized_institute}
                USING (composed_site_id like %s);
                """
    do_query(create_rls_policy, role, (f"%{institute}%",))




"""
# Authenticate with Google Sheets API
def google_sheets_auth():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = Credentials.from_service_account_file("path/to/your-service-account.json", scopes=scopes)
    client = gspread.authorize(credentials)
    return client

def wildcard_db_id(client):
    # Authenticate and load Google Sheets data
    client = google_sheets_auth()
    sheet_url = "https://docs.google.com/spreadsheets/d/1ZThaqGRmRJG7jfbnWNP52C_IKnjHgofU3lR87dgD0o0/edit?gid=0#gid=0"
    sheet = client.open_by_url(sheet_url).worksheet("WILDCARD Meta Data")
    sheet_data = pd.DataFrame(sheet.get_all_records())
    
    # Display initial data for debugging
    write_and_log("Google Sheet data loaded:", sheet_data.head())
    
    # Connect to PostgreSQL and retrieve data
    db_data = do_query(get_wildcard_db_id)
    
    # Display database data for debugging
    write_and_log("Database data loaded:", db_data.head())
    
    # Process and update Google Sheets data
    sheet_data["WILDCARD_DB_ID"] = None
    for i, row in sheet_data.iterrows():
        matched_rows = db_data[db_data["composed_site_id"] == row["composed_site_id"]]
        
        if len(matched_rows) == 1:
            sheet_data.at[i, "WILDCARD_DB_ID"] = matched_rows["record_id"].values[0]
        elif len(matched_rows) > 1:
            st.warning(f"Multiple matches for composed_site_id: {row['composed_site_id']}")
            sheet_data.at[i, "WILDCARD_DB_ID"] = ",".join(matched_rows["record_id"].astype(str))

    # Determine the column letter for "WILDCARD_DB_ID" and update Google Sheets
    col_letter = get_column_letter(sheet.find("WILDCARD_DB_ID").col)
    cell_range = f"{col_letter}2:{col_letter}{len(sheet_data) + 1}"
    sheet.update(cell_range, [[val] for val in sheet_data["WILDCARD_DB_ID"].tolist()])

    st.success("Google Sheets updated successfully!")

"""