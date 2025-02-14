import streamlit as st
import psycopg2
import os
import io
import logging
from datetime import datetime
import pandas as pd
from modules.logs import write_and_log
from modules.dataframe_actions import determine_copy_command_for_ecology_with_ignore, determine_copy_command_with_ignore, prepare_dataframe_for_copy

#queries used in helper operations
get_wildcard_db_id = "SELECT composed_site_id, record_id FROM public.sites"

tree_staging_id =f"""
        UPDATE tree_staging t
        SET unique_plot_id = p.record_id
        FROM plots p
        WHERE 
            t.composed_site_id = p.composed_site_id
            AND t.inventory_year = p.inventory_year
            AND (t.lpi_id = p.lpi_id OR t.spi_id = p.spi_id)
			AND p.inventory_id = t.inventory_id
			AND t.circle_no = p.circle_no;
        """
plots_id =f"""
        UPDATE plots p
        SET unique_site_design_id = d.record_id
        FROM site_design d
        WHERE 
            p.composed_site_id = d.composed_site_id
            AND p.inventory_year = d.inventory_year
            AND (p.lpi_id = d.lpi_id OR p.spi_id = d.spi_id)
			AND p.inventory_id = d.inventory_id
			AND p.circle_no = d.circle_no
			and d.composed_site_id like %s;
        """
site_design =f"""
        UPDATE site_design d
		SET unique_site_id = s.record_id
		FROM sites s
		WHERE 
			d.composed_site_id = s.composed_site_id
            and s.composed_site_id like %s;
        """
move_data_to_tree = """
        INSERT INTO public.tree (composed_site_id, unique_plot_id, tree_id, stem_id, piece_id, inventory_year, consistent_id, life, position, integrity, height, date, full_scientific, dbh, decay, diameter_1, diameter_2, length, geom, extended_attributes, institute, wildcard_sub_id, circle_no)
        SELECT 
            composed_site_id, unique_plot_id, tree_id, stem_id, piece_id, inventory_year, consistent_id, life, position, integrity, height, date, full_scientific, dbh, decay, diameter_1, diameter_2, length, geom, extended_attributes, institute, wildcard_sub_id, circle_no
        FROM
            public.tree_staging;
        """

composed_site_id_sites = """
        UPDATE sites
        SET composed_site_id = CONCAT(institute, '__', site_id, '__', reserve_name, '__', wildcard_id); 
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

def get_db_connection():
    #["postgres"], ["vukoz"]
    role = "postgres_EuFoRIa_trees_db"
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

def do_query(query, placeholder=None):
    conn = get_db_connection()
    if conn is None:
        st.error("Database connection failed.")
        return None  # If connection fails, return None

    try:
        # Execute query and fetch results
        cur = conn.cursor()
        if placeholder is not None:
            cur.execute(query, (f"%{placeholder}%",))
        else:
            cur.execute(query)
        
        if "dbh < d.standing_alive_threshold" in query:
            # Fetch all results
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]  # Get column names

            # Create a DataFrame for easy display
            result_df = pd.DataFrame(rows, columns=columns)
            conn.commit()
            return result_df  # Return the DataFrame with results
        else:
            conn.commit()

    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        return None
    finally:
        cur.close()
        conn.close()

def load_data_with_copy_command(df, file_path, table_name, ordered_core_attributes, extra_columns, ignored_columns, column_mapping):
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
    copy_command = determine_copy_command_with_ignore(file_path, ordered_core_attributes, extra_columns, table_name, ignored_columns)

    # Prepare the DataFrame to include `extended_attributes`
    df_ready = prepare_dataframe_for_copy(df, ordered_core_attributes, extra_columns, column_mapping, ignored_columns)
    st.write(f'DF to upload:', df_ready.head())
    # Connect to the database and execute the COPY command
    conn = get_db_connection()
    if conn is None:
        return

    try:
        cur = conn.cursor()
        
        # Use COPY command to insert the data
        copy_file_like_object = io.StringIO(df_ready.to_csv(index=False, sep='\t', header=True, na_rep='\\N'))
        cur.copy_expert(copy_command, copy_file_like_object)

        conn.commit()
        print(f"Data successfully loaded into {table_name}")

    except Exception as e:
        conn.rollback()
        print(f"Error inserting data: {str(e)}")
    
    finally:
        cur.close()
        conn.close()

def load_ecological_data_with_copy_command(df, file_path, table_name, ordered_core_attributes, extra_columns, ignored_columns):
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
    conn = get_db_connection()
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
        
def composed_site_id_to_all():
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
        do_query(composed_site_id_update_in_all_from_sites)
    
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