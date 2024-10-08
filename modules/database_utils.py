import streamlit as st
import psycopg2
import os
import io
import logging
from datetime import datetime
import json
from modules.logs import write_and_log
from modules.dataframe_actions import prepare_dataframe_for_copy, determine_copy_command
    
# Hardcoded PostgreSQL database connection details
#DB_HOST = '147.251.253.245'
#DB_PORT = '5432'
#DB_NAME = 'EuFoRIa_trees_db'
#DB_USER = 'vukoz'
#DB_PASSWORD = 'W0Ja3l9WbabOxWatduegk6akPTJg9kZi6JxaKuWIjncX7AK0ct2vYaL9kDExoVjH'

DB_HOST = '147.251.253.245'
DB_PORT = '5432'
DB_NAME = 'trees_db'
DB_USER = 'postgres'
DB_PASSWORD = 'eKRG7uA1Wb56yYDOdbRua9kFrQBDVevXckoYQA3YGcL8dbAEF8nu53gXMqOoZXlI'


# Set up logging
log_folder = 'logs'
os.makedirs(log_folder, exist_ok=True)
log_file = os.path.join(log_folder, 'data_copy.log')

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("Connection to the database was successful!")
        return conn
    except Exception as e:
        print("An error occurred while connecting to the database:", str(e))
        return None

def load_data_with_copy_command(df, file_path, table_name, ordered_core_attributes, extra_columns):
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
    copy_command = determine_copy_command(file_path, ordered_core_attributes, extra_columns, table_name)
    
    # Prepare the DataFrame to include `extended_attributes`
    df_ready = prepare_dataframe_for_copy(df, ordered_core_attributes, extra_columns)
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

# Function to update unique_plot_id in tree_staging
def update_unique_plot_id():
    conn = get_db_connection()
    if conn is None:
        return  # If connection fails, do nothing

    try:
        cur = conn.cursor()
    
        update_data_query = """
        UPDATE tree_staging t
        SET unique_plot_id = p.record_id
        FROM plots p
        WHERE 
            t.wildcard_id = p.wildcard_id
            AND t.inventory_year = p.inventory_year
            AND (t.lpi_id = p.lpi_id OR t.spi_id = p.spi_id)
            AND p.unique_plot_id <= 8258;
        """
    
        cur.execute(update_data_query)
        conn.commit()
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
            cur.close()
            conn.close()

# Function to move data from tree_staging to tree
def move_data_to_tree():
    conn = get_db_connection()
    if conn is None:
        return  # If connection fails, do nothing

    try:
        cur = conn.cursor()
    
        move_data_query = """
        INSERT INTO public.tree (composed_site_id, unique_plot_id, tree_id, stem_id, inventory_year, consistent_id, life, position, integrity, height, date, full_scientific, dbh, decay, diameter_1, diameter_2, length, geom, extended_attributes, institute, wildcard_id)
        SELECT 
            composed_site_id, unique_plot_id, tree_id, stem_id, inventory_year, consistent_id, life, position, integrity, height, date, full_scientific, dbh, decay, diameter_1, diameter_2, length, geom, extended_attributes, institute, wildcard_id
        FROM
            public.tree_staging;
        """
    
        cur.execute(move_data_query)
        conn.commit()
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
            cur.close()
            conn.close()

# Function to move data from tree_staging to tree
def composed_site_id_tree():
    conn = get_db_connection()
    if conn is None:
        return  # If connection fails, do nothing

    try:
        cur = conn.cursor()
    
        composed_site_id_data_query = """
        UPDATE tree
        SET composed_site_id = 
        CASE 
        WHEN wildcard_id = '1093' THEN 'FVA-BW__37__Allgaier Riedle__1093'
        WHEN wildcard_id = '1094' THEN 'FVA-BW__37__Allgaier Riedle__1094'
        WHEN wildcard_id = '990' THEN 'FVA-BW__588__Altlochkar-Rotwasser__990'
        WHEN wildcard_id = '971' THEN 'FVA-BW__434__Altspock__971'
        WHEN wildcard_id = '1012' THEN 'FVA-BW__69__Bahnholz__1012'
        WHEN wildcard_id = '792' THEN 'FVA-BW__677__Baldeck__792'
        WHEN wildcard_id = '976' THEN 'FVA-BW__500__Barlochkar__976'
        WHEN wildcard_id = '996' THEN 'FVA-BW__610__Battert__996'
        WHEN wildcard_id = '1095' THEN 'FVA-BW__66__Bayrischer Schlag__1095'
        WHEN wildcard_id = '907' THEN 'FVA-BW__2__Bechtaler Wald__907'
        WHEN wildcard_id = '1097' THEN 'FVA-BW__61__Birkenkopf__1097'
        WHEN wildcard_id = '784' THEN 'FVA-BW__666__Bosler__784'
        WHEN wildcard_id = '984' THEN 'FVA-BW__546__Bruchsaler Bruch__984'
        WHEN wildcard_id = '1099' THEN 'FVA-BW__36__Brunnenholzried__1099'
        WHEN wildcard_id = '1100' THEN 'FVA-BW__36__Brunnenholzried__1100'
        WHEN wildcard_id = '985' THEN 'FVA-BW__552__Buch__985'
        WHEN wildcard_id = '972' THEN 'FVA-BW__436__Buigen__972'
        WHEN wildcard_id = '989' THEN 'FVA-BW__585__Burghard__989'
        WHEN wildcard_id = '914' THEN 'FVA-BW__11__Conventwald__914'
        WHEN wildcard_id = '437' THEN 'FVA-BW__770__Donntal-Lange Steige__437'
        WHEN wildcard_id = '670' THEN 'FVA-BW__770__Donntal-Lange Steige__670'
        WHEN wildcard_id = '789' THEN 'FVA-BW__674__Drackenberg__789'
        WHEN wildcard_id = '975' THEN 'FVA-BW__499__Eiberg__975'
        WHEN wildcard_id = '1104' THEN 'FVA-BW__8__Faulbach__1104'
        WHEN wildcard_id = '1105' THEN 'FVA-BW__8__Faulbach__1105'
        WHEN wildcard_id = '964' THEN 'FVA-BW__62__Feldseewald__964'
        WHEN wildcard_id = '96405ASF' THEN 'FVA-BW__62__Feldseewald__96405ASF'
        WHEN wildcard_id = '1106' THEN 'FVA-BW__7__Fluh__1106'
        WHEN wildcard_id = '1107' THEN 'FVA-BW__7__Fluh__1107'
        WHEN wildcard_id = '967' THEN 'FVA-BW__65__Fohlenhaus__967'
        WHEN wildcard_id = '791' THEN 'FVA-BW__676__Fohrenberg__791'
        WHEN wildcard_id = '1062' THEN 'FVA-BW__1039__Geschwender Halde__1062'
        WHEN wildcard_id = '754' THEN 'FVA-BW__744__Gieselwald-Heumacher__754'
        WHEN wildcard_id = '755' THEN 'FVA-BW__744__Gieselwald-Heumacher__755'
        WHEN wildcard_id = '951' THEN 'FVA-BW__49__Grosse Tannen__951'
        WHEN wildcard_id = '933' THEN 'FVA-BW__30__Grubenhau__933'
        WHEN wildcard_id = '997' THEN 'FVA-BW__611__Haldenwald__997'
        WHEN wildcard_id = '794' THEN 'FVA-BW__681__Hochberg-Amseltal__794'
        WHEN wildcard_id = '1143' THEN 'FVA-BW__52__Hofstatt__1143'
        WHEN wildcard_id = '1144' THEN 'FVA-BW__52__Hofstatt__1144'
        WHEN wildcard_id = '1003' THEN 'FVA-BW__625__Hohberg__1003'
        WHEN wildcard_id = '977' THEN 'FVA-BW__504__Hollmuth__977'
        WHEN wildcard_id = '680' THEN 'FVA-BW__781__Jorgenbuhl - Geichenbuch__680'
        WHEN wildcard_id = '743' THEN 'FVA-BW__781__Jorgenbuhl - Geichenbuch__743'
        WHEN wildcard_id = '786' THEN 'FVA-BW__671__Kaltental__786'
        WHEN wildcard_id = '969' THEN 'FVA-BW__68__Kesselgraben__969'
        WHEN wildcard_id = '1152' THEN 'FVA-BW__508__Klebwald__1152'
        WHEN wildcard_id = '1153' THEN 'FVA-BW__508__Klebwald__1153'
        WHEN wildcard_id = '1141' THEN 'FVA-BW__42__Kleiner Imberg__1141'
        WHEN wildcard_id = '1142' THEN 'FVA-BW__42__Kleiner Imberg__1142'
        WHEN wildcard_id = '1130' THEN 'FVA-BW__27__Kohltal__1130'
        WHEN wildcard_id = '1131' THEN 'FVA-BW__27__Kohltal__1131'
        WHEN wildcard_id = '922' THEN 'FVA-BW__19__Lindach__922'
        WHEN wildcard_id = '994' THEN 'FVA-BW__603__Maienberg__994'
        WHEN wildcard_id = '987' THEN 'FVA-BW__580__Mietholz__987'
        WHEN wildcard_id = '787' THEN 'FVA-BW__672__Nagelesfelsen-Eichhalde__787'
        WHEN wildcard_id = '1109' THEN 'FVA-BW__9__Napf__1109'
        WHEN wildcard_id = '1110' THEN 'FVA-BW__9__Napf__1110'
        WHEN wildcard_id = '1111' THEN 'FVA-BW__9__Napf__1111'
        WHEN wildcard_id = '1059' THEN 'FVA-BW__1036__Nollenwald__1059'
        WHEN wildcard_id = '995' THEN 'FVA-BW__609__Ofenberg__995'
        WHEN wildcard_id = '1112' THEN 'FVA-BW__43__Pfannenberg__1112'
        WHEN wildcard_id = '1113' THEN 'FVA-BW__43__Pfannenberg__1113'
        WHEN wildcard_id = '1147' THEN 'FVA-BW__56__Pfrunger-Burgweiler Ried__1147'
        WHEN wildcard_id = '1132' THEN 'FVA-BW__28__Rabensteig__1132'
        WHEN wildcard_id = '1133' THEN 'FVA-BW__28__Rabensteig__1133'
        WHEN wildcard_id = '779' THEN 'FVA-BW__28__Rabensteig__779'
        WHEN wildcard_id = '1139' THEN 'FVA-BW__40__Reisinsel__1139'
        WHEN wildcard_id = '1140' THEN 'FVA-BW__40__Reisinsel__1140'
        WHEN wildcard_id = '993' THEN 'FVA-BW__602__Riedis__993'
        WHEN wildcard_id = '99322ASF' THEN 'FVA-BW__602__Riedis__99322ASF'
        WHEN wildcard_id = '946' THEN 'FVA-BW__44__Rotenacker__946'
        WHEN wildcard_id = '992' THEN 'FVA-BW__600__Rottlerwald__992'
        WHEN wildcard_id = '1001' THEN 'FVA-BW__622__Schachen__1001'
        WHEN wildcard_id = '956' THEN 'FVA-BW__54__Scheibenfelsen__956'
        WHEN wildcard_id = '923' THEN 'FVA-BW__20__Schlierbach__923'
        WHEN wildcard_id = '1134' THEN 'FVA-BW__32__Schmalegger Tobel__1134'
        WHEN wildcard_id = '1135' THEN 'FVA-BW__32__Schmalegger Tobel__1135'
        WHEN wildcard_id = '1126' THEN 'FVA-BW__17__Schnapsried__1126'
        WHEN wildcard_id = '1127' THEN 'FVA-BW__17__Schnapsried__1127'
        WHEN wildcard_id = '983' THEN 'FVA-BW__543__Schnepfenmoos__983'
        WHEN wildcard_id = '970' THEN 'FVA-BW__432__Schuslersklinge__970'
        WHEN wildcard_id = '1114' THEN 'FVA-BW__5__Schwarzahalden__1114'
        WHEN wildcard_id = '1115' THEN 'FVA-BW__5__Schwarzahalden__1115'
        WHEN wildcard_id = '3' THEN 'FVA-BW__735__Schwetzinger Hardt__3'
        WHEN wildcard_id = '4' THEN 'FVA-BW__735__Schwetzinger Hardt__4'
        WHEN wildcard_id = '598' THEN 'FVA-BW__735__Schwetzinger Hardt__598'
        WHEN wildcard_id = '801' THEN 'FVA-BW__735__Schwetzinger Hardt__801'
        WHEN wildcard_id = '998' THEN 'FVA-BW__612__Siebter Fuss__998'
        WHEN wildcard_id = '1000' THEN 'FVA-BW__621__Siedigkopf__1000'
        WHEN wildcard_id = '966' THEN 'FVA-BW__64__Silbersandgrube__966'
        WHEN wildcard_id = '921' THEN 'FVA-BW__18__Stammberg__921'
        WHEN wildcard_id = '988' THEN 'FVA-BW__584__Steinriegelhang__988'
        WHEN wildcard_id = '955' THEN 'FVA-BW__53__Stimpfach__955'
        WHEN wildcard_id = '537' THEN 'FVA-BW__772__Stoffelberg/Pfullinger Berg__537'
        WHEN wildcard_id = '980' THEN 'FVA-BW__510__Sturmlesloch__980'
        WHEN wildcard_id = '944' THEN 'FVA-BW__41__Taubergiesen__944'
        WHEN wildcard_id = '950' THEN 'FVA-BW__48__Teufelsloch__950'
        WHEN wildcard_id = '961' THEN 'FVA-BW__59__Teufelsries__961'
        WHEN wildcard_id = '1004' THEN 'FVA-BW__627__Vorsenz__1004'
        WHEN wildcard_id = '986' THEN 'FVA-BW__553__Waldecker Schloswald__986'
        WHEN wildcard_id = '1124' THEN 'FVA-BW__15__Waldmoor-Torfstich__1124'
        WHEN wildcard_id = '1125' THEN 'FVA-BW__15__Waldmoor-Torfstich__1125'
        WHEN wildcard_id = '1118' THEN 'FVA-BW__1__Weisweiler Rheinwald__1118'
        WHEN wildcard_id = '1119' THEN 'FVA-BW__1__Weisweiler Rheinwald__1119'
        WHEN wildcard_id = '1002' THEN 'FVA-BW__623__Wettenberger Ried__1002'
        WHEN wildcard_id = '1122' THEN 'FVA-BW__14__Wildseemoor__1122'
        WHEN wildcard_id = '1123' THEN 'FVA-BW__14__Wildseemoor__1123'
        WHEN wildcard_id = '1149' THEN 'FVA-BW__57__Zimmeracker__1149'
        WHEN wildcard_id = '1150' THEN 'FVA-BW__57__Zimmeracker__1150'
        WHEN wildcard_id = '1151' THEN 'FVA-BW__57__Zimmeracker__1151'
        WHEN wildcard_id = '1120' THEN 'FVA-BW__10__Zweribach__1120'
    END;
        """
    
        cur.execute(composed_site_id_data_query)
        conn.commit()
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
            cur.close()
            conn.close()
