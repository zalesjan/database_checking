import streamlit as st
from modules.database_utils import do_query, show_counts_of_all
from modules.logs import write_and_log

# Set the title and a brief introduction
st.title("Wildcard/EuFoRIA Data Validation")
st.write(
    """
    This application validates data, running data integrity and plausibility tests and aids with uploading data to the database.
    """
)

# Section: Key Functionalities
st.header("Key Functionalities")
st.markdown("""
- **Data Validation**: Ensure that your data contain all obligatory fields and meet other data integrity standards.
- **Data Exploration**: Explore distinct values, count occurrences, and validate data ranges.
- **Plausibility tests**: Check your data for plausibility - dbh reduction, state/position inversions, species chenges.- **Database Operations**: Load data into the database, update records, and perform schema alignment (restricted).
- **File Comparison**: Compare CSV files to identify any differences between original and database data.
- **Helper Functions**: Easily manage core identifiers like `composed_site_id` across different tables (restricted).
""")

st.header("Rule no1:")
st.write("**USE TXT FILES** (NOT EXCEL) + **ADHERE TO NAMING CONVENTION** (DESCRIBED BELOW) WHEN UPLOADING YOUR FILE")
st.write(
    """
    **Naming convention**: 
    - include the key word for the table you are uploading
    - **Key words**:design, plots, standing, lying, cwd, metadata
    - Example: xxdesignxx.txt, xxplotsxx.txt, xxstandingxx, e.g. VUKOZ_design.txt, or VUKOZ_plots.txt
    """
)

# Section: Navigation Guide
st.header("Navigation Guide")
st.write(
    """
    Use the sidebar to navigate through the following sections:
    - **Validation and Exploration**: Upload your file to check it for data ranges/values and columns compliance, explore distinct values, count occurrences.
    - **Plausibility tests**: Check your data for plausibility - dbh reduction, state/position inversions, species chenges.
    - **File Comparison**: Compare your data output files to ensure consistency.
    - **Database Actions**: Use this page to upload to the database and for management of the database (restricted).
    """
)

# Section: Quick Start
st.header("Quick Start Guide")
st.markdown("""
1. **Upload a txt File**: Go to the **Upload and Validation** section to upload your CSV or text file.
2. **Run Validation Checks**: Perform checks on data format and value ranges.
3. **Explore Data**: Utilize data exploration tools to understand distinct values and counts.
4. **Database Upload**: After successful validation, contact Jan Zálešák (zalesak@vukoz.cz) to load your data to DB.
5. **Compare Files**: contact VUK to identify differences between your original data and DB.
""")

# Closing Note
st.header("Contacts")
st.write(
    """
    *If you believe you need institutional or special access to the database, contact Jan Zálešák (zalesak@vukoz.cz).
    *For any assistance, contact Jan Zálešák (zalesak@vukoz.cz) for support.* 
    *Enjoy using the app!*
    """
)

if st.button("show how many sites, plots and trees we have"):           
    _, show_counts_of_all_df = do_query(show_counts_of_all, role='vukoz')
    if show_counts_of_all_df is not None:
        # Define the filename
        show_counts_of_all_file = f"temp_dir/show_counts_of_all_file.csv"
        # Save the DataFrame as a CSV file
        show_counts_of_all_df.to_csv(show_counts_of_all_file, index=False)
        write_and_log("This many sites, plots and trees we have:")
        st.dataframe(show_counts_of_all_df)  # Display the result as a DataFrame