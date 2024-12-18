import streamlit as st

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
- **Data Validation**: Ensure the uploaded data contain all obligatory fields and meet other data integrity standards.
- **Data Exploration**: Explore distinct values, count occurrences, and validate data ranges.
- **Database Operations**: Load data into the database, update records, and perform schema alignment (restricted).
- **File Comparison**: Compare CSV files to identify any differences between original and databse data.
- **Helper Functions**: Easily manage core identifiers like `composed_site_id` across different tables (restricted).
""")

st.header("Rule no1:")
st.write("**USE TXT FILES** (NOT EXCEL) + **ADHERE TO NAMING CONVENTION** DESCRIBED BELOW WHEN UPLOADING YOUR FILE")
st.write(
    """
    **Naming convention**: 
    - include the key word for the table you are uploading
    - **Key words**:design, plots, standing, lying, cwd, metadata
    - Example: xxdesignxx.txt, xxplotsxx.txt, xxstandingxx, e.g. VUKOZ_design.txt, or zofin_reserve_plots.txt
    """
)

# Section: Navigation Guide
st.header("Navigation Guide")
st.write(
    """
    Use the sidebar to navigate through the following sections:
    - **Upload and Validation**: Start by uploading your file and check for data ranges/values and columns compliance.
    - **Database Actions**: For verified data, use this page to upload directly to the database (restricted).
    - **Helper Operations**: Access specialized functions for data updates and management within the database (restricted).
    - **File Comparison**: Compare your data output files to ensure consistency.
    """
)

# Section: Quick Start
st.header("Quick Start Guide")
st.markdown("""
1. **Upload a Data File**: Go to the **Upload and Validation** section to upload your CSV or text file.
2. **Run Validation Checks**: Perform checks on data format and value ranges.
3. **Explore Data**: Utilize data exploration tools to understand distinct values and counts.
4. **Database Upload**: After successful validation, contact Jan Zálešák (zalesak@vukoz.cz) to load your data to DB.
5. **Compare Files**: contact Vokoz to identify differences between your original data and DB.
""")

# Closing Note
st.header("Contacts")
st.write(
    """
    *If you believe you need insitutional or special access to the database, contact Jan Zálešák (zalesak@vukoz.cz).
    *For any assistance, contact Jan Zálešák (zalesak@vukoz.cz) for support.* 
    *Enjoy using the app!*
    """
)
