import streamlit as st

# Set the title and a brief introduction
st.title("Welcome to the Data Validation and Database Management App")
st.write(
    """
    This application is designed to streamline the process of validating data, uploading files to the database, 
    running integrity tests, and providing useful insights from forestry data.
    """
)

# Section: Key Functionalities
st.header("Key Functionalities")
st.markdown("""
- **Data Validation**: Ensure the uploaded data meets the required format and data integrity standards.
- **Data Exploration**: Explore distinct values, count occurrences, and validate data ranges.
- **Database Operations**: Load data into the database, update records, and perform schema alignment.
- **File Comparison**: Compare different CSV outputs to identify any differences in data from various sources.
- **Helper Functions**: Easily manage core identifiers like `composed_site_id` across different tables.
""")

# Section: Navigation Guide
st.header("Navigation Guide")
st.write(
    """
    Use the sidebar to navigate through the following sections:
    - **Upload and Validation**: Start by uploading your file here to check for data integrity and format compliance.
    - **Database Actions**: For verified data, use this page to upload directly to the database.
    - **Helper Operations**: Access specialized functions for data updates and management within the database.
    - **File Comparison**: Compare your data output files to ensure consistency.
    """
)

# Section: Quick Start
st.header("Quick Start Guide")
st.markdown("""
1. **Upload a Data File**: Go to the **Upload and Validation** section to upload your CSV or text file.
2. **Run Validation Checks**: Perform various checks on data format and value ranges.
3. **Explore Data**: Utilize data exploration tools to understand distinct values and counts.
4. **Database Upload**: After successful validation, head to **Database Actions** to load the data into the database.
5. **Run Helper Functions**: Use helper functions for tasks like updating `composed_site_id` across tables.
6. **Compare Files**: Visit **File Comparison** to identify differences between your CSV outputs.
""")

# Closing Note
st.write(
    """
    *For any assistance, consult the sidebar instructions or reach out to support. Enjoy using the app!*
    """
)
