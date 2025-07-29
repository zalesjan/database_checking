import streamlit as st
import pandas as pd
import json
import os
import re
from modules.logs import write_and_log
from modules.validate_files_module import file_comparison
import logging
    
logging.basicConfig(
    filename='logs.log',  # Specify your log file name
    level=logging.INFO,  # Set the logging level to INFO or WARNING
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the format of log messages
)
logging.info('Logging setup is complete.')

# Page Name
st.title("4_File_Comparison")

#FILE COMPARISON ("QUERy TESTING")
# Initialize session state for files if not already done
if "file_1" not in st.session_state:
    st.session_state["file_1"] = None
if "file_2" not in st.session_state:
    st.session_state["file_2"] = None

# File uploaders for two CSV files with state saving
st.session_state["file_1"] = st.file_uploader("Upload first CSV file", type=["csv", "txt", "xls", "xlsx"])
st.session_state["file_2"] = st.file_uploader("Upload second CSV file", type=["csv", "txt", "xls", "xlsx"])

if st.button("Compare DB output to provider output") and st.session_state["file_1"] and st.session_state["file_2"]:
    # Proceed only if both files are uploaded
    file_1 = st.session_state["file_1"]
    file_2 = st.session_state["file_2"]

    merged_df = file_comparison(st.session_state["file_1"], st.session_state["file_2"])
    write_and_log("Merged DataFrame with Differences:")
    write_and_log(merged_df)