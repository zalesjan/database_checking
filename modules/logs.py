import streamlit as st
import logging

# a helper function to write and log
def write_and_log(message):
    st.write(message)          # Write to Streamlit UI
    logging.info(message) 