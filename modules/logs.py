import streamlit as st
import logging

# a helper function to write and log
def write_and_log(message):
    st.write(message)          # Write to Streamlit UI
    logging.info(message) 

import streamlit as st
import time

def do_action_after_role_check(role, do_action, *args, **kwargs):
    # Initialize session state variables if they don't exist
    if 'ready_to_truncate' not in st.session_state:
        st.session_state['ready_to_truncate'] = False
    if 'confirmation_time' not in st.session_state:
        st.session_state['confirmation_time'] = None

    # Helper to reset after timeout
    def reset_confirmation_if_needed():
        if st.session_state['confirmation_time']:
            elapsed = time.time() - st.session_state['confirmation_time']
            if elapsed > 30:  # Reset after 30 seconds
                st.session_state['ready_to_truncate'] = False
                st.session_state['confirmation_time'] = None
                st.info("🔄 Confirmation expired. Please request again.")

    reset_confirmation_if_needed()

    if role == 'vukoz':
        if not st.session_state['ready_to_truncate']:
            if st.button("⚡ Request truncation (Sensitive Operation)"):
                st.session_state['ready_to_truncate'] = True
                st.session_state['confirmation_time'] = time.time()
                st.experimental_rerun()
        else:
            st.warning("⚠️ You are using the PRODUCTION role 'vukoz'. Confirm carefully.")
            confirm = st.checkbox("✅ Yes, I understand the risk", key="confirm_truncate_prod")
            if confirm:
                st.success("✅ You confirmed. Now click to execute the action.")
                if st.button("🚀 Confirm and RUN"):
                    do_action(*args, **kwargs)
                    st.success("🎯 Action successfully executed!")
                    st.session_state['ready_to_truncate'] = False
                    st.session_state['confirmation_time'] = None
            else:
                st.info("☝️ Please confirm first before proceeding.")
    else:
        if st.button("✅ Run the action (Non-prod role)"):
            do_action(*args, **kwargs)
