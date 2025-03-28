import streamlit as st
import logging

# a helper function to write and log
def write_and_log(message):
    st.write(message)          # Write to Streamlit UI
    logging.info(message) 

def do_action_after_role_check(role, do_action, *args, **kwargs):
    if role == 'moje':
        st.session_state['truncate_requested'] = True
    else:
        do_action(*args, **kwargs)
                
    # If truncation was requested in prod, ask for confirmation
    if st.session_state.get('truncate_requested', False) and role == 'moje':
        st.warning("⚠️ You are using the production role 'moje'. Please confirm you want to proceed.")
        confirm = st.checkbox("Yes, I understand and want to proceed", key="confirm_truncate_prod")


        if confirm:
            # Proceed with truncation
            do_action(*args, **kwargs)
            st.session_state['truncate_requested'] = False