import streamlit as st

from modules.update_checks import (
    render_result,
    results_json_bytes,
    validate_uploaded_file,
)

st.title("File Upload and Validation")
st.markdown(
    """
Upload one or more TXT files for validation.

Rules:
- filename must follow `DATATYPE_INSTITUTE_TABLE(_other).txt`
- allowed table tokens: `design`, `plots`, `trees`, `cwd`, `metadata`
- file must be UTF-8, TAB-delimited
- empty values should be `\\N`
- `UPLOAD` files must not contain any `*_record_id`
- `UPDATE` files must contain the correct `*_record_id` plus one or more changed columns
"""
)

uploaded_files = st.file_uploader(
    "Upload file(s) to validate",
    type=["txt"],
    accept_multiple_files=True,
)

if st.button("Run tests", type="primary"):
    if not uploaded_files:
        st.warning("Please upload at least one file.")
        st.stop()

    all_results = []

    for uploaded_file in uploaded_files:
        st.divider()
        st.header(uploaded_file.name)

        try:
            result = validate_uploaded_file(uploaded_file)
            render_result(result)
            all_results.append(result)
        except Exception as exc:
            st.error(f"Unexpected error while validating {uploaded_file.name}: {exc}")

    if all_results:
        st.download_button(
            label="Download results as JSON",
            data=results_json_bytes(all_results),
            file_name="validation_results.json",
            mime="application/json",
        )