import pandas as pd
import streamlit as st

from modules.database_utils import password_check, select_role
from modules.upload_utils import process_single_file, sort_uploaded_files
from modules.validate_files_module import (
    distinct_values_with_counts,
    value_counts_for_each_distinct_value,
)
from modules.update_checks import (
    validate_uploaded_file,
    render_result,
    try_read_csv_with_fallback,
    normalise_dataframe,
)


st.title("7_Update_Uploader")
st.markdown(
    """
This page is the new upload flow.

1. Upload one or more TAB-delimited TXT files.
2. Each file is validated with `update_checks.py`.
3. Upload is enabled only if all files pass.
4. Files are then inserted/updated directly in PostgreSQL.
"""
)


def validate_for_this_page(uploaded_file):
    result = validate_uploaded_file(uploaded_file)

    parsed = result.parsed_name or {}
    if parsed and parsed.get("action") not in {"upload", "update"}:
        result.add_issue(
            "ERROR",
            "filename.unsupported_action",
            "This page only supports UPLOAD_* and UPDATE_* files.",
        )

    return result


def get_columns_for_exploration(result):
    excluded = {"geom"}
    columns = []
    for issue in result.issues:
        if issue.column and issue.column not in excluded:
            columns.append(issue.column)
    return sorted(set(columns))


if password_check():
    role = select_role()

    uploaded_files = st.file_uploader(
        "Upload TAB-delimited TXT files",
        type=["txt"],
        accept_multiple_files=True,
    )

    if uploaded_files:
        sorted_files = sort_uploaded_files(uploaded_files)

        st.markdown("### Processing order")
        st.write([uploaded_file.name for uploaded_file in sorted_files])

        if st.button("Run validation"):
            all_passed = True
            validation_summary = []

            for uploaded_file in sorted_files:
                st.markdown("---")
                result = validate_for_this_page(uploaded_file)
                render_result(result)

                df, _, load_error = try_read_csv_with_fallback(uploaded_file)
                if not load_error and df is not None and not df.empty:
                    df = normalise_dataframe(df)
                    columns_for_exploration = get_columns_for_exploration(result)

                    if columns_for_exploration:
                        st.markdown("#### Values present in flagged columns")
                        distinct_values_with_counts(df, columns_for_exploration)
                        value_counts_for_each_distinct_value(df, columns_for_exploration)

                validation_summary.append(
                    {
                        "file_name": uploaded_file.name,
                        "passed": result.passed,
                        "action": result.parsed_name.get("action"),
                        "table_token": result.parsed_name.get("table_token"),
                        "db_table": result.parsed_name.get("db_table"),
                        "rows": result.stats.get("n_rows"),
                        "columns": result.stats.get("n_columns"),
                        "issues": len(result.issues),
                    }
                )

                if not result.passed:
                    all_passed = False

            st.session_state["update_uploader_all_passed"] = all_passed
            st.session_state["update_uploader_validation_summary"] = validation_summary

            st.markdown("### Validation summary")
            st.dataframe(pd.DataFrame(validation_summary), hide_index=True, width="stretch")

            if all_passed:
                st.success("All files passed validation. You can now upload them.")
            else:
                st.error("At least one file failed validation. Fix the files before uploading.")

        if st.session_state.get("update_uploader_validation_summary"):
            st.markdown("### Last validation summary")
            st.dataframe(
                pd.DataFrame(st.session_state["update_uploader_validation_summary"]),
                hide_index=True,
                width="stretch",
            )

        if st.button("Upload validated files"):
            validation_results = [validate_for_this_page(uploaded_file) for uploaded_file in sorted_files]
            failed_results = [result for result in validation_results if not result.passed]

            if failed_results:
                st.error("Upload stopped because at least one file did not pass validation.")
                for failed in failed_results:
                    st.markdown("---")
                    render_result(failed)
            else:
                outcomes = []
                for uploaded_file in sorted_files:
                    outcome = process_single_file(uploaded_file, role)
                    outcomes.append(outcome.to_dict())

                st.success("Upload finished.")

                outcome_df = pd.DataFrame(outcomes)[
                    [
                        "file_name",
                        "action",
                        "table_token",
                        "db_table",
                        "original_rows_in_df",
                        "rows_prepared_for_db",
                        "rows_inserted",
                        "rows_updated",
                        "rows_skipped_or_unchanged",
                        "rows_with_missing_target_record_id",
                    ]
                ]

                st.markdown("### Upload summary")
                st.dataframe(outcome_df, hide_index=True, width="stretch")

                with st.expander("Ignored columns and extended attributes details"):
                    st.json(outcomes, expanded=False)