import streamlit as st

import json
from pathlib import Path
import streamlit as st

EXPECTATIONS_DIR = Path(__file__).resolve().parent / "expectations"




def load_expectations(expectations_dir: Path) -> dict:
    schemas = {}

    allowed_files = ["design.json", "plots.json", "trees.json", "cwd.json", "metadata.json"]

    # for json_path in sorted(expectations_dir.glob("*.json")):
    for file_name in allowed_files:
        json_path = expectations_dir / file_name
        with open(json_path, "r", encoding="utf-8") as f:
            schema = json.load(f)

        schemas[schema["table_token"]] = schema

    return schemas


def get_upload_mandatory_columns(schema: dict) -> list[str]:
    return [
        col_name
        for col_name, col_def in schema["columns"].items()
        if col_def.get("mandatory_upload", False)
    ]


def get_update_required_columns(schema: dict) -> list[str]:
    # Based on your current JSON structure, UPDATE requires the record_id
    # and then whichever columns the user wants to change.
    cols = [schema["update_record_id"]]

    extra_required = [
        col_name
        for col_name, col_def in schema["columns"].items()
        if col_def.get("mandatory_update", False)
    ]

    cols.extend(extra_required)
    return cols


def get_geom_rules(schema: dict) -> dict:
    geom_rules = {}
    for col_name, col_def in schema["columns"].items():
        if col_def.get("dtype") == "ewkt":
            geom_rules[col_name] = col_def.get("allowed_srid")
    return geom_rules


def format_srid_help(allowed_srids: list[int] | None) -> str:
    if not allowed_srids:
        return "No SRID restriction defined."
    labels = [str(s) for s in allowed_srids]
    if 0 in allowed_srids:
        labels = [("0 (local geometry)" if s == "0" else s) for s in labels]
    return ", ".join(labels)

EXPECTATIONS = load_expectations(EXPECTATIONS_DIR)

# Set the title and a brief introduction
st.title("Wildcard/EuFoRIA Data Validation")
st.write(
"""
This application validates data for uploading data to the database.
"""
)

# Section: Key Functionalities
st.header("Key functionalities")
st.markdown(
"""
- **Data Validation**: Ensure that your data contain all obligatory fields and meet other data integrity standards.
""")

st.header("DB upload workflow:")
st.write(
"""
1. Upload data for the upload check through the File Validation page.
2. Pass the checks.
3. Email the data to us, together with your queries (applies only to new data). You can email it to whoever you were in touch with and cc Magda.
4. We recheck and upload the data.
""")

st.header("Upload guidelines:")
st.write("**USE TAB-DELIMITED TXT FILES** (NOT EXCEL) + **ADHERE TO NAMING CONVENTION** (DESCRIBED BELOW) WHEN UPLOADING YOUR FILE")
st.write(
"""
**Naming convention**: 
- **File name**: DATATYPE_INSTITUTE_TABLE(_other).txt
-  e.g. UPLOAD_VUK_TREES.txt or UPLOAD_VUK_TREES_20260216.txt
- **DATATYPE**: UPLOAD or UPDATE.
- **INSTITUTE**: the institute's acronym used in WILDCARD. Same as in WILDCARD (WP2) - Meta Data, including spaces, e.g. AlberIT - UNIRC (spaces around the hyphen) vs BGD-NP (no spaces).
- **TABLE**: table name -> DESIGN, PLOTS, TREES, METADATA, CWD. Send either or both lying and standing trees as TREES. For updates of the SITES table, change the values in the online WILDCARD (WP2) - Meta Data and let Magda know.
- **other**: optional part, you can put whatever helps you differentiate your files. Can be a date, site_name, v2, 2, plot_id, whatever. It is just for your convenience.


**File type**: txt with TAB delimiter and dot (.) decimal.

**Additional information:**
- Fill in all columns, even those that are empty. Empty columns should contain \\N (exactly like this).
- Pay attention not to leave additional tabs in the file, whether in the column names or the data itself. Otherwise it will break the upload.
- Difference between new data (whether partial, e.g. new trees in plots, or complete) and updates. In case you have both new data and updates, then separate these into different files eg. UPLOAD_VUK_TREES.csv and UPDATE_VUK_PLOTS.csv.
- If an update includes changes in more than one column, but not all of those columns were changed in every row, the preferred approach is to split the submission into separate groups or files so that each file contains rows with the same set of changed columns. Alternatively, everything can be submitted in a single file, but in that case any column that was not changed in a given row should still contain its original value rather than \\N.
- Combining new data and updates is possible and will be approached on a case-by-case basis to facilitate efficiency.
- Geometry should be in the `EWKT format` -> SRID=epsg_code;WKT e.g. SRID=4326;POINT(14.0 50.0). You can check the allowed srid values in the 'Mandatory columns by table' section.

**UPLOAD**
- New data. Makes no changes to the data already in the database.
- Send your data according to the templates and fill in all the mandatory columns.

**UPDATE**
- Changes to the data already present in the database.
- Send your data according to the template, but include only the respective record_id and the columns that have changed.
- Example: In the table 'trees', there are four trees with the wrong position and volume → send a file UPDATE_yourinstitute_TREES.csv containing the columns: tree_record_id, position, and volume.
"""
)

# Section: Mandatory columns 
st.header("Mandatory columns by table")

for table_token, schema in EXPECTATIONS.items():
    upload_cols = get_upload_mandatory_columns(schema)
    update_cols = get_update_required_columns(schema)

    title = f"{table_token.upper()}"

    # Optional: show the database table name when it differs
    if schema["db_table"] != table_token:
        title += f"  (db: {schema['db_table']})"

    with st.expander(title):
        st.markdown("**UPLOAD: mandatory columns**")
        st.code("\n".join(upload_cols), language=None)

        st.markdown("**UPDATE: required columns**")
        st.code("\n".join(update_cols), language=None)

        geom_rules = get_geom_rules(schema)
        if geom_rules:
            st.markdown("**Geometry rules**")
            for geom_col, allowed_srids in geom_rules.items():
                st.write(
                    f"- {geom_col}: allowed SRIDs = {format_srid_help(allowed_srids)}"
                )

        st.caption(
            "For UPDATE files, include the record_id and only the columns you want to change."
        )

# Section: Navigation Guide
st.header("Navigation Guide")
st.write(
"""
Use the sidebar to navigate through the following sections:
- **File Validation**: Upload your file to check it for data ranges/values and columns compliance.
- **Terms and Conditions**: Gain access to the database. Only applicable to people who have a valid reason to access the data of other institutes. Access to your own data is automatic and you don't have to fill in this form.
"""
)

# Section: Quick Start
st.header("Quick Start Guide")
st.markdown(
"""
1. **File Validation**: Go to the **File Validation** section to upload your TXT file. You will see the result of the file check and can download a JSON with all the results. 
""")

# Closing Note
st.header("Contacts")
st.write(
"""
*For any assistance or if you believe you need insitutional or special access to the database, contact Magda Guńka (magdalena.gunka(at)vuk.gov.cz).*
*Enjoy using the app!*
"""
)