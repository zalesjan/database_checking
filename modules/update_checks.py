from __future__ import annotations

import io
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from shapely import from_wkt
from shapely.validation import explain_validity
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXPECTATIONS_DIR = PROJECT_ROOT / "expectations"

# filename token -> expectation file + db naming
TABLE_CONFIG = {
    "design": {
        "expectation_file": "design.json",
        "db_table": "site_design",
        "update_record_id": "site_design_record_id",
        "allowed_srids": [4326],
    },
    "plots": {
        "expectation_file": "plots.json",
        "db_table": "plots",
        "update_record_id": "plot_record_id",
        "allowed_srids": [3035, 0],
    },
    "trees": {
        "expectation_file": "trees.json",
        "db_table": "trees",
        "update_record_id": "tree_record_id",
        "allowed_srids": [3035, 0],
    },
    "cwd": {
        "expectation_file": "cwd.json",
        "db_table": "cwd",
        "update_record_id": "cwd_record_id",
        "allowed_srids": None,
    },
    "metadata": {
        "expectation_file": "metadata.json",
        "db_table": "metadata",
        "update_record_id": "metadata_record_id",
        "allowed_srids": None,
    },
}

RECORD_ID_PATTERN = re.compile(r".*_record_id$", re.IGNORECASE)
FILENAME_RE = re.compile(
    r"^(upload|update|query)_(?P<institute>.+?)_(design|plots|trees|cwd|metadata)(?:_(?P<other>.+))?\.txt$",
    re.IGNORECASE,
)

EWKT_RE = re.compile(r"^\s*SRID=(\d+);(.+)$", re.IGNORECASE | re.DOTALL)
WKT_RE = re.compile(
    r"^\s*(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\s*\(",
    re.IGNORECASE,
)

BOOLEAN_VALUES = {"0", "1", "true", "false", "t", "f", "y", "n"}


@dataclass
class Issue:
    level: str
    code: str
    message: str
    column: Optional[str] = None
    rows: List[int] = field(default_factory=list)
    details: Optional[Dict[str, Any]] = None


@dataclass
class ValidationResult:
    file_name: str
    passed: bool = True
    parsed_name: Dict[str, Any] = field(default_factory=dict)
    issues: List[Issue] = field(default_factory=list)
    stats: Dict[str, Any] = field(default_factory=dict)
    manual_review: Dict[str, Any] = field(default_factory=dict)

    def add_issue(
        self,
        level: str,
        code: str,
        message: str,
        column: Optional[str] = None,
        rows: Optional[List[int]] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.issues.append(
            Issue(
                level=level,
                code=code,
                message=message,
                column=column,
                rows=rows or [],
                details=details,
            )
        )
        if level.upper() == "ERROR":
            self.passed = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file_name": self.file_name,
            "passed": self.passed,
            "parsed_name": self.parsed_name,
            "stats": self.stats,
            "manual_review": self.manual_review,
            "issues": [
                {
                    "level": i.level,
                    "code": i.code,
                    "message": i.message,
                    "column": i.column,
                    "rows": i.rows,
                    "details": i.details,
                }
                for i in self.issues
            ],
        }
    

def load_expectation(table_token: str) -> Dict[str, Any]:
    table_cfg = TABLE_CONFIG[table_token]
    path = EXPECTATIONS_DIR / table_cfg["expectation_file"]
    if not path.exists():
        raise FileNotFoundError(f"Expectation file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_filename(file_name: str) -> Optional[Dict[str, Any]]:
    lowered = file_name.strip().lower()
    match = FILENAME_RE.match(lowered)
    if not match:
        return None

    action = lowered.split("_", 1)[0]
    table_token = match.group(3).lower()

    return {
        "action": action,
        "institute": match.group("institute"),
        "table_token": table_token,
        "db_table": TABLE_CONFIG[table_token]["db_table"],
        "update_record_id": TABLE_CONFIG[table_token]["update_record_id"],
        "allowed_srids": TABLE_CONFIG[table_token]["allowed_srids"],
        "other": match.group("other"),
    }


def try_read_csv_with_fallback(uploaded_file: Any)-> Tuple[Optional[pd.DataFrame], Optional[Dict[str, Any]], Optional[str]]:
    raw = uploaded_file.getvalue()

    encodings_to_try = [
        ("utf-8", False),
        ("cp1250", True),
        ("cp1252", True),
        ("latin-1", True),
    ]

    decoded_text = None
    used_encoding = None
    encoding_warning = None

    for enc, is_fallback in encodings_to_try:
        try:
            decoded_text = raw.decode(enc)
            used_encoding = enc
            if is_fallback:
                encoding_warning = (
                    f"File is not UTF-8. It was read using fallback encoding '{enc}'. "
                    "Validation continued, but this should still be fixed."
                )
            break
        except UnicodeDecodeError:
            continue

    if decoded_text is None:
        return None, None, "File could not be decoded with UTF-8 or fallback encodings."

    first_line = decoded_text.splitlines()[0] if decoded_text.splitlines() else ""
    delimiter_error = None
    if "\t" not in first_line:
        delimiter_error = "File is not TAB-delimited."

    try:
        df = pd.read_csv(
            io.StringIO(decoded_text),
            sep="\t",
            dtype="object",
            keep_default_na=True,
            na_values=[r"\N"],
        )
    except Exception as exc:
        return None, None, f"Could not load CSV: {exc}"

    return df, {
        "used_encoding": used_encoding,
        "encoding_warning": encoding_warning,
        "delimiter_error": delimiter_error,
    }, None

def normalise_component(value: Any, missing_as: Optional[str] = None) -> Optional[str]:
    if pd.isna(value):
        return missing_as

    text = str(value).strip()
    if text == "":
        return missing_as

    try:
        f = float(text)
        if f.is_integer() and "." in text:
            return str(int(f))
    except Exception:
        pass

    return text

def normalise_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip().lower() for c in out.columns]
    for col in out.columns:
        out[col] = out[col].map(lambda x: x.strip() if isinstance(x, str) else x)
    return out

def resolve_present_columns(
    df: pd.DataFrame, expectation: Dict[str, Any]
) -> Tuple[Dict[str, str], List[str], List[str]]:
    columns_cfg = expectation["columns"]
    resolved = {}
    all_aliases = set()

    for canonical_name, rules in columns_cfg.items():
        aliases = [a.strip().lower() for a in rules.get("aliases", [])]
        if canonical_name.lower() not in aliases:
            aliases = [canonical_name.lower()] + aliases
        all_aliases.update(aliases)

        for alias in aliases:
            if alias in df.columns:
                resolved[canonical_name] = alias
                break

    missing_expected = [col for col in columns_cfg if col not in resolved]
    unexpected = [col for col in df.columns if col not in all_aliases]
    return resolved, missing_expected, unexpected

def split_composed_site_id(value: Any) -> Optional[Dict[str, str]]:
    if pd.isna(value):
        return None

    text = str(value).strip()
    parts = text.split("__")

    # expected:
    # institute__siteid__sitename__wildcardsubid

    if len(parts) != 4:
        return None

    institute, site_id, site_name, wildcard_sub_id = parts

    return {
        "institute": normalise_component(institute),
        "site_id": normalise_component(site_id),
        "site_name": normalise_component(site_name, missing_as="NA"),
        "wildcard_sub_id": normalise_component(wildcard_sub_id, missing_as="NA"),
    }

def split_inventory_id(value: Any) -> Optional[Dict[str, str]]:
    if pd.isna(value):
        return None

    text = str(value).strip()
    parts = text.split("__")

    # expected:
    # institute__dataset__inventoryyear__siteid__wildcardsubid__inventorytype
    # dataset is present only here and is ignored in the comparison
    if len(parts) != 6:
        return None

    institute, dataset, inventory_year, site_id, wildcard_sub_id, inventory_type = parts

    return {
    "institute": normalise_component(institute),
    "dataset": normalise_component(dataset),
    "inventory_year": normalise_component(inventory_year),
    "site_id": normalise_component(site_id),
    "wildcard_sub_id": normalise_component(wildcard_sub_id, missing_as="NA"),
    "inventory_type": normalise_component(inventory_type),
}


def parse_ewkt(value: Any) -> Tuple[bool, Optional[int], Optional[str], Optional[str]]:
    """
    Returns:
        ok, srid, wkt, error_message
    """
    if pd.isna(value):
        return True, None, None, None

    text = str(value).strip()
    match = EWKT_RE.match(text)
    if not match:
        return False, None, None, "Value is not valid EWKT."

    srid = int(match.group(1))
    wkt = match.group(2).strip()

    if not WKT_RE.match(wkt):
        return False, srid, wkt, "WKT does not start with a recognised geometry type."

    try:
        geom = from_wkt(wkt)
    except Exception as exc:
        return False, srid, wkt, f"WKT could not be parsed: {exc}"

    if not geom.is_valid:
        return False, srid, wkt, f"Geometry is invalid: {explain_validity(geom)}"

    return True, srid, wkt, None


def extract_first_coords(wkt: str) -> Optional[Tuple[float, float]]:
    match = re.search(r"\(\s*(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)", wkt)
    if not match:
        return None
    return float(match.group(1)), float(match.group(2))


def plausible_coords(srid: int, x: float, y: float) -> bool:
    if srid == 4326:
        return -180 <= x <= 180 and -90 <= y <= 90
    
    if srid == 3035:
        return 0 <= x <= 8_000_000 and 0 <= y <= 8_000_000
    
    if srid == 0:
        return True
    
    return abs(x) < 10_000_000 and abs(y) < 10_000_000

def validate_composed_site_id_components(df: pd.DataFrame, result: ValidationResult) -> None:
    if "composed_site_id" not in df.columns:
        return

    comparable_fields = [
        "institute",
        "site_id",
        "site_name",
        "wildcard_sub_id",
    ]
    present_fields = [c for c in comparable_fields if c in df.columns]

    if not present_fields:
        return

    bad_rows = []
    bad_details = []

    for idx, row in df.iterrows():
        parsed = split_composed_site_id(row["composed_site_id"])
        if parsed is None:
            bad_rows.append(int(idx) + 2)
            bad_details.append(
                {"row": int(idx) + 2, "reason": "Could not parse composed_site_id"}
            )
            continue

        checks = {}
        if "institute" in df.columns:
            checks["institute"] = normalise_component(row["institute"])
        if "site_id" in df.columns:
            checks["site_id"] = normalise_component(row["site_id"])
        if "site_name" in df.columns:
            checks["site_name"] = normalise_component(row["site_name"], missing_as="NA")
        if "wildcard_sub_id" in df.columns:
            checks["wildcard_sub_id"] = normalise_component(row["wildcard_sub_id"], missing_as="NA")

        mismatches = {}
        for key, row_val in checks.items():
            parsed_val = parsed[key]
            if row_val != parsed_val:
                mismatches[key] = {
                    "from_composed_site_id": parsed_val,
                    "from_column": row_val,
                }

        if mismatches:
            bad_rows.append(int(idx) + 2)
            bad_details.append(
                {
                    "row": int(idx) + 2,
                    "composed_site_id": row["composed_site_id"],
                    "mismatches": mismatches,
                }
            )

    if bad_rows:
        result.add_issue(
            "ERROR",
            "row.composed_site_id_mismatch",
            "Values derived from composed_site_id do not match one or more present component columns.",
            column="composed_site_id",
            rows=bad_rows,
            details={"examples": bad_details},
        )

def validate_inventory_id_components(df: pd.DataFrame, result: ValidationResult) -> None:
    if "inventory_id" not in df.columns:
        return

    comparable_fields = [
        "institute",
        "inventory_year",
        "site_id",
        "wildcard_sub_id",
        "inventory_type",
    ]
    present_fields = [c for c in comparable_fields if c in df.columns]

    if not present_fields:
        return

    bad_rows = []
    bad_details = []

    for idx, row in df.iterrows():
        parsed = split_inventory_id(row["inventory_id"])
        if parsed is None:
            bad_rows.append(int(idx) + 2)
            bad_details.append(
                {"row": int(idx) + 2, "reason": "Could not parse inventory_id"}
            )
            continue

        checks = {}
        if "institute" in df.columns:
            checks["institute"] = normalise_component(row["institute"])
        if "inventory_year" in df.columns:
            checks["inventory_year"] = normalise_component(row["inventory_year"])
        if "site_id" in df.columns:
            checks["site_id"] = normalise_component(row["site_id"])
        if "wildcard_sub_id" in df.columns:
            checks["wildcard_sub_id"] = normalise_component(row["wildcard_sub_id"], missing_as="NA")
        if "inventory_type" in df.columns:
            checks["inventory_type"] = normalise_component(row["inventory_type"])

        mismatches = {}
        for key, row_val in checks.items():
            parsed_val = parsed[key]
            if row_val != parsed_val:
                mismatches[key] = {
                    "from_inventory_id": parsed_val,
                    "from_column": row_val,
                }

        if mismatches:
            bad_rows.append(int(idx) + 2)
            bad_details.append(
                {
                    "row": int(idx) + 2,
                    "inventory_id": row["inventory_id"],
                    "mismatches": mismatches,
                }
            )

    if bad_rows:
        result.add_issue(
            "ERROR",
            "row.inventory_id_mismatch",
            "Values derived from inventory_id do not match one or more present component columns.",
            column="inventory_id",
            rows=bad_rows,
            details={"examples": bad_details},
        )


def validate_upload_update_shape(
    df: pd.DataFrame,
    result: ValidationResult,
    parsed: Dict[str, Any],
) -> None:
    action = parsed["action"]
    record_id_col = parsed["update_record_id"]

    if action == "upload":
        forbidden = [c for c in df.columns if RECORD_ID_PATTERN.match(c)]
        for col in forbidden:
            result.add_issue(
                "ERROR",
                "upload.record_id_forbidden",
                f"UPLOAD files must not contain record_id column '{col}'.",
                column=col,
            )

    if action == "update":
        if record_id_col not in df.columns:
            result.add_issue(
                "ERROR",
                "update.record_id_missing",
                f"UPDATE file must contain '{record_id_col}'.",
                column=record_id_col,
            )
            return

        null_rows = df.index[df[record_id_col].isna()].tolist()
        if null_rows:
            result.add_issue(
                "ERROR",
                "update.record_id_null",
                f"Column '{record_id_col}' must not contain nulls in UPDATE files.",
                column=record_id_col,
                rows=[int(i) + 2 for i in null_rows],
            )

        other_cols = [c for c in df.columns if c != record_id_col]
        if not other_cols:
            result.add_issue(
                "ERROR",
                "update.no_changed_columns",
                "UPDATE file must contain at least one additional column besides the record_id.",
            )
            return

        empty_rows = df.index[df[other_cols].isna().all(axis=1)].tolist()
        if empty_rows:
            result.add_issue(
                "ERROR",
                "update.row_without_changes",
                "Each UPDATE row must contain at least one non-null changed value besides the record_id.",
                rows=[int(i) + 2 for i in empty_rows],
            )


def validate_column_rules(
    df: pd.DataFrame,
    result: ValidationResult,
    expectation: Dict[str, Any],
    parsed: Dict[str, Any],
) -> None:
    columns_cfg = expectation["columns"]
    resolved, missing_expected, unexpected = resolve_present_columns(df, expectation)

    action = parsed["action"]

    for canonical_name, rules in columns_cfg.items():
        mandatory_flag = f"mandatory_{action}"
        if rules.get(mandatory_flag, False) and canonical_name not in resolved:
            result.add_issue(
                "ERROR",
                "column.required_missing",
                f"Required column '{canonical_name}' is missing.",
                column=canonical_name,
            )

    # for col in missing_expected:
    #     if not columns_cfg[col].get(f"mandatory_{action}", False):
    #         result.add_issue(
    #             "INFO",
    #             "column.expected_missing",
    #             f"Expected column '{col}' is not present.",
    #             column=col,
    #         )

    extra_allowed = expectation.get("additional_columns_allowed", True)

    if unexpected:
        result.manual_review["additional_columns_not_checked"] = unexpected

        if not extra_allowed:
            for col in unexpected:
                result.add_issue(
                    "WARNING",
                    "column.unexpected",
                    f"Additional column '{col}' is present and was not checked.",
                    column=col,
                )

    numeric_summary = {}
    unit_map = {}

    for canonical_name, actual_col in resolved.items():
        rules = columns_cfg[canonical_name]
        series = df[actual_col]

        if rules.get("not_null", False):
            null_rows = df.index[series.isna()].tolist()
            if null_rows:
                result.add_issue(
                    "ERROR",
                    "null.not_allowed",
                    f"Column '{actual_col}' contains nulls but is marked not_null.",
                    column=actual_col,
                    rows=[int(i) + 2 for i in null_rows],
                )

        dtype = rules.get("dtype")

        if dtype in {"int", "float", "numeric"}:
            numeric = pd.to_numeric(series, errors="coerce")
            bad_rows = df.index[numeric.isna() & series.notna()].tolist()
            if bad_rows:
                result.add_issue(
                    "ERROR",
                    "type.numeric",
                    f"Column '{actual_col}' contains non-numeric values.",
                    column=actual_col,
                    rows=[int(i) + 2 for i in bad_rows],
                )
            else:
                if numeric.notna().any():
                    numeric_summary[actual_col] = {
                        "min_in_file": float(numeric.min()),
                        "max_in_file": float(numeric.max()),
                    }

                if "min" in rules:
                    low_rows = df.index[numeric.notna() & (numeric < rules["min"])].tolist()
                    if low_rows:
                        result.add_issue(
                            "ERROR",
                            "range.min",
                            f"Column '{actual_col}' has values below {rules['min']}.",
                            column=actual_col,
                            rows=[int(i) + 2 for i in low_rows],
                        )

                if "max" in rules:
                    max_value = rules["max"]
                    if max_value == "current_year":
                        max_value = datetime.now().year
                    high_rows = df.index[numeric.notna() & (numeric > max_value)].tolist()
                    if high_rows:
                        result.add_issue(
                            "ERROR",
                            "range.max",
                            f"Column '{actual_col}' has values above {max_value}.",
                            column=actual_col,
                            rows=[int(i) + 2 for i in high_rows],
                        )

        elif dtype == "date":
            parsed_dates = pd.to_datetime(series, errors="coerce", format="mixed")
            bad_rows = df.index[series.notna() & parsed_dates.isna()].tolist()
            if bad_rows:
                result.add_issue(
                    "ERROR",
                    "type.date",
                    f"Column '{actual_col}' contains invalid date values.",
                    column=actual_col,
                    rows=[int(i) + 2 for i in bad_rows],
                )

        elif dtype == "boolean":
            bad_mask = series.notna() & ~series.astype(str).str.lower().isin(BOOLEAN_VALUES)
            bad_rows = df.index[bad_mask].tolist()
            if bad_rows:
                result.add_issue(
                    "ERROR",
                    "type.boolean",
                    f"Column '{actual_col}' contains invalid boolean-like values.",
                    column=actual_col,
                    rows=[int(i) + 2 for i in bad_rows],
                )

        elif dtype == "ewkt":
            invalid_rows = []
            invalid_details = []
            implausible_rows = []
            wrong_srid_rows = []
            srids = []

            allowed_srids = parsed.get("allowed_srids")

            for idx, value in series.items():
                ok, srid, wkt, geom_error = parse_ewkt(value)

                if not ok:
                    if pd.notna(value):
                        row_number = int(idx) + 2
                        invalid_rows.append(row_number)
                        invalid_details.append(
                            {
                                "row": row_number,
                                "value": value,
                                "reason": geom_error,
                            }
                        )
                    continue

                if srid is not None:
                    srids.append(srid)

                if allowed_srids is not None and srid is not None and srid not in allowed_srids:
                    wrong_srid_rows.append(int(idx) + 2)

                if srid is not None and wkt:
                    coords = extract_first_coords(wkt)
                    if coords:
                        x, y = coords
                        if not plausible_coords(srid, x, y):
                            implausible_rows.append(int(idx) + 2)

            if invalid_rows:
                result.add_issue(
                    "ERROR",
                    "geom.invalid",
                    f"Column '{actual_col}' contains invalid geometries.",
                    column=actual_col,
                    rows=invalid_rows,
                    details={"examples": invalid_details},
                )

            if wrong_srid_rows:
                result.add_issue(
                    "ERROR",
                    "geom.wrong_srid",
                    f"Column '{actual_col}' must use one of SRIDs {allowed_srids} for table '{parsed.get('table_token')}'.",
                    column=actual_col,
                    rows=wrong_srid_rows,
                )

            if implausible_rows:
                result.add_issue(
                    "WARNING",
                    "geom.implausible",
                    f"Column '{actual_col}' contains geometries that look implausible for the SRID.",
                    column=actual_col,
                    rows=implausible_rows,
                )

            if srids:
                result.stats["geom_srids_found"] = sorted(set(srids))

        allowed_values = rules.get("allowed_values")
        if allowed_values is not None:
            allowed_norm = {str(v).strip().lower() for v in allowed_values}
            bad_mask = series.notna() & ~series.astype(str).str.lower().isin(allowed_norm)
            bad_rows = df.index[bad_mask].tolist()
            if bad_rows:
                result.add_issue(
                    "ERROR",
                    "value.not_allowed",
                    f"Column '{actual_col}' contains disallowed values.",
                    column=actual_col,
                    rows=[int(i) + 2 for i in bad_rows],
                    details={
                        "bad_values": sorted(series[bad_mask].dropna().astype(str).unique().tolist()),
                        "allowed_values": allowed_values,
                    },
                )

        if "unit" in rules:
            unit_map[actual_col] = rules["unit"]

    if "composed_site_id" in df.columns:
        validate_composed_site_id_components(df, result)

    if "inventory_id" in df.columns:
        validate_inventory_id_components(df, result)

    result.manual_review["units"] = unit_map
    result.manual_review["numeric_columns_min_max"] = numeric_summary
    result.manual_review["note"] = (
        "Please manually confirm the units for numeric columns. "
        "The app reports units from the expectation file and min/max values found in the upload."
    )


def validate_uploaded_file(uploaded_file) -> ValidationResult:
    result = ValidationResult(file_name=uploaded_file.name)

    parsed = parse_filename(uploaded_file.name)
    if not parsed:
        result.add_issue(
            "ERROR",
            "filename.invalid",
            "Filename does not match DATATYPE_INSTITUTE_TABLE(_other).txt using table token design/plots/trees/cwd/metadata.",
        )
        return result

    result.parsed_name = parsed

    df, load_meta, load_error = try_read_csv_with_fallback(uploaded_file)

    if load_error:
        result.add_issue("ERROR", "file.invalid", load_error)
        return result

    if load_meta["encoding_warning"]:
        result.add_issue(
            "WARNING",
            "file.non_utf8",
            load_meta["encoding_warning"],
        )

    if load_meta["delimiter_error"]:
        result.add_issue(
            "ERROR",
            "file.not_tab_delimited",
            load_meta["delimiter_error"],
        )
        return result

    if df is None or df.empty:
        result.add_issue("ERROR", "file.empty", "File contains no rows.")
        return result

    df = normalise_dataframe(df)

    expectation = load_expectation(parsed["table_token"])

    validate_upload_update_shape(df, result, parsed)
    validate_column_rules(df, result, expectation, parsed)

    result.stats["n_rows"] = int(len(df))
    result.stats["n_columns"] = int(len(df.columns))
    result.stats["columns"] = df.columns.tolist()

    return result

def render_result(result: ValidationResult) -> None:
    header = f"{result.file_name}"
    if result.passed:
        st.success(f"Tests for {header} passed.")
    else:
        st.error(f"Tests for {header} not passed.")

    parsed = result.parsed_name

    st.markdown("### File summary")
    st.markdown(
        f"""
**Action:** {parsed.get('action', '-')}

**Institute:** {parsed.get('institute', '-')}

**Table token:** {parsed.get('table_token', '-')}

**DB table:** {parsed.get('db_table', '-')}

**Rows:** {result.stats.get('n_rows', '-')}

**Columns:** {result.stats.get('n_columns', '-')}
"""
    )

    st.markdown("### Issues")
    if not result.issues:
        st.write("No issues found.")
    else:
        for idx, issue in enumerate(result.issues, start=1):
            row_txt = ""
            if issue.rows:
                preview = ", ".join(map(str, issue.rows))
                row_txt = f" Rows: {preview}"
                if len(issue.rows) > 10:
                    row_txt += " ..."

            col_txt = f" Column: `{issue.column}`." if issue.column else ""
            st.markdown(
                f"**{idx}. [{issue.level}] {issue.code}**  \n"
                f"{issue.message}{col_txt}{row_txt}"
            )

            if issue.details:
                with st.expander("Details", expanded=False):
                    st.json(issue.details, expanded=False)

    st.markdown("### Manual review")
    units = result.manual_review.get("units", {})
    minsmaxs = result.manual_review.get("numeric_columns_min_max", {})
    extras = result.manual_review.get("additional_columns_not_checked", [])

    if units:
        st.markdown("**Expected units for numeric columns**")
        for col, unit in units.items():
            st.write(f"- {col}: {unit}")

    if minsmaxs:
        st.markdown("**Min/max values found in file**")
        mm_df = pd.DataFrame(
            [
                {"column": col, **vals}
                for col, vals in minsmaxs.items()
            ]
        )
        st.dataframe(mm_df, width="stretch", hide_index=True)

    if extras:
        st.markdown("**Additional columns not checked**")
        st.write(", ".join(extras))

    note = result.manual_review.get("note")
    if note:
        st.info(note)


def results_json_bytes(results: List[ValidationResult]) -> bytes:
    return json.dumps([r.to_dict() for r in results], indent=2, ensure_ascii=False).encode("utf-8")