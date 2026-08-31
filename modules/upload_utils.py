from __future__ import annotations

import io
import json
import re
import uuid
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Tuple

import pandas as pd
from psycopg2 import sql

from modules.database_utils import get_db_connection
from modules.update_checks import (
    load_expectation,
    normalise_dataframe,
    parse_filename,
    resolve_present_columns,
    try_read_csv_with_fallback,
)

TABLE_ORDER = {
    "sites": 1,
    "design": 2,
    "plots": 3,
    "trees": 4,
    "cwd": 5,
    "metadata": 6,
}

GEOM_TABLES = {"plots", "trees"}


@dataclass
class UploadOutcome:
    file_name: str
    action: str
    table_token: str
    db_table: str
    original_rows_in_df: int
    rows_prepared_for_db: int
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_skipped_or_unchanged: int = 0
    rows_with_missing_target_record_id: int = 0
    ignored_columns: List[str] | None = None
    extra_columns_to_extended_attributes: List[str] | None = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def sort_uploaded_files(uploaded_files: List[Any]) -> List[Any]:
    decorated = []
    for uploaded_file in uploaded_files:
        parsed = parse_filename(uploaded_file.name)
        order = 999
        if parsed:
            order = TABLE_ORDER.get(parsed["table_token"], 999)
        decorated.append((order, uploaded_file.name.lower(), uploaded_file))
    decorated.sort(key=lambda item: (item[0], item[1]))
    return [item[2] for item in decorated]


def read_normalised_uploaded_file(uploaded_file: Any) -> Tuple[pd.DataFrame, Dict[str, Any], Dict[str, Any]]:
    parsed = parse_filename(uploaded_file.name)
    if not parsed:
        raise ValueError(f"Invalid file name: {uploaded_file.name}")

    df, load_meta, load_error = try_read_csv_with_fallback(uploaded_file)
    if load_error:
        raise ValueError(load_error)
    if df is None or df.empty:
        raise ValueError(f"File '{uploaded_file.name}' is empty.")

    df = normalise_dataframe(df)
    expectation = load_expectation(parsed["table_token"])
    return df, parsed, expectation


def _canonicalise_dataframe(df: pd.DataFrame, expectation: Dict[str, Any]) -> pd.DataFrame:
    resolved, _, _ = resolve_present_columns(df, expectation)
    rename_map = {actual_name: canonical_name for canonical_name, actual_name in resolved.items()}

    out = df.rename(columns=rename_map).copy()
    out = out.loc[:, ~out.columns.duplicated()]
    return out


def _to_jsonable(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    return value


def _build_extended_attributes(df: pd.DataFrame, extra_columns: List[str]) -> pd.Series:
    def row_to_json(row: pd.Series) -> Any:
        payload = {}
        for col, value in row.items():
            cleaned = _to_jsonable(value)
            if cleaned is None:
                continue
            payload[col] = cleaned
        return json.dumps(payload, ensure_ascii=False) if payload else None

    return df[extra_columns].apply(row_to_json, axis=1)


def _srid_from_ewkt(value: Any) -> int | None:
    if pd.isna(value):
        return None

    text = str(value).strip()
    if text in {"", r"\N"}:
        return None

    match = re.match(r"^\s*SRID=(\d+);", text, flags=re.IGNORECASE)
    if not match:
        return None

    return int(match.group(1))


def _geom_local_from_ewkt(value: Any) -> bool | None:
    srid = _srid_from_ewkt(value)
    if srid is None:
        return None
    return srid == 0

def _build_point(longitude, latitude, srid=0):
    if pd.isna(longitude) or pd.isna(latitude):
        return None
    return f"SRID={srid};POINT({longitude} {latitude})"


def _normalise_plots_list(value: Any) -> Any:
    if pd.isna(value):
        return None

    text = str(value).strip()
    if text in {"", r"\N"}:
        return None

    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
            return json.dumps(parsed, ensure_ascii=False)
        except Exception:
            pass

    items = [item.strip() for item in text.split(";") if item.strip()]
    return json.dumps(items, ensure_ascii=False)


def _get_upload_config(expectation: Dict[str, Any]) -> Dict[str, Any]:
    upload_cfg = expectation.get("upload", {})

    db_columns = list(upload_cfg.get("db_columns", []))
    stage_only_columns = list(upload_cfg.get("stage_only_columns", []))
    never_upload_columns = set(upload_cfg.get("never_upload_columns", []))

    return {
        "db_columns": db_columns,
        "stage_only_columns": stage_only_columns,
        "never_upload_columns": never_upload_columns,
    }


def prepare_dataframe_for_db(
    df: pd.DataFrame,
    parsed: Dict[str, Any],
    expectation: Dict[str, Any],
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    canonical_df = _canonicalise_dataframe(df, expectation)
    db_table = parsed["db_table"]
    action = parsed["action"]
    record_id_col = parsed["update_record_id"]

    upload_cfg = _get_upload_config(expectation)
    db_columns = upload_cfg["db_columns"]
    stage_only_columns = upload_cfg["stage_only_columns"]
    never_upload_columns = upload_cfg["never_upload_columns"]

    if db_table == "site_design" and "plots_list" in canonical_df.columns:
        canonical_df["plots_list"] = canonical_df["plots_list"].map(_normalise_plots_list)

    if db_table == "sites":
        if "longitude" in canonical_df.columns and "latitude" in canonical_df.columns:
            lon = pd.to_numeric(canonical_df["longitude"], errors="coerce")
            lat = pd.to_numeric(canonical_df["latitude"], errors="coerce")

            canonical_df["geom"] = [
                _build_point(x, y, 4326)
                for x, y in zip(lon, lat)
                ]

    # columns that may physically exist in the temp stage table
    stage_columns = [
        col for col in (db_columns + stage_only_columns)
        if col not in {"extended_attributes", "geom_local"}
    ]

    # UPDATE files must carry their record_id into stage
    if action == "update" and record_id_col in canonical_df.columns:
        stage_columns = [record_id_col] + [col for col in stage_columns if col != record_id_col]

    # only keep real incoming columns that are allowed to reach staging
    incoming_stage_columns = [
        col for col in stage_columns
        if col in canonical_df.columns and col not in never_upload_columns
    ]

    prepared_df = canonical_df[incoming_stage_columns].copy()

    # normalise integer columns so values like 820.0 become 820 before COPY
    int_columns = [
        col
        for col, rules in expectation["columns"].items()
        if rules.get("dtype") == "int" and col in prepared_df.columns
    ]

    for col in int_columns:
        numeric = pd.to_numeric(prepared_df[col], errors="coerce")

        # keep real nulls as nulls
        invalid_numeric = prepared_df[col].notna() & numeric.isna()
        if invalid_numeric.any():
            bad_rows = (prepared_df.index[invalid_numeric] + 2).tolist()
            raise ValueError(
                f"File {parsed['file_name'] if 'file_name' in parsed else 'Filename missing'};\nColumn '{col}' contains non-numeric values in rows {bad_rows[:10]}"
            )

        # do not silently round true decimals
        non_integer = numeric.notna() & (numeric % 1 != 0)
        if non_integer.any():
            bad_rows = (prepared_df.index[non_integer] + 2).tolist()
            raise ValueError(
                f"File {parsed['file_name'] if 'file_name' in parsed else 'Filename missing'};\nColumn '{col}' contains decimal values but must be integer in rows {bad_rows[:10]}"
            )

        prepared_df[col] = numeric.astype("Int64")

    # anything present in the file but not actually staged should go to extended_attributes
    extra_columns = [
        col for col in canonical_df.columns
        if col not in incoming_stage_columns
        and col not in never_upload_columns
        and col != record_id_col
        and col != "geom_local"
    ]

    if "extended_attributes" in db_columns and extra_columns and db_table != "metadata":
        prepared_df["extended_attributes"] = _build_extended_attributes(canonical_df, extra_columns)

    if "geom_local" in db_columns and "geom" in prepared_df.columns and db_table in GEOM_TABLES:
        prepared_df["geom_local"] = prepared_df["geom"].map(_geom_local_from_ewkt)

    if action == "update":
        meaningful_columns = [col for col in prepared_df.columns if col != record_id_col]
        if not meaningful_columns:
            raise ValueError(
                f"{parsed['file_name'] if 'file_name' in parsed else parsed['db_table']}: "
                "after applying upload rules there is nothing left to update."
            )

    return prepared_df, {
        "db_columns": db_columns,
        "stage_only_columns": stage_only_columns,
        "never_upload_columns": sorted(never_upload_columns),
        "extra_columns_to_extended_attributes": extra_columns,
    }


def _make_stage_table_name(db_table: str) -> str:
    suffix = uuid.uuid4().hex[:8]
    safe_table = re.sub(r"[^a-z0-9_]+", "_", db_table.lower())
    return f"stage_{safe_table}_{suffix}"


def _create_temp_stage_table(
    cur,
    schema: str,
    db_table: str,
    stage_table: str,
    stage_only_columns: List[str] | None = None,
) -> None:
    query = sql.SQL(
        """
        CREATE TEMP TABLE {stage_table}
        (LIKE {schema}.{db_table} INCLUDING DEFAULTS)
        ON COMMIT DROP
        """
    ).format(
        stage_table=sql.Identifier(stage_table),
        schema=sql.Identifier(schema),
        db_table=sql.Identifier(db_table),
    )
    cur.execute(query)

    stage_only_columns = stage_only_columns or []

    # add stage-only helper columns like trees.inventory_type / trees.plot_id
    for col in stage_only_columns:
        cur.execute(
            sql.SQL(
                "ALTER TABLE {stage_table} ADD COLUMN {col} text"
            ).format(
                stage_table=sql.Identifier(stage_table),
                col=sql.Identifier(col),
            )
        )

    # drop NOT NULL only for FK record_ids that are filled later by matching
    fk_record_id_columns = {
        "site_design": ["site_record_id"],
        "plots": ["site_design_record_id"],
        "trees": ["plot_record_id"],
        "cwd": ["plot_record_id"],
    }.get(db_table, [])

    for col in fk_record_id_columns:
        cur.execute(
            sql.SQL(
                "ALTER TABLE {stage_table} ALTER COLUMN {col} DROP NOT NULL"
            ).format(
                stage_table=sql.Identifier(stage_table),
                col=sql.Identifier(col),
            )
        )


def _copy_dataframe_to_stage(cur, df: pd.DataFrame, stage_table: str) -> None:
    copy_buffer = io.StringIO()
    df.to_csv(copy_buffer, index=False, sep="\t", header=True, na_rep="\\N")
    copy_buffer.seek(0)

    copy_sql = sql.SQL(
        "COPY {stage_table} ({columns}) FROM STDIN WITH DELIMITER E'\\t' CSV HEADER NULL '\\N'"
    ).format(
        stage_table=sql.Identifier(stage_table),
        columns=sql.SQL(", ").join(sql.Identifier(col) for col in df.columns),
    )

    cur.copy_expert(copy_sql.as_string(cur), copy_buffer)


def _populate_foreign_keys(cur, stage_table: str, db_table: str) -> None:
    if db_table == "site_design":
        cur.execute(
            sql.SQL(
                """
                UPDATE {stage} AS s
                SET site_record_id = src.site_record_id
                FROM public.sites AS src
                WHERE s.composed_site_id = src.composed_site_id
                """
            ).format(stage=sql.Identifier(stage_table))
        )
        return

    if db_table == "plots":
        cur.execute(
            sql.SQL(
                """
                UPDATE {stage} AS s
                SET site_design_record_id = src.site_design_record_id
                FROM public.site_design AS src
                WHERE s.composed_site_id = src.composed_site_id
                  AND s.inventory_id = src.inventory_id
                  AND s.inventory_year = src.inventory_year
                  AND s.inventory_type = src.inventory_type
                  AND s.circle_radius IS NOT DISTINCT FROM src.circle_radius
                  AND s.circle_no IS NOT DISTINCT FROM src.circle_no
                  AND COALESCE(src.plots_list, '[]'::jsonb) @> jsonb_build_array(s.plot_id);
                """
            ).format(stage=sql.Identifier(stage_table))
        )
        return

    if db_table == "trees":
        cur.execute(
            sql.SQL(
                """
                UPDATE {stage} AS s
                SET plot_record_id = src.plot_record_id
                FROM public.plots AS src
                WHERE s.composed_site_id = src.composed_site_id
                  AND s.inventory_year = src.inventory_year
                  AND s.inventory_id = src.inventory_id
                  AND s.inventory_type = src.inventory_type
                  AND s.circle_no IS NOT DISTINCT FROM src.circle_no
                  AND s.plot_id IS NOT DISTINCT FROM src.plot_id
                """
            ).format(stage=sql.Identifier(stage_table))
        )
        return

    
    if db_table == "cwd":
        cur.execute(
            sql.SQL(
                """
                UPDATE {stage} AS s
                SET plot_record_id = src.plot_record_id
                FROM public.plots AS src
                WHERE s.composed_site_id = src.composed_site_id
                  AND s.inventory_year = src.inventory_year
                  AND s.inventory_id = src.inventory_id
                  AND s.inventory_type = src.inventory_type
                  AND s.plot_id IS NOT DISTINCT FROM src.plot_id
                  AND ((s.inventory_type = 'LPI' AND src.circle_no IS NULL) OR (s.inventory_type = 'SPI' AND src.circle_no = 1))
                """
            ).format(stage=sql.Identifier(stage_table))
        )


def _insert_upload_rows(
    cur,
    schema: str,
    db_table: str,
    stage_table: str,
    insert_columns: List[str],
) -> int:
    insert_sql = sql.SQL(
        """
        INSERT INTO {schema}.{db_table} ({columns})
        SELECT {columns}
        FROM {stage_table}
        """
    ).format(
        schema=sql.Identifier(schema),
        db_table=sql.Identifier(db_table),
        columns=sql.SQL(", ").join(sql.Identifier(col) for col in insert_columns),
        stage_table=sql.Identifier(stage_table),
    )

    cur.execute(insert_sql)
    return cur.rowcount


def _count_matched_update_rows(cur, schema: str, db_table: str, stage_table: str, record_id_col: str) -> int:
    count_sql = sql.SQL(
        """
        SELECT COUNT(*)
        FROM {stage_table} AS s
        JOIN {schema}.{db_table} AS t
          ON t.{record_id_col} = s.{record_id_col}
        """
    ).format(
        stage_table=sql.Identifier(stage_table),
        schema=sql.Identifier(schema),
        db_table=sql.Identifier(db_table),
        record_id_col=sql.Identifier(record_id_col),
    )
    cur.execute(count_sql)
    return cur.fetchone()[0]


def _update_existing_rows(
    cur,
    schema: str,
    db_table: str,
    stage_table: str,
    df: pd.DataFrame,
    record_id_col: str,
) -> Tuple[int, int, int]:
    update_columns = [col for col in df.columns if col != record_id_col]

    set_clauses = []
    change_conditions = []

    for col in update_columns:
        if col == "extended_attributes":
            set_clauses.append(
                sql.SQL(
                    "{col} = CASE "
                    "WHEN s.{col} IS NULL THEN t.{col} "
                    "ELSE COALESCE(t.{col}, '{{}}'::jsonb) || s.{col} "
                    "END"
                ).format(col=sql.Identifier(col))
            )
            change_conditions.append(
                sql.SQL(
                    "(s.{col} IS NOT NULL AND "
                    "COALESCE(t.{col}, '{{}}'::jsonb) IS DISTINCT FROM "
                    "(COALESCE(t.{col}, '{{}}'::jsonb) || s.{col}))"
                ).format(col=sql.Identifier(col))
            )
        else:
            set_clauses.append(
                sql.SQL("{col} = COALESCE(s.{col}, t.{col})").format(col=sql.Identifier(col))
            )
            change_conditions.append(
                sql.SQL("(s.{col} IS NOT NULL AND t.{col} IS DISTINCT FROM s.{col})").format(
                    col=sql.Identifier(col)
                )
            )

    if not set_clauses:
        return 0, 0, 0

    matched_rows = _count_matched_update_rows(cur, schema, db_table, stage_table, record_id_col)

    update_sql = sql.SQL(
        """
        UPDATE {schema}.{db_table} AS t
        SET {set_clauses}
        FROM {stage_table} AS s
        WHERE t.{record_id_col} = s.{record_id_col}
          AND ({change_conditions})
        """
    ).format(
        schema=sql.Identifier(schema),
        db_table=sql.Identifier(db_table),
        set_clauses=sql.SQL(", ").join(set_clauses),
        stage_table=sql.Identifier(stage_table),
        record_id_col=sql.Identifier(record_id_col),
        change_conditions=sql.SQL(" OR ").join(change_conditions),
    )

    cur.execute(update_sql)
    updated_rows = cur.rowcount
    unchanged_rows = matched_rows - updated_rows
    return matched_rows, updated_rows, unchanged_rows


def process_single_file(uploaded_file: Any, role: str, schema: str = "public") -> UploadOutcome:
    parsed = parse_filename(uploaded_file.name)
    if not parsed:
        raise ValueError(f"Invalid filename: {uploaded_file.name}")

    if parsed["action"] not in {"upload", "update"}:
        raise ValueError(f"{uploaded_file.name}: this page only supports UPLOAD_* and UPDATE_* files.")

    df, parsed, expectation = read_normalised_uploaded_file(uploaded_file)
    parsed["file_name"] = uploaded_file.name

    prepared_df, prep_meta = prepare_dataframe_for_db(df, parsed, expectation)

    outcome = UploadOutcome(
        file_name=uploaded_file.name,
        action=parsed["action"],
        table_token=parsed["table_token"],
        db_table=parsed["db_table"],
        original_rows_in_df=len(df),
        rows_prepared_for_db=len(prepared_df),
        ignored_columns=prep_meta["never_upload_columns"],
        extra_columns_to_extended_attributes=prep_meta["extra_columns_to_extended_attributes"],
    )

    conn = get_db_connection(role)
    if conn is None:
        raise RuntimeError("Database connection failed.")

    stage_table = _make_stage_table_name(parsed["db_table"])
    record_id_col = parsed["update_record_id"]

    try:
        cur = conn.cursor()

        _create_temp_stage_table(
            cur,
            schema,
            parsed["db_table"],
            stage_table,
            prep_meta["stage_only_columns"],
        )
        _copy_dataframe_to_stage(cur, prepared_df, stage_table)
        _populate_foreign_keys(cur, stage_table, parsed["db_table"])

        if parsed["action"] == "upload":
            insert_columns = [col for col in prep_meta["db_columns"] if col != record_id_col]

            outcome.rows_inserted = _insert_upload_rows(
                cur=cur,
                schema=schema,
                db_table=parsed["db_table"],
                stage_table=stage_table,
                insert_columns=insert_columns,
            )
        else:
            matched_rows, updated_rows, unchanged_rows = _update_existing_rows(
                cur=cur,
                schema=schema,
                db_table=parsed["db_table"],
                stage_table=stage_table,
                df=prepared_df,
                record_id_col=record_id_col,
            )
            outcome.rows_updated = updated_rows
            outcome.rows_skipped_or_unchanged = unchanged_rows
            outcome.rows_with_missing_target_record_id = outcome.rows_prepared_for_db - matched_rows

        conn.commit()
        return outcome

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()