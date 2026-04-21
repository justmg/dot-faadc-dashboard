"""
Repackage the DoD Assistance Listing Shell PBIT as a DOT-targeted shell.

What this script does:
  1. Rewrites the FAADC Data Pool partition to a hybrid CSV (GitHub raw) + API
     (USAspending spending_by_award) pattern.
  2. Restructures the 3 derived FAADC tables (FAADC FY20+, FAADC FY20+ Dups,
     Map-Table_FAIN-to-ALN) to reference FAADC Data Pool instead of re-pulling
     the source 4x.
  3. Replaces 8 internal/DoD-only source-bound tables (Acronyms, Data Dict &
     Criteria, OMB 1-PRO/AL, Events & Tasks, Annual Update, Consolidated
     Business Names 3, EDA, AL POCs) with empty typed stubs that preserve
     column schema so DAX measures and relationships still compile.
  4. Removes Parameter1/Parameter2 and Transform* helper expressions left over
     from the SharePoint Excel tables.
  5. Adds 4 expression parameters: BaseCsvUrl, AgencyId, FYStart, OverlapDays.
  6. Strips stale RemoteArtifacts from Connections.
  7. Repackages as "<original>.DOT.pbit" alongside the source file.

The DataModelSchema is UTF-16LE without BOM; Connections is UTF-8. Other files
are copied through unchanged.
"""

from __future__ import annotations

import json
import shutil
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
EXTRACTED = ROOT / "extracted"
SOURCE_PBIT = ROOT / "250415 Assistance Listing Shell.pbit"
OUTPUT_PBIT = ROOT / "250415 Assistance Listing Shell.DOT.pbit"

# --------------------------------------------------------------------------
# Schemas for empty typed stubs — column name and Power Query M type token.
# Preserving these makes DAX measures and auto-detected relationships compile.
# --------------------------------------------------------------------------
STUB_SCHEMAS: dict[str, list[tuple[str, str]]] = {
    "Acronyms": [
        ("Acronym", "text"),
        ("Description", "text"),
    ],
    "AL POCs": [
        ("AL#", "number"),
        ("Title", "text"),
        ("Funding Sub Tier Agency", "text"),
        ("POC Name", "text"),
        ("POC email", "text"),
        ("POC phone", "text"),
        ("POC address", "text"),
    ],
    "Data Dict & Criteria": [
        ("Source", "text"),
        ("Criteria Type", "text"),
        ("Data", "text"),
        ("Definitions & Notes", "text"),
    ],
    "OMB 1-PRO/AL": [
        ("Assistance Listing (AL) Number", "text"),
        ("Assistance Listing (AL) Name", "text"),
        ("Number of Programs Represented by this AL", "Int64.Type"),
        ("Do you plan to take action on this AL (Yes / No)?", "text"),
        ("If yes, what action do you plan to take?", "text"),
        ("If yes, by what date do you plan to take this action?", "text"),
        ("If no, why do you not plan to take action?", "text"),
        ("Any additional Information to provide?", "text"),
    ],
    "Events & Tasks": [
        ("Occurrence", "text"),
        ("Time", "text"),
        ("Location, if available", "text"),
        ("Action Required", "text"),
        ("Notes", "text"),
        ("Milestone", "date"),
        ("Start", "date"),
        ("End", "date"),
        ("Task", "text"),
        ("Resource", "text"),
    ],
    "Annual Update": [
        ("No.", "Int64.Type"),
        ("Status", "text"),
        ("Assistance Listings", "text"),
        ("Funding Sub-Tier Agency", "text"),
        ("Sub-Tier", "text"),
        ("AL#", "number"),
        ("Popular Name", "text"),
    ],
    "EDA": [
        ("contract", "text"),
        ("aco_mod", "text"),
        ("pco_mod", "text"),
        ("issue_dodaac", "text"),
        ("awarding_agency", "text"),
        ("fiscal_year", "Int64.Type"),
        ("sign_date", "date"),
        ("instrument_type", "text"),
        ("admin_dodaac", "text"),
        ("total_obligated_amount", "number"),
        ("closed_date", "text"),
        ("Contract_mod", "text"),
        ("FY_Contract_Mod", "text"),
    ],
    "Consolidated Business Names 3": [
        ("Original Name", "text"),
        ("Consolidated Name", "text"),
    ],
}


def empty_table_expression(schema: list[tuple[str, str]]) -> list[str]:
    """Build a list-of-lines M expression producing an empty typed table."""
    cols = ", ".join(f"#\"{name}\" = {tp}" for name, tp in schema)
    return [
        "let",
        f"    Source = #table(type table [{cols}], {{}})",
        "in",
        "    Source",
    ]


# --------------------------------------------------------------------------
# Expression parameters (appear under Manage Parameters in Power BI Desktop)
# --------------------------------------------------------------------------
PARAMETERS = [
    {
        "name": "BaseCsvUrl",
        "kind": "m",
        "expression": [
            "\"https://github.com/justmg/dot-faadc-dashboard/releases/download/snapshot/dot_faadc.csv.gz\""
            " meta [IsParameterQuery=true, Type=\"Text\", IsParameterQueryRequired=true]"
        ],
        "lineageTag": "a1b2c3d4-e5f6-7890-1234-56789abcdef0",
        "annotations": [{"name": "PBI_ResultType", "value": "Text"}],
    },
    {
        "name": "AgencyName",
        "kind": "m",
        "expression": [
            "\"Department of Transportation\""
            " meta [IsParameterQuery=true, Type=\"Text\", IsParameterQueryRequired=true]"
        ],
        "lineageTag": "b2c3d4e5-f6a7-8901-2345-6789abcdef01",
        "annotations": [{"name": "PBI_ResultType", "value": "Text"}],
    },
    {
        "name": "FYStart",
        "kind": "m",
        "expression": [
            "2020 meta [IsParameterQuery=true, Type=\"Number\", IsParameterQueryRequired=true]"
        ],
        "lineageTag": "c3d4e5f6-a7b8-9012-3456-789abcdef012",
        "annotations": [{"name": "PBI_ResultType", "value": "Number"}],
    },
    {
        "name": "OverlapDays",
        "kind": "m",
        "expression": [
            "30 meta [IsParameterQuery=true, Type=\"Number\", IsParameterQueryRequired=true]"
        ],
        "lineageTag": "d4e5f6a7-b8c9-0123-4567-89abcdef0123",
        "annotations": [{"name": "PBI_ResultType", "value": "Number"}],
    },
]


# --------------------------------------------------------------------------
# FAADC Data Pool - hybrid M
# Column-mapping logic tolerates missing/renamed columns from USAspending by
# treating them as null, so the query does not fail if the upstream schema
# shifts slightly.
# --------------------------------------------------------------------------
FAADC_DATA_POOL_M: list[str] = [
    "let",
    "    // Fetch + decompress the DOT FABS CSV snapshot from GitHub Releases",
    "    Source = Csv.Document(",
    "        Binary.Decompress(Web.Contents(BaseCsvUrl), Compression.GZip),",
    "        [Delimiter=\",\", Encoding=65001, QuoteStyle=QuoteStyle.Csv]",
    "    ),",
    "    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),",
    "    Renamed = Table.RenameColumns(Promoted, {",
    "        {\"award_id_fain\", \"FAIN\"},",
    "        {\"modification_number\", \"Amendment Number\"},",
    "        {\"recipient_name\", \"Legal Business Name\"},",
    "        {\"recipient_uei\", \"UEI\"},",
    "        {\"action_date\", \"Action Date\"},",
    "        {\"period_of_performance_start_date\", \"Period Of Performance Start Date\"},",
    "        {\"period_of_performance_current_end_date\", \"Period Of Performance End Date\"},",
    "        {\"assistance_type_description\", \"Assistance Type Description\"},",
    "        {\"cfda_number\", \"ALN\"},",
    "        {\"cfda_title\", \"Assistance Listing Program Titles\"},",
    "        {\"prime_award_base_transaction_description\", \"Assistance Description\"},",
    "        {\"primary_place_of_performance_state_name\", \"Principal Place Of Performance State Name\"},",
    "        {\"primary_place_of_performance_country_name\", \"Principal Place Of Performance Country Name\"},",
    "        {\"primary_place_of_performance_code\", \"Principal Place Of Performance Code\"},",
    "        {\"funding_agency_name\", \"Funding Department Name\"},",
    "        {\"funding_sub_agency_name\", \"Funding Sub-Tier Agency\"},",
    "        {\"funding_sub_agency_code\", \"Funding Sub Tier Agency Code\"},",
    "        {\"funding_office_name\", \"Funding Office Name\"},",
    "        {\"funding_office_code\", \"Funding Office Code\"},",
    "        {\"funding_opportunity_number\", \"Funding Opportunity Number\"},",
    "        {\"business_types_code\", \"FABS Business Types Code\"},",
    "        {\"business_types_description\", \"FABS Business Types Description\"},",
    "        {\"federal_action_obligation\", \"FAO\"},",
    "        {\"last_modified_date\", \"Last Modified Date\"}",
    "    }, MissingField.Ignore),",
    "    Typed = Table.TransformColumnTypes(Renamed, {",
    "        {\"Action Date\", type date},",
    "        {\"Period Of Performance Start Date\", type date},",
    "        {\"Period Of Performance End Date\", type date},",
    "        {\"Last Modified Date\", type date},",
    "        {\"FAO\", Currency.Type},",
    "        {\"Principal Place Of Performance Code\", type text}",
    "    }, \"en-US\"),",
    "    WithFY = Table.AddColumn(Typed, \"Fiscal Year\",",
    "        each if [Action Date] = null then null",
    "             else if Date.Month([Action Date]) >= 10 then Date.Year([Action Date]) + 1",
    "             else Date.Year([Action Date]),",
    "        Int64.Type),",
    "    WithFainAmend = Table.AddColumn(WithFY, \"FAIN_Amend\",",
    "        each (if [FAIN] = null then \"\" else Text.From([FAIN])) & \"_\"",
    "           & (if [Amendment Number] = null then \"\" else Text.From([Amendment Number])),",
    "        type text),",
    "    WithFYFainAmend = Table.AddColumn(WithFainAmend, \"FY_FAIN_Amend\",",
    "        each (if [Fiscal Year] = null then \"\" else Text.From([Fiscal Year])) & \"_\" & [FAIN_Amend],",
    "        type text),",
    "    WithLbn = Table.AddColumn(WithFYFainAmend, \"LBN-Consolidated\",",
    "        each [Legal Business Name], type text),",
    "    // Congressional District: FABS PrimeAwardSummaries omits it. Keep the",
    "    // column as null so any slicers referencing it still compile.",
    "    WithCD = Table.AddColumn(WithLbn, \"Principal Place Of Performance Congressional District\",",
    "        each null, type text)",
    "in",
    "    WithCD",
]


# --------------------------------------------------------------------------
# Derived FAADC tables now reference the main FAADC Data Pool query.
# --------------------------------------------------------------------------
FAADC_FY20_PLUS_DUPS_M = [
    "let",
    "    Source = #\"FAADC Data Pool\",",
    "    #\"Filtered FY20+\" = Table.SelectRows(Source, each [Fiscal Year] <> null and [Fiscal Year] >= 2020)",
    "in",
    "    #\"Filtered FY20+\"",
]

FAADC_FY20_PLUS_M = [
    "let",
    "    Source = #\"FAADC FY20+ Dups\",",
    "    // Dedup on FAIN_Amend — with PrimeAwardSummaries, modification_number is",
    "    // blank so FAIN_Amend collapses to FAIN_ and can collide across fiscal years",
    "    // (the one-side relationship on this column then rejects the load).",
    "    #\"Removed Duplicates\" = Table.Distinct(Source, {\"FAIN_Amend\"})",
    "in",
    "    #\"Removed Duplicates\"",
]

MAP_FAIN_TO_ALN_M = [
    "let",
    "    Source = #\"FAADC Data Pool\",",
    "    #\"Selected\" = Table.SelectColumns(Source, {\"FAIN\", \"Amendment Number\", \"ALN\"}),",
    "    #\"Changed Type\" = Table.TransformColumnTypes(#\"Selected\", {{\"ALN\", type number}}),",
    "    #\"Deduped\" = Table.Distinct(#\"Changed Type\"),",
    "    #\"Merged\" = Table.CombineColumns(#\"Deduped\", {\"FAIN\", \"Amendment Number\"},",
    "        Combiner.CombineTextByDelimiter(\"-\", QuoteStyle.None), \"FAIN+Amend#\")",
    "in",
    "    #\"Merged\"",
]


# --------------------------------------------------------------------------
# Remove helper expressions that exist only to power the now-stubbed SharePoint tables.
# --------------------------------------------------------------------------
# With UnappliedChanges stripped from the .pbit, these helpers can be removed
# safely — nothing references them anymore.
EXPRESSIONS_TO_REMOVE = {
    "Parameter1", "Sample File", "Transform Sample File", "Transform File",
    "Parameter2", "Sample File (2)", "Transform Sample File (2)", "Transform File (2)",
}

# --------------------------------------------------------------------------
# Phase 2: bake the MCP-driven cleanup in so a fresh regenerate lands clean.
# --------------------------------------------------------------------------

# Drop these tables entirely (no empty-stub partition, just gone from the model).
# Visuals that referenced them will need manual cleanup on the report canvas.
TABLES_TO_DROP = {
    "Acronyms", "AL POCs", "Data Dict & Criteria", "OMB 1-PRO/AL",
    "Events & Tasks", "Annual Update", "EDA", "Consolidated Business Names 3",
}

# Columns removed from FAADC Data Pool: DoD-workflow fields (always null for
# public data), R&D fields (DOT has ~none), composites that depend on them,
# and leftover USAspending columns the new M does not rename.
FAADC_COLUMNS_TO_DROP = {
    # DoD-workflow — not in public data
    "Approved By", "Last Modified By", "Closed Status", "Number of Records",
    "Indirect Cost Dollars",
    # R&D — DOT has ~none
    "R&D Indicator", "R&D Type",
    # Calculated composites whose base columns are gone
    "Closed Status (groups)", "R&D Type (groups)", "Higher Learning",
    # USAspending leftovers not part of the FAADC contract
    "assistance_type_code", "transaction_description", "total_obligated_amount",
    "awarding_agency_name", "awarding_sub_agency_name",
}

# Measures to remove — these reference dropped tables or columns
MEASURES_TO_DROP = {"CountInFAADCNotInEDA", "InFAADC"}

# New calculated column: map Funding Sub-Tier Agency -> short DOT mode abbrev
DOT_MODE_COLUMN = {
    "name": "DOT Mode",
    "dataType": "string",
    "isDataTypeInferred": True,
    "type": "calculated",
    "expression": [
        "SWITCH( TRUE(),",
        "    CONTAINSSTRING('FAADC Data Pool'[Funding Sub-Tier Agency], \"Highway Admin\"),           \"FHWA\",",
        "    CONTAINSSTRING('FAADC Data Pool'[Funding Sub-Tier Agency], \"Transit Admin\"),           \"FTA\",",
        "    CONTAINSSTRING('FAADC Data Pool'[Funding Sub-Tier Agency], \"Aviation Admin\"),          \"FAA\",",
        "    CONTAINSSTRING('FAADC Data Pool'[Funding Sub-Tier Agency], \"Railroad Admin\"),          \"FRA\",",
        "    CONTAINSSTRING('FAADC Data Pool'[Funding Sub-Tier Agency], \"Motor Carrier\"),           \"FMCSA\",",
        "    CONTAINSSTRING('FAADC Data Pool'[Funding Sub-Tier Agency], \"Maritime Admin\"),          \"MARAD\",",
        "    CONTAINSSTRING('FAADC Data Pool'[Funding Sub-Tier Agency], \"Highway Traffic Safety\"),  \"NHTSA\",",
        "    CONTAINSSTRING('FAADC Data Pool'[Funding Sub-Tier Agency], \"Pipeline and Hazardous\"),  \"PHMSA\",",
        "    CONTAINSSTRING('FAADC Data Pool'[Funding Sub-Tier Agency], \"Saint Lawrence\"),          \"SLSDC\",",
        "    CONTAINSSTRING('FAADC Data Pool'[Funding Sub-Tier Agency], \"Secretary of Transportation\"), \"OST\",",
        "    \"Other\"",
        ")",
    ],
    "lineageTag": "d07m0de0-0000-4000-8000-000000000001",
    "summarizeBy": "none",
    "annotations": [{"name": "SummarizationSetBy", "value": "Automatic"}],
}


def drop_tables_and_cascade(model: dict, drop: set[str]) -> None:
    """Remove tables from model.tables, plus orphaned LocalDateTable_* that
    were wired to their date columns, plus any relationship that touches a
    dropped table, plus any annotation reference."""
    tables = model["tables"]

    # Find LocalDateTables reachable only from dropped tables via relationships.
    ldts_to_drop: set[str] = set()
    for r in model.get("relationships", []):
        if r.get("fromTable") in drop and str(r.get("toTable", "")).startswith("LocalDateTable_"):
            ldts_to_drop.add(r["toTable"])
    drop = drop | ldts_to_drop

    model["tables"] = [t for t in tables if t.get("name") not in drop]

    # Prune relationships touching dropped tables
    model["relationships"] = [
        r for r in model.get("relationships", [])
        if r.get("fromTable") not in drop and r.get("toTable") not in drop
    ]

    # Trim the PBI_QueryOrder annotation so dropped tables do not reappear
    for ann in model.get("annotations", []):
        if ann.get("name") == "PBI_QueryOrder":
            try:
                order = json.loads(ann["value"])
                ann["value"] = json.dumps([n for n in order if n not in drop])
            except (KeyError, json.JSONDecodeError):
                pass


def drop_columns_from_table(model: dict, table_name: str, columns_to_drop: set[str]) -> None:
    tbl = next((t for t in model["tables"] if t.get("name") == table_name), None)
    if not tbl:
        return
    tbl["columns"] = [c for c in tbl.get("columns", []) if c.get("name") not in columns_to_drop]
    # Prune relationships that reference any dropped column on this table
    model["relationships"] = [
        r for r in model.get("relationships", [])
        if not (
            (r.get("fromTable") == table_name and r.get("fromColumn") in columns_to_drop)
            or (r.get("toTable") == table_name and r.get("toColumn") in columns_to_drop)
        )
    ]


def drop_measures(model: dict, measure_names: set[str]) -> None:
    for tbl in model["tables"]:
        if "measures" in tbl:
            tbl["measures"] = [m for m in tbl["measures"] if m.get("name") not in measure_names]


def add_calculated_column(model: dict, table_name: str, column: dict) -> None:
    tbl = next((t for t in model["tables"] if t.get("name") == table_name), None)
    if not tbl:
        return
    if any(c.get("name") == column["name"] for c in tbl.get("columns", [])):
        return  # idempotent
    tbl.setdefault("columns", []).append(column)


def rewrite_partition(partition: dict, new_expression: list[str]) -> None:
    partition["mode"] = "import"
    partition["source"] = {"type": "m", "expression": new_expression}


def transform_model(model_json: dict) -> None:
    model = model_json["model"]

    # 1. Rewrite partition M for the FAADC family
    for tbl in model["tables"]:
        name = tbl.get("name", "")
        parts = tbl.get("partitions")
        if not parts:
            continue
        if name == "FAADC Data Pool":
            rewrite_partition(parts[0], FAADC_DATA_POOL_M)
            parts[0]["name"] = "FAADC Data Pool"
        elif name == "FAADC FY20+ Dups":
            rewrite_partition(parts[0], FAADC_FY20_PLUS_DUPS_M)
        elif name == "FAADC FY20+":
            rewrite_partition(parts[0], FAADC_FY20_PLUS_M)
        elif name == "Map-Table_FAIN-to-ALN":
            rewrite_partition(parts[0], MAP_FAIN_TO_ALN_M)

    # 2. Drop the 8 DoD/SharePoint-sourced tables outright (plus their LDTs, relationships)
    drop_tables_and_cascade(model, TABLES_TO_DROP)

    # 3. Prune dead columns from FAADC Data Pool (DoD-workflow, R&D, leftover USAspending)
    drop_columns_from_table(model, "FAADC Data Pool", FAADC_COLUMNS_TO_DROP)

    # 4. Remove measures that referenced dropped tables/columns
    drop_measures(model, MEASURES_TO_DROP)

    # 5. Add the DOT Mode calculated column
    add_calculated_column(model, "FAADC Data Pool", DOT_MODE_COLUMN)

    # 6. Clean obsolete helper expressions, add parameters
    exprs = model.get("expressions", [])
    exprs = [e for e in exprs if e.get("name") not in EXPRESSIONS_TO_REMOVE]
    existing_names = {e["name"] for e in exprs}
    new_params = [p for p in PARAMETERS if p["name"] not in existing_names]
    model["expressions"] = new_params + exprs

    # 7. QueryOrder: parameters first, drop tables we removed
    param_prefix = ["BaseCsvUrl", "AgencyName", "FYStart", "OverlapDays"]
    for ann in model.get("annotations", []):
        if ann.get("name") == "PBI_QueryOrder":
            try:
                existing = json.loads(ann.get("value") or "[]")
            except json.JSONDecodeError:
                existing = []
            merged = param_prefix + [
                n for n in existing
                if n not in param_prefix and n not in TABLES_TO_DROP
            ]
            ann["value"] = json.dumps(merged)


def load_utf16le_json(path: Path) -> dict:
    return json.loads(path.read_bytes().decode("utf-16-le"))


def save_utf16le_json(path: Path, data: dict) -> None:
    # Matches the original file format: UTF-16LE, no BOM, no pretty-print newlines
    # (Power BI is tolerant about whitespace — we just emit compact JSON).
    s = json.dumps(data, ensure_ascii=False)
    path.write_bytes(s.encode("utf-16-le"))


def clean_connections(path: Path) -> None:
    data = json.loads(path.read_bytes().decode("utf-8"))
    data["RemoteArtifacts"] = []
    path.write_bytes(json.dumps(data).encode("utf-8"))


# UnappliedChanges is a Power Query "pending edits" file that OVERRIDES
# DataModelSchema on load. It holds the original DoD dataflow M queries, so we
# strip it entirely and let DataModelSchema be authoritative.
SKIP_FILES = {"UnappliedChanges"}


def repackage(src_pbit: Path, extracted_dir: Path, out_pbit: Path) -> None:
    # Preserve original ordering/compression — open the source zip as a template.
    with zipfile.ZipFile(src_pbit, "r") as src_zf:
        names = src_zf.namelist()
    if out_pbit.exists():
        out_pbit.unlink()
    with zipfile.ZipFile(out_pbit, "w", zipfile.ZIP_DEFLATED) as out_zf:
        for name in names:
            if name in SKIP_FILES:
                continue
            src_path = extracted_dir / name
            if not src_path.exists():
                # Fall back to the original (e.g. for unchanged binary files)
                with zipfile.ZipFile(src_pbit, "r") as src_zf:
                    out_zf.writestr(name, src_zf.read(name))
            else:
                out_zf.write(src_path, arcname=name)


def main() -> int:
    if not SOURCE_PBIT.exists():
        raise FileNotFoundError(SOURCE_PBIT)
    if not EXTRACTED.exists():
        raise FileNotFoundError(EXTRACTED)

    dms_path = EXTRACTED / "DataModelSchema"
    conn_path = EXTRACTED / "Connections"

    print(f"Loading {dms_path}")
    dms = load_utf16le_json(dms_path)
    print(f"  tables: {len(dms['model']['tables'])}, expressions: {len(dms['model'].get('expressions', []))}")

    print("Transforming model...")
    transform_model(dms)
    print(f"  tables: {len(dms['model']['tables'])}, expressions: {len(dms['model']['expressions'])}")

    save_utf16le_json(dms_path, dms)
    print(f"Wrote {dms_path}")

    clean_connections(conn_path)
    print(f"Cleaned {conn_path}")

    repackage(SOURCE_PBIT, EXTRACTED, OUTPUT_PBIT)
    size_mb = OUTPUT_PBIT.stat().st_size / (1024 * 1024)
    print(f"Wrote {OUTPUT_PBIT} ({size_mb:.2f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
