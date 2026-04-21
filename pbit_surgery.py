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
        "annotations": [{"name": "PBI_ResultType", "value": "Text"}],
    },
    {
        "name": "AgencyName",
        "kind": "m",
        "expression": [
            "\"Department of Transportation\""
            " meta [IsParameterQuery=true, Type=\"Text\", IsParameterQueryRequired=true]"
        ],
        "annotations": [{"name": "PBI_ResultType", "value": "Text"}],
    },
    {
        "name": "FYStart",
        "kind": "m",
        "expression": [
            "2020 meta [IsParameterQuery=true, Type=\"Number\", IsParameterQueryRequired=true]"
        ],
        "annotations": [{"name": "PBI_ResultType", "value": "Number"}],
    },
    {
        "name": "OverlapDays",
        "kind": "m",
        "expression": [
            "30 meta [IsParameterQuery=true, Type=\"Number\", IsParameterQueryRequired=true]"
        ],
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
    "    WithCD   = Table.AddColumn(WithLbn,  \"Principal Place Of Performance Congressional District\", each null, type text),",
    "    WithRD1  = Table.AddColumn(WithCD,   \"R&D Indicator\",  each null, type text),",
    "    WithRD2  = Table.AddColumn(WithRD1,  \"R&D Type\",       each null, type text),",
    "    WithAB   = Table.AddColumn(WithRD2,  \"Approved By\",    each null, type text),",
    "    WithLMB  = Table.AddColumn(WithAB,   \"Last Modified By\", each null, type text),",
    "    WithCS   = Table.AddColumn(WithLMB,  \"Closed Status\",  each null, type text),",
    "    WithNR   = Table.AddColumn(WithCS,   \"Number of Records\", each null, Int64.Type),",
    "    WithIC   = Table.AddColumn(WithNR,   \"Indirect Cost Dollars\", each null, Currency.Type)",
    "in",
    "    WithIC",
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
    "    #\"Removed Duplicates\" = Table.Distinct(Source, {\"FY_FAIN_Amend\"})",
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
EXPRESSIONS_TO_REMOVE = {
    "Parameter1", "Sample File", "Transform Sample File", "Transform File",
    "Parameter2", "Sample File (2)", "Transform Sample File (2)", "Transform File (2)",
}


def rewrite_partition(partition: dict, new_expression: list[str]) -> None:
    partition["mode"] = "import"
    partition["source"] = {"type": "m", "expression": new_expression}


def transform_model(model_json: dict) -> None:
    model = model_json["model"]
    tables = model["tables"]

    for tbl in tables:
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
        elif name in STUB_SCHEMAS:
            rewrite_partition(parts[0], empty_table_expression(STUB_SCHEMAS[name]))

    # Clean obsolete helper expressions, add parameters
    exprs = model.get("expressions", [])
    exprs = [e for e in exprs if e.get("name") not in EXPRESSIONS_TO_REMOVE]
    # Prepend parameters so they appear at top of the list
    existing_names = {e["name"] for e in exprs}
    new_params = [p for p in PARAMETERS if p["name"] not in existing_names]
    model["expressions"] = new_params + exprs

    # Update PBI_QueryOrder annotation to drop removed helpers and surface parameters first
    keep_order = [
        "BaseCsvUrl", "AgencyName", "FYStart", "OverlapDays",
        "FAADC Data Pool", "FAADC FY20+ Dups", "FAADC FY20+",
        "EDA", "Acronyms", "AL POCs", "Data Dict & Criteria", "OMB 1-PRO/AL",
        "Time of Refresh", "Events & Tasks", "FABS BUS TYPE DES",
        "Unique Legal Business Name", "Map-Table_FAIN-to-ALN", "Annual Update",
        "Consolidated Business Names 3",
    ]
    for ann in model.get("annotations", []):
        if ann.get("name") == "PBI_QueryOrder":
            ann["value"] = json.dumps(keep_order)


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


def repackage(src_pbit: Path, extracted_dir: Path, out_pbit: Path) -> None:
    # Preserve original ordering/compression — open the source zip as a template.
    with zipfile.ZipFile(src_pbit, "r") as src_zf:
        names = src_zf.namelist()
    if out_pbit.exists():
        out_pbit.unlink()
    with zipfile.ZipFile(out_pbit, "w", zipfile.ZIP_DEFLATED) as out_zf:
        for name in names:
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
