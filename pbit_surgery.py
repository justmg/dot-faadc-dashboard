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
    "    // ============================================================",
    "    // BASE LAYER: CSV snapshot (GitHub raw URL, refreshed monthly)",
    "    // ============================================================",
    "    BaseCsvRaw = try Csv.Document(",
    "        Binary.Decompress(Web.Contents(BaseCsvUrl), Compression.GZip),",
    "        [Delimiter=\",\", Columns=null, Encoding=65001, QuoteStyle=QuoteStyle.Csv]",
    "    ) otherwise null,",
    "",
    "    BasePromoted = if BaseCsvRaw = null then null",
    "                   else Table.PromoteHeaders(BaseCsvRaw, [PromoteAllScalars=true]),",
    "",
    "    // Resolve any of several possible FABS column names; return null column if missing.",
    "    GetCol = (tbl as nullable table, names as list) as any =>",
    "        if tbl = null then null",
    "        else let",
    "            cols = Table.ColumnNames(tbl),",
    "            hit  = List.First(List.Select(names, each List.Contains(cols, _)), null)",
    "        in if hit = null then null else Table.Column(tbl, hit),",
    "",
    "    MapFabsTable = (tbl as nullable table) as table =>",
    "        if tbl = null or Table.RowCount(tbl) = 0 then",
    "            #table(type table [",
    "                #\"Fiscal Year\" = Int64.Type, #\"Action Date\" = date,",
    "                #\"FABS Business Types Code\" = text, #\"FABS Business Types Description\" = text,",
    "                #\"Legal Business Name\" = text, #\"UEI\" = text,",
    "                #\"FAIN\" = text, #\"Amendment Number\" = text,",
    "                #\"Assistance Type Description\" = text,",
    "                #\"R&D Indicator\" = text, #\"R&D Type\" = text,",
    "                #\"Period Of Performance Start Date\" = date,",
    "                #\"Period Of Performance End Date\" = date,",
    "                #\"ALN\" = text, #\"Assistance Listing Program Titles\" = text,",
    "                #\"Assistance Description\" = text,",
    "                #\"Principal Place Of Performance Congressional District\" = text,",
    "                #\"Principal Place Of Performance State Name\" = text,",
    "                #\"Principal Place Of Performance Country Name\" = text,",
    "                #\"Principal Place Of Performance Code\" = Int64.Type,",
    "                #\"Approved By\" = text, #\"Last Modified By\" = text,",
    "                #\"Last Modified Date\" = date, #\"Closed Status\" = text,",
    "                #\"Funding Department Name\" = text,",
    "                #\"Funding Office Code\" = text, #\"Funding Office Name\" = text,",
    "                #\"Funding Sub Tier Agency Code\" = text,",
    "                #\"Funding Sub-Tier Agency\" = text,",
    "                #\"Funding Opportunity Number\" = text,",
    "                #\"Number of Records\" = Int64.Type,",
    "                #\"Indirect Cost Dollars\" = Currency.Type,",
    "                #\"FAO\" = Currency.Type,",
    "                #\"FAIN_Amend\" = text, #\"FY_FAIN_Amend\" = text,",
    "                #\"LBN-Consolidated\" = text",
    "            ], {})",
    "        else let",
    "            n = Table.RowCount(tbl),",
    "            Nulls = List.Repeat({null}, n),",
    "            GetOrNulls = (names as list) => let c = GetCol(tbl, names) in if c = null then Nulls else c,",
    "            actionDate = List.Transform(GetOrNulls({\"action_date\"}), each try Date.FromText(_) otherwise null),",
    "            fy = List.Transform(GetOrNulls({\"action_date_fiscal_year\",\"fiscal_year\"}), each try Int64.From(_) otherwise null),",
    "            fyComputed = List.Transform(List.Zip({actionDate, fy}), each",
    "                if _{1} <> null then _{1}",
    "                else if _{0} = null then null",
    "                else if Date.Month(_{0}) >= 10 then Date.Year(_{0}) + 1",
    "                else Date.Year(_{0})),",
    "            fain = List.Transform(GetOrNulls({\"fain\",\"award_id_fain\"}), each if _ = null then null else Text.From(_)),",
    "            amend = List.Transform(GetOrNulls({\"modification_number\"}), each if _ = null then null else Text.From(_)),",
    "            fainAmend = List.Transform(List.Zip({fain, amend}), each",
    "                if _{0} = null then null else Text.From(_{0}) & \"_\" & Text.From(_{1} ?? \"\")),",
    "            fyFainAmend = List.Transform(List.Zip({fyComputed, fainAmend}), each",
    "                if _{0} = null or _{1} = null then null else Text.From(_{0}) & \"_\" & _{1}),",
    "            recipient = List.Transform(GetOrNulls({\"recipient_name\"}), each if _ = null then null else Text.From(_)),",
    "            fao = List.Transform(GetOrNulls({\"federal_action_obligation\",\"total_funding_amount\",\"total_federal_funding_amount\"}), each try Currency.From(_) otherwise null),",
    "            out = #table(",
    "                {",
    "                    \"Fiscal Year\", \"Action Date\", \"FABS Business Types Code\", \"FABS Business Types Description\",",
    "                    \"Legal Business Name\", \"UEI\", \"FAIN\", \"Amendment Number\",",
    "                    \"Assistance Type Description\", \"R&D Indicator\", \"R&D Type\",",
    "                    \"Period Of Performance Start Date\", \"Period Of Performance End Date\",",
    "                    \"ALN\", \"Assistance Listing Program Titles\", \"Assistance Description\",",
    "                    \"Principal Place Of Performance Congressional District\",",
    "                    \"Principal Place Of Performance State Name\",",
    "                    \"Principal Place Of Performance Country Name\",",
    "                    \"Principal Place Of Performance Code\",",
    "                    \"Approved By\", \"Last Modified By\", \"Last Modified Date\", \"Closed Status\",",
    "                    \"Funding Department Name\", \"Funding Office Code\", \"Funding Office Name\",",
    "                    \"Funding Sub Tier Agency Code\", \"Funding Sub-Tier Agency\",",
    "                    \"Funding Opportunity Number\", \"Number of Records\",",
    "                    \"Indirect Cost Dollars\", \"FAO\", \"FAIN_Amend\", \"FY_FAIN_Amend\", \"LBN-Consolidated\"",
    "                },",
    "                List.Zip({",
    "                    fyComputed, actionDate,",
    "                    GetOrNulls({\"business_types\",\"business_types_code\"}),",
    "                    GetOrNulls({\"business_types_description\"}),",
    "                    recipient, GetOrNulls({\"recipient_uei\"}),",
    "                    fain, amend,",
    "                    GetOrNulls({\"assistance_type_description\",\"assistance_type\"}),",
    "                    GetOrNulls({\"research_and_development_funds_indicator\",\"research_and_development_funds\"}),",
    "                    GetOrNulls({\"type_of_research_and_development_funds_description\",\"type_of_research_and_development_funds\"}),",
    "                    List.Transform(GetOrNulls({\"period_of_performance_start_date\"}), each try Date.FromText(_) otherwise null),",
    "                    List.Transform(GetOrNulls({\"period_of_performance_current_end_date\",\"period_of_performance_end_date\"}), each try Date.FromText(_) otherwise null),",
    "                    GetOrNulls({\"cfda_number\",\"assistance_listing_number\"}),",
    "                    GetOrNulls({\"cfda_title\",\"assistance_listing_title\"}),",
    "                    GetOrNulls({\"award_description\",\"transaction_description\",\"prime_award_base_transaction_description\"}),",
    "                    GetOrNulls({\"primary_place_of_performance_congressional_district\",\"place_of_performance_congressional_district\"}),",
    "                    GetOrNulls({\"primary_place_of_performance_state_name\",\"place_of_performance_state_name\"}),",
    "                    GetOrNulls({\"primary_place_of_performance_country_name\",\"place_of_performance_country_name\"}),",
    "                    List.Transform(GetOrNulls({\"primary_place_of_performance_code\",\"place_of_performance_code\"}), each try Int64.From(_) otherwise null),",
    "                    Nulls, Nulls,",
    "                    List.Transform(GetOrNulls({\"last_modified_date\",\"record_last_modified_date\"}), each try Date.FromText(_) otherwise null),",
    "                    Nulls,",
    "                    GetOrNulls({\"funding_agency_name\",\"funding_department_name\"}),",
    "                    GetOrNulls({\"funding_office_code\"}),",
    "                    GetOrNulls({\"funding_office_name\"}),",
    "                    GetOrNulls({\"funding_sub_agency_code\",\"funding_sub_tier_agency_code\"}),",
    "                    GetOrNulls({\"funding_sub_agency_name\",\"funding_sub_tier_agency_name\"}),",
    "                    GetOrNulls({\"funding_opportunity_number\"}),",
    "                    Nulls,",
    "                    List.Transform(GetOrNulls({\"indirect_federal_sharing\",\"indirect_cost_federal_share_amount\"}), each try Currency.From(_) otherwise null),",
    "                    fao, fainAmend, fyFainAmend, recipient",
    "                })",
    "            ),",
    "            typed = Table.TransformColumnTypes(out, {",
    "                {\"Fiscal Year\", Int64.Type}, {\"Action Date\", type date},",
    "                {\"FABS Business Types Code\", type text}, {\"FABS Business Types Description\", type text},",
    "                {\"Legal Business Name\", type text}, {\"UEI\", type text},",
    "                {\"FAIN\", type text}, {\"Amendment Number\", type text},",
    "                {\"Assistance Type Description\", type text},",
    "                {\"R&D Indicator\", type text}, {\"R&D Type\", type text},",
    "                {\"Period Of Performance Start Date\", type date},",
    "                {\"Period Of Performance End Date\", type date},",
    "                {\"ALN\", type text}, {\"Assistance Listing Program Titles\", type text},",
    "                {\"Assistance Description\", type text},",
    "                {\"Principal Place Of Performance Congressional District\", type text},",
    "                {\"Principal Place Of Performance State Name\", type text},",
    "                {\"Principal Place Of Performance Country Name\", type text},",
    "                {\"Principal Place Of Performance Code\", Int64.Type},",
    "                {\"Approved By\", type text}, {\"Last Modified By\", type text},",
    "                {\"Last Modified Date\", type date}, {\"Closed Status\", type text},",
    "                {\"Funding Department Name\", type text},",
    "                {\"Funding Office Code\", type text}, {\"Funding Office Name\", type text},",
    "                {\"Funding Sub Tier Agency Code\", type text},",
    "                {\"Funding Sub-Tier Agency\", type text},",
    "                {\"Funding Opportunity Number\", type text},",
    "                {\"Number of Records\", Int64.Type},",
    "                {\"Indirect Cost Dollars\", Currency.Type},",
    "                {\"FAO\", Currency.Type},",
    "                {\"FAIN_Amend\", type text}, {\"FY_FAIN_Amend\", type text},",
    "                {\"LBN-Consolidated\", type text}",
    "            })",
    "        in typed,",
    "",
    "    BaseMapped = MapFabsTable(BasePromoted),",
    "",
    "    // ============================================================",
    "    // DELTA LAYER: paginated USAspending spending_by_award API",
    "    // ============================================================",
    "    BaseMax = if Table.RowCount(BaseMapped) = 0 then null",
    "              else List.Max(BaseMapped[#\"Action Date\"]),",
    "    DeltaStart = if BaseMax = null then #date(FYStart - 1, 10, 1)",
    "                 else Date.AddDays(BaseMax, - OverlapDays),",
    "    DeltaEnd = Date.From(DateTime.LocalNow()),",
    "",
    "    DeltaFields = {",
    "        \"Award ID\", \"Recipient Name\", \"Recipient UEI\", \"Start Date\", \"End Date\",",
    "        \"Award Amount\", \"Description\", \"Awarding Agency\", \"Awarding Sub Agency\",",
    "        \"Funding Agency\", \"Funding Sub Agency\", \"Funding Office\",",
    "        \"Place of Performance State Code\", \"Place of Performance State\",",
    "        \"Place of Performance Country Code\", \"Place of Performance Zip5\",",
    "        \"Place of Performance Congressional District\",",
    "        \"Assistance Listings\", \"prime_award_recipient_id\", \"Last Modified Date\"",
    "    },",
    "",
    "    FetchPage = (page as number) as record =>",
    "        Json.Document(Web.Contents(",
    "            \"https://api.usaspending.gov/\",",
    "            [",
    "                RelativePath = \"api/v2/search/spending_by_award/\",",
    "                Headers = [#\"Content-Type\" = \"application/json\"],",
    "                Content = Json.FromValue([",
    "                    filters = [",
    "                        award_type_codes = {\"02\",\"03\",\"04\",\"05\"},",
    "                        agencies = {[#\"type\" = \"awarding\", tier = \"toptier\", name = AgencyName]},",
    "                        time_period = {[",
    "                            start_date = Date.ToText(DeltaStart, \"yyyy-MM-dd\"),",
    "                            end_date   = Date.ToText(DeltaEnd,   \"yyyy-MM-dd\"),",
    "                            date_type  = \"action_date\"",
    "                        ]}",
    "                    ],",
    "                    fields = DeltaFields,",
    "                    page = page,",
    "                    limit = 100,",
    "                    sort = \"Award ID\",",
    "                    order = \"asc\"",
    "                ])",
    "            ])),",
    "",
    "    DeltaPages = List.Generate(",
    "        () => [p = 1, r = try FetchPage(1) otherwise [results = {}, page_metadata = [hasNext = false]]],",
    "        each [r][page_metadata][hasNext]? = true or [p] = 1,",
    "        each [p = [p] + 1, r = try FetchPage([p] + 1) otherwise [results = {}, page_metadata = [hasNext = false]]],",
    "        each [r][results]",
    "    ),",
    "",
    "    DeltaRecords = List.Combine(DeltaPages),",
    "    DeltaTable = if List.IsEmpty(DeltaRecords) then null",
    "                 else Table.FromRecords(DeltaRecords),",
    "",
    "    // Map spending_by_award response fields into the FAADC contract. Most award-level",
    "    // fields are populated; action-level fields (Amendment Number, FABS Business",
    "    // Types, Indirect Cost) remain null because the Search API does not expose them.",
    "    MapDelta = (tbl as nullable table) as table =>",
    "        if tbl = null or Table.RowCount(tbl) = 0 then",
    "            MapFabsTable(null)",
    "        else let",
    "            n = Table.RowCount(tbl),",
    "            Nulls = List.Repeat({null}, n),",
    "            Col = (name) => if List.Contains(Table.ColumnNames(tbl), name) then Table.Column(tbl, name) else Nulls,",
    "            startDate = List.Transform(Col(\"Start Date\"), each try Date.FromText(_) otherwise null),",
    "            endDate = List.Transform(Col(\"End Date\"), each try Date.FromText(_) otherwise null),",
    "            fyComputed = List.Transform(startDate, each if _ = null then null",
    "                        else if Date.Month(_) >= 10 then Date.Year(_) + 1 else Date.Year(_)),",
    "            fain = List.Transform(Col(\"Award ID\"), each if _ = null then null else Text.From(_)),",
    "            fainAmend = List.Transform(fain, each if _ = null then null else _ & \"_\"),",
    "            fyFainAmend = List.Transform(List.Zip({fyComputed, fainAmend}), each",
    "                if _{0} = null or _{1} = null then null else Text.From(_{0}) & \"_\" & _{1}),",
    "            recipient = List.Transform(Col(\"Recipient Name\"), each if _ = null then null else Text.From(_)),",
    "            fao = List.Transform(Col(\"Award Amount\"), each try Currency.From(_) otherwise null)",
    "        in #table(",
    "            Table.ColumnNames(MapFabsTable(null)),",
    "            List.Zip({",
    "                fyComputed, startDate, Nulls, Nulls,",
    "                recipient, Col(\"Recipient UEI\"),",
    "                fain, Nulls,",
    "                Nulls, Nulls, Nulls,",
    "                startDate, endDate,",
    "                Col(\"Assistance Listings\"), Col(\"Assistance Listings\"),",
    "                Col(\"Description\"),",
    "                Col(\"Place of Performance Congressional District\"),",
    "                Col(\"Place of Performance State\"),",
    "                Nulls,",
    "                Nulls,",
    "                Nulls, Nulls,",
    "                List.Transform(Col(\"Last Modified Date\"), each try Date.FromText(_) otherwise null),",
    "                Nulls,",
    "                Col(\"Funding Agency\"),",
    "                Nulls, Col(\"Funding Office\"),",
    "                Nulls, Col(\"Funding Sub Agency\"),",
    "                Nulls,",
    "                Nulls,",
    "                Nulls,",
    "                fao, fainAmend, fyFainAmend, recipient",
    "            })",
    "        ),",
    "",
    "    DeltaMapped = MapDelta(DeltaTable),",
    "",
    "    // ============================================================",
    "    // COMBINE and dedupe",
    "    // ============================================================",
    "    Combined = Table.Combine({BaseMapped, DeltaMapped}),",
    "    Deduped = Table.Distinct(Combined, {\"FAIN_Amend\", \"Action Date\"})",
    "in",
    "    Deduped",
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
