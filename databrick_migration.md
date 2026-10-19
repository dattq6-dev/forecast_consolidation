# Block 1: 
============================================================
PARAMETERS & CONFIGURATION
Converted from T-SQL DECLARE statements
============================================================

CATALOG = "user_workbench_fin_priv"
SCHEMA = "fin_bf_rah_ai"
FQN = f"{CATALOG}.{SCHEMA}"  # Fully Qualified Namespace

--- FC Period Parameters ---
START_FC      = "202601"   # FC period begin
END_FC        = "202612"   # FC period end
DATE_FC_LM    = '2026-08-29' 
DATE_RV_BUD   = spark.sql(f"""
                          SELECT MAX(DS_PARTITION_DATE) 
                          FROM user_workbench_fin_priv.fin_bf_rah.rbg_f_revised_budget_2026_unionall
                          """).collect()[0][0] 
REPORT_MONTH = END_FC
YTD_MONTH    = REPORT_MONTH

--- Source Table References ---
TBL_BUDGET_2026   = f"user_workbench_fin_priv.fin_bf_rah.rbg_f_budget_2026_UnionAll"
TBL_FORECAST_2026 = f"user_workbench_fin_priv.fin_bf_rah_ai.Forecast_2026_UnionAll"
TBL_FORECAST_LM   = f"user_workbench_fin_priv.fin_bf_rah.rbg_f_forecast_2026_unionall"
TBL_BUDGET        = f"user_workbench_fin_priv.fin_bf_rah.rbg_f_budget_2026_unionall"
TBL_RV_BUDGET     = f"user_workbench_fin_priv.fin_bf_rah.rbg_f_revised_budget_2026_unionall"

print(f"Catalog.Schema : {FQN}")
print(f"Report Month   : {REPORT_MONTH}")
print(f"YTD Month      : {YTD_MONTH}")
print(f"Forecast table : {TBL_FORECAST_2026}")
print(f"Forecast LM    : {TBL_FORECAST_LM}")
print(f"DATE_RV_BUD    : {DATE_RV_BUD}")

# Block 2: Pre-check: Validate All Input Tables Exist
============================================================
PRE-CHECK: Validate all input tables exist before running
============================================================
# --- All tables referenced in this notebook ---
input_tables = {
    # Source product tables (STEP 2)
    "tbl_cus": "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_cus",
    "tbl_casa": "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_casa",
    "tbl_credit_card": "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_credit_card",
    "tbl_debit_card": "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_debit_card",
    "tbl_lending": "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_lending",
    "tbl_household": "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_household",
    "tbl_td": "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_td",
    "tbl_bond": "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_bond",
    "tbl_banca": "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_banca",
    "tbl_u2fp": "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_u2fp",
    "tbl_fx": "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_fx",
    "tbl_other_income": "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_other_income",
    "tbl_sme": "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_sme",
    "tbl_rhe": "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_rhe",
    # Consolidation source tables (STEP 4a)
    "TBL_BUDGET_2026": TBL_BUDGET_2026,
    "TBL_FORECAST_2026": TBL_FORECAST_2026,
    "TBL_FORECAST_LM": TBL_FORECAST_LM,
    "TBL_RV_BUDGET": TBL_RV_BUDGET,
    # Mapping & reference tables (STEP 4c, 4i, PART 2)
    "mapping_2026_X": f"{FQN}.mapping_2026_X",
    "FC_2026_Y": f"{FQN}.FC_2026_Y",
    "MAPPING_DAYS": "user_workbench_fin_priv.fin_bf_rah.rbg_dim_mapping_days",
}

missing = []
exists = []  # (label, tbl, last_modified)

for label, tbl in input_tables.items():
    if spark.catalog.tableExists(tbl):
        try:
            detail = spark.sql(f"DESCRIBE DETAIL {tbl}").select("lastModified").collect()[0][0]
            last_mod = detail.strftime("%Y-%m-%d %H:%M:%S") if detail else "N/A"
        except Exception as e:
            last_mod = f"ERR: {str(e)[:40]}"
        exists.append((label, tbl, last_mod))
    else:
        missing.append((label, tbl))

 --- Report ---
print(f"\n[OK] {len(exists)} table(s) found:")
print(f"  {'LABEL':<25s}   {'TABLE':<70s}   {'LAST MODIFIED'}")
print(f"  {'-'*25}   {'-'*70}   {'-'*19}")
for label, tbl, last_mod in exists:
    print(f"  + {label:<25s} -> {tbl:<70s}   {last_mod}")

if missing:
    print(f"\n[FAIL] {len(missing)} table(s) MISSING:")
    for label, tbl in missing:
        print(f"  X {label:25s} -> {tbl}")
    print(f"\n{'=' * 60}")
    print("ACTION REQUIRED: Fix missing tables before running the notebook.")
    print("=" * 60)
    raise SystemExit(f"Pre-check FAILED: {len(missing)} table(s) not found.")
else:
    print(f"\n{'=' * 60}")
    print("ALL INPUT TABLES AVAILABLE - Safe to proceed.")
    print("=" * 60)

# Block 3: STEP 1 - Clean & Standardize Source Tables
source_tables = [
    'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_cus',
    'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_casa',
    'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_credit_card',
    'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_debit_card',
    'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_lending',
    'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_household',
    'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_td',
    'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_bond',
    'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_banca',
    'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_u2fp',
    'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_fx',
    'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_other_income',
    'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_sme',
    'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_rhe',
]

--Target type: all columns → STRING, except VALUE → DOUBLE
VALUE_COL_TYPE = 'DOUBLE'

print("=" * 70)
print("CLEANING SOURCE TABLES: Align column names & types")
print("=" * 70)

cleaned_views = {}  # original_table -> view_name
CLEAN_VIEW_COLS = {}  # table -> [col_names_upper] — pre-computed for Cell 5

for tbl in source_tables:
    tbl_short = tbl.split('.')[-1]
    view_name = f"clean_{tbl_short}"
    
    if not spark.catalog.tableExists(tbl):
        print(f"  SKIP: {tbl_short} (does not exist)")
        continue
    
    # Get raw schema
    schema = spark.table(tbl).schema
    
    # Build SELECT with cleaned column names and aligned types
    select_parts = []
    fixes_applied = []
    
    for field in schema.fields:
        raw_name = field.name
        clean_name = raw_name.strip().upper()
        actual_type = field.dataType.simpleString()
        
        # Skip rescued data columns
        if clean_name.startswith('_'):
            continue
        
        # Build column expression
        col_ref = f"`{raw_name}`"  # backtick-quote to handle spaces/special chars
        
        if clean_name == 'VALUE':
            # VALUE → DOUBLE (TRY_CAST to handle literal 'NULL' strings)
            if actual_type != 'double':
                expr = f"TRY_CAST({col_ref} AS {VALUE_COL_TYPE}) AS `{clean_name}`"
                fixes_applied.append(f"{clean_name}: {actual_type}→{VALUE_COL_TYPE}")
            elif raw_name != clean_name:
                expr = f"{col_ref} AS `{clean_name}`"
                fixes_applied.append(f"rename: '{raw_name}'→'{clean_name}'")
            else:
                expr = col_ref
        else:
            # All other columns → STRING
            if actual_type != 'string':
                expr = f"CAST({col_ref} AS STRING) AS `{clean_name}`"
                fixes_applied.append(f"{clean_name}: {actual_type}→STRING")
            elif raw_name != clean_name:
                expr = f"{col_ref} AS `{clean_name}`"
                fixes_applied.append(f"rename: '{raw_name}'→'{clean_name}'")
            else:
                expr = col_ref
        
        select_parts.append(expr)
    
    # Create cleaned temp view
    select_sql = ", ".join(select_parts)
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {view_name} AS SELECT {select_sql} FROM {tbl}")
    cleaned_views[tbl] = view_name
    CLEAN_VIEW_COLS[tbl] = [field.name.strip().upper() for field in schema.fields if not field.name.strip().upper().startswith('_')]
    
    if fixes_applied:
        print(f"  ✓ {tbl_short:<50} fixes: {', '.join(fixes_applied)}")
    else:
        print(f"  ✓ {tbl_short:<50} (no fixes needed)")

print(f"\n{'=' * 70}")
print(f"DONE: {len(cleaned_views)} cleaned temp views created.")
print("Cell 5 will read from these views instead of raw tables.")
print("=" * 70)

============================================================
MISSING COLUMN CHECK
Validate each source table has all required columns for STEP 2
============================================================

Columns STEP 2 always expects (no default fallback)
REQUIRED_COLS = {
    'MONTH', 'BIZLINE', 'CUS_TYPE', 'SEGMENT', 'CHANNEL',
    'CUSTOMER_TIER', 'PRODUCT_FAMILY', 'PRODUCT_LINE',
    'SUB_PRODUCT', 'GROUP_MEASURE', 'MEASURE',
    'SUB_MEASURE', 'SUB_MEASURE_2', 'VALUE',
}

Columns that have conditional defaults in STEP 2 (OK if absent)
OPTIONAL_COLS = {
    'SUB_CHANNEL', 'PERSONA', 'S_FLAG', 'INTERNAL', 'CCY',
    'INFO_1', 'INFO_2', 'MTB_FLAG', 'BANCA_CHANNEL', 'PAYMENT_MODE',
}

Collect schemas once (avoid repeated Analyze RPCs in loop - SC best practice)
schema_map = {}
for tbl in source_tables:
    if spark.catalog.tableExists(tbl):
        schema_map[tbl] = [f.name.strip().upper() for f in spark.table(tbl).schema.fields]

print(f"\n{'=' * 70}")
print("COLUMN COMPLETENESS CHECK vs STEP 2 requirements")
print(f"{'=' * 70}")

has_issues = False
for tbl in source_tables:
    tbl_short = tbl.split('.')[-1]
    if tbl not in schema_map:
        print(f"  SKIP: {tbl_short} (table not found)")
        continue

    col_names_upper = set(schema_map[tbl])
    missing_required = REQUIRED_COLS - col_names_upper
    missing_optional = OPTIONAL_COLS - col_names_upper

    if missing_required:
        has_issues = True
        print(f"  ✗ {tbl_short}")
        print(f"      MISSING REQUIRED : {sorted(missing_required)}")
        if missing_optional:
            print(f"      missing optional : {sorted(missing_optional)}  (will use defaults)")
    else:
        opt_msg = f"  (optional absent: {sorted(missing_optional)})" if missing_optional else ""
        print(f"  ✓ {tbl_short}{opt_msg}")

print(f"{'=' * 70}")
if has_issues:
    print("⚠ ACTION REQUIRED: Fix tables with missing REQUIRED columns before STEP 2.")
else:
    print("ALL source tables have required columns — safe to proceed to STEP 2.")
print(f"{'=' * 70}")

# Block 4: STEP 2 - Create Forecast_2026_UnionAll Table
 ============================================================
 STEP 1: CREATE TABLE Forecast_2026_UnionAll
 Converted from: CREATE TABLE ANALYSIS..Forecast_2026_UnionAll
 ============================================================

spark.sql(f"DROP TABLE IF EXISTS {TBL_FORECAST_2026}")

spark.sql(f"""
    CREATE TABLE {TBL_FORECAST_2026}
    (
       `MONTH`          STRING
      ,BIZLINE          STRING
      ,CUS_TYPE         STRING
      ,SEGMENT          STRING
      ,CHANNEL          STRING
      ,SUB_CHANNEL      STRING
      ,CUSTOMER_TIER    STRING
      ,PERSONA          STRING
      ,PRODUCT_FAMILY   STRING
      ,PRODUCT_LINE     STRING
      ,SUB_PRODUCT      STRING
      ,GROUP_MEASURE    STRING
      ,MEASURE          STRING
      ,SUB_MEASURE      STRING
      ,SUB_MEASURE_2    STRING
      ,INTERNAL         STRING
      ,CCY              STRING
      ,S_FLAG           STRING
      ,INFO_1           STRING
      ,INFO_2           STRING
      ,`VALUE`          DOUBLE
      ,SOURCE_TBL       STRING
    )
    USING DELTA
""")

print(f"=== Table created: {TBL_FORECAST_2026} ===")

# Block 5: STEP 3 - Insert from Product Tables
 ============================================================
 STEP 2: SINGLE INSERT FROM ALL PRODUCT TABLES
 OPTIMIZED: Replaced 12 separate INSERT statements (12 Delta transactions)
            with 1 UNION ALL INSERT (1 Delta transaction)
 Source tables defined in cell 8
 ============================================================

 ===== SOURCE PRODUCT TABLES (from cell 8) =====
tbl_cus           = 'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_cus'
tbl_casa          = 'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_casa'
tbl_credit_card   = 'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_credit_card'
tbl_debit_card    = 'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_debit_card'
tbl_lending       = 'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_lending'
tbl_household     = 'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_household'
tbl_td            = 'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_td'
tbl_bond          = 'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_bond'
tbl_banca         = 'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_banca'
tbl_u2fp          = 'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_u2fp'
tbl_fx            = 'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_fx'
tbl_other_income  = 'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_other_income'
tbl_sme           = 'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_sme'
tbl_rhe           = 'user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_rhe'

source_tables = [
    tbl_cus, tbl_casa, tbl_credit_card, tbl_debit_card,
    tbl_lending, tbl_household, tbl_td, tbl_bond,
    tbl_banca, tbl_u2fp, tbl_other_income, tbl_sme
    , tbl_fx, tbl_rhe
]

 Build UNION ALL query for all product tables
select_clauses = []

skipped_tables = []

for idx, full_tbl in enumerate(source_tables, start=1):
    tbl_name = full_tbl.split('.')[-1]
    
    # --- Use cleaned view + pre-computed metadata from STEP 1b (no metastore calls) ---
    if full_tbl not in CLEAN_VIEW_MAP:
        print(f"WARNING [{idx}]: {tbl_name} not cleaned in STEP 1b - SKIPPED")
        skipped_tables.append(full_tbl)
        continue
    
    use_tbl = CLEAN_VIEW_MAP[full_tbl]
    col_names_upper = CLEAN_VIEW_COLS[full_tbl]
    
    # Cleaned views have normalized column names (uppercase, no spaces)
    col_value = "`VALUE`"
    col_month = "`MONTH`"
    
    # --- Determine column substitutions ---
    col_internal = "INTERNAL" if "TD" in tbl_name.upper() else "''"
    col_ccy = "CCY" if "TD" in tbl_name.upper() else "''"
    col_s_flag = "S_FLAG" if "S_FLAG" in col_names_upper else "1"
    col_persona = "PERSONA" if "PERSONA" in col_names_upper else "'All'"
    col_sub_channel = "SUB_CHANNEL" if "SUB_CHANNEL" in col_names_upper else "'All'"
    
    # INFO_1/INFO_2 logic
    tbl_upper = tbl_name.upper()
    if "BANCA" in tbl_upper:
        col_info_1, col_info_2 = "BANCA_CHANNEL", "PAYMENT_MODE"
    elif "CASA" in tbl_upper:
        col_info_1, col_info_2 = "MTB_FLAG", "''"
    elif "CREDIT_CARD" in tbl_upper:
        col_info_1, col_info_2 = "MTB_FLAG", "''"
    elif "LENDING" in tbl_upper or "SME_MSME" in tbl_upper:
        col_info_1, col_info_2 = "INFO_1", "''"
    else:
        col_info_1, col_info_2 = "''", "''"
    
    # S_FLAG filter
    s_flag_filter = f"AND TRY_CAST({col_s_flag} AS DOUBLE) <> 0" if col_s_flag == "S_FLAG" else ""
    
    # Build SELECT for this table
    select_sql = f"""
    SELECT CAST({col_month} AS STRING) AS `MONTH`
          ,BIZLINE
          ,CUS_TYPE
          ,SEGMENT
          ,CHANNEL
          ,{col_sub_channel}   AS SUB_CHANNEL
          ,CUSTOMER_TIER
          ,{col_persona}       AS PERSONA
          ,PRODUCT_FAMILY
          ,PRODUCT_LINE
          ,SUB_PRODUCT
          ,GROUP_MEASURE
          ,MEASURE
          ,SUB_MEASURE
          ,SUB_MEASURE_2
          ,{col_internal}      AS INTERNAL
          ,{col_ccy}           AS CCY
          ,CAST({col_s_flag} AS STRING) AS S_FLAG
          ,{col_info_1}        AS INFO_1
          ,{col_info_2}        AS INFO_2
          ,TRY_CAST({col_value} AS DOUBLE) AS `VALUE`
          ,'{tbl_name}'        AS SOURCE_TBL
    FROM {use_tbl}
    WHERE TRY_CAST({col_value} AS DOUBLE) <> 0
          {s_flag_filter}
          AND CAST({col_month} AS STRING) BETWEEN '202401' AND '{REPORT_MONTH}'
    """
    
    select_clauses.append(select_sql)
    print(f"Added [{idx}]: {tbl_name}  |  INTERNAL={col_internal}  CCY={col_ccy}  S_FLAG={col_s_flag}")

 Combine all SELECTs with UNION ALL
union_query = "\nUNION ALL\n".join(select_clauses)

 === Summary of skipped tables ===
if skipped_tables:
    print(f"\n{'='*60}")
    print(f"WARNING: {len(skipped_tables)} table(s) NOT FOUND and skipped:")
    for t in skipped_tables:
        print(f"  - {t}")
    print(f"{'='*60}")

 === SINGLE INSERT (1 Delta transaction instead of 12) ===
print(f"\n=== Executing SINGLE INSERT from {len(select_clauses)} tables ===")
spark.sql(f"""
    INSERT INTO {TBL_FORECAST_2026}
    {union_query}
""")

row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {TBL_FORECAST_2026}").collect()[0].cnt
print(f"=== STEP 2 COMPLETE: {row_count:,} rows inserted (1 transaction) ===")

# Block 6: Data Cleaning: DELETE + UPDATE Statements
 ============================================================
 STEP 2c: CLEAN DATA & UPDATE FORMAT - OPTIMIZED
 BEFORE: 32 separate UPDATE/DELETE statements = 32 Delta transactions
 AFTER:  1 DELETE + 1 CREATE OR REPLACE = 2 Delta transactions
 ============================================================

TBL = TBL_FORECAST_2026

 === STEP A: SINGLE DELETE for all unwanted rows ===
print("=== Deleting unwanted rows (1 transaction) ===")
spark.sql(f"""
    DELETE FROM {TBL}
    WHERE UPPER(MEASURE) IN (
            'EOP TICKET SIZE'
           ,'%UNIQUE CUS HAVING NEW CONTRACT - BOND'
           ,'%UNIQUE CUS HAVING NEW CONTRACT - ALL'
           ,'% ACTIVE CUS'
           ,'% CI','% CI_ADJ','% EOP BAL/AVG BAL','% FTP','% NIM'
           ,'AVG BAL/ACTIVE CUS','BOP CUS','NFI/ ACTIVE CUS','NUMBER TNX/ACTIVE CUS'
          )
       OR (UPPER(MEASURE) IN ('UNIQUE CUS HAVING NEW CONTRACT') AND UPPER(SUB_PRODUCT) IN ('BOND','FUND'))
""")
spark.sql(f"DROP TABLE IF EXISTS {TBL_FORECAST_2026}_tmp")

Rename original table temporarily
spark.sql(f"ALTER TABLE {TBL} RENAME TO {TBL_FORECAST_2026}_tmp")

Create new table with all CASE WHEN transformations applied
spark.sql(f"""
    CREATE OR REPLACE TABLE {TBL}
    USING DELTA
    AS
    SELECT
        `MONTH`
       ,BIZLINE
       -- 14. ETB -> Existing
       ,CASE WHEN UPPER(CUS_TYPE) = 'ETB' THEN 'Existing' ELSE CUS_TYPE END AS CUS_TYPE
       -- 15. SEGMENT null -> Others
       ,COALESCE(NULLIF(SEGMENT, ''), 'Others') AS SEGMENT
       -- 12. CHANNEL normalization
       ,CASE WHEN UPPER(CHANNEL) = 'ALL' OR UPPER(CHANNEL) NOT IN ('BRANCH','DEVELOPER','PARTNERSHIP','DIGITAL','WINLIFE','OTHERS')
             THEN 'Others' ELSE CHANNEL END AS CHANNEL
       ,SUB_CHANNEL
       -- 11. CUSTOMER_TIER normalization
       ,CASE WHEN UPPER(CUSTOMER_TIER) IN ('ALL','NULL') OR CUSTOMER_TIER IS NULL
             THEN 'Others' ELSE CUSTOMER_TIER END AS CUSTOMER_TIER
       ,PERSONA
       -- 16. QR Merchant product family + 23. NFX -> Fee base
       ,CASE WHEN UPPER(PRODUCT_LINE) = 'QR MERCHANT' THEN 'QR Merchant'
             WHEN UPPER(PRODUCT_FAMILY) = 'NFX' AND UPPER(BIZLINE) = 'RETAIL' THEN 'Fee base'
             ELSE PRODUCT_FAMILY END AS PRODUCT_FAMILY
       -- 13. Loans to CIs -> Other Loans | 17. Trading Income | 24. FX Flow -> NFX (Retail)
       -- 30. FX Flow -> NFX (all bizlines)
       ,CASE WHEN UPPER(SUB_PRODUCT) IN ('LOANS TO CIS') THEN 'Other Loans'
             WHEN UPPER(PRODUCT_LINE) = 'TRADING INCOME' THEN 'Trading Income'
             WHEN UPPER(PRODUCT_LINE) = 'FX FLOW' THEN 'NFX'
             ELSE PRODUCT_LINE END AS PRODUCT_LINE
       -- 4. BOND-FUND Prix | 13. Loans to CIs | 17. Trading Income | 22. CD Self trade
       ,CASE WHEN UPPER(PRODUCT_LINE) = 'BOND-FUND' AND UPPER(SUB_MEASURE) = 'INCENTIVE FEE' 
                  AND UPPER(SUB_MEASURE_2) = 'SECONDARY FEE' AND UPPER(SUB_PRODUCT) = 'OTHERS' THEN 'Prix'
             WHEN UPPER(SUB_PRODUCT) IN ('LOANS TO CIS') THEN 'Other Loans'
             WHEN UPPER(PRODUCT_LINE) = 'TRADING INCOME' THEN 'Other Trading Income'
             WHEN UPPER(SUB_PRODUCT) = 'CD SELF TRADE' AND UPPER(BIZLINE) = 'RETAIL' THEN 'CDST'
             ELSE SUB_PRODUCT END AS SUB_PRODUCT
       -- 7. Lending NII | 8. Lending/Card NFI
       ,CASE WHEN UPPER(PRODUCT_FAMILY) IN ('LENDING') AND UPPER(GROUP_MEASURE) = 'NII' THEN 'NII'
             WHEN UPPER(PRODUCT_FAMILY) IN ('LENDING','CARD') AND UPPER(GROUP_MEASURE) = 'NFI' THEN 'NFI'
             ELSE GROUP_MEASURE END AS GROUP_MEASURE
       -- Multiple MEASURE transformations (2,3,7,8,9,18,25,26,27,31)
       ,CASE 
             -- 9. TD EOP Cus Distinct
             WHEN UPPER(PRODUCT_FAMILY) = 'TIME DEPOSIT' AND UPPER(PRODUCT_LINE) = 'ALL' AND UPPER(MEASURE) = 'EOP CUS DISTINCT'
                  THEN 'EOP Cus'
             -- 18. FX Flow Ticket size (Retail)
             WHEN UPPER(PRODUCT_LINE) = 'FX FLOW' AND UPPER(BIZLINE) = 'RETAIL' THEN 'Ticket size FX'
             -- 25. NFX Txn_amt -> TXN Amt
             WHEN UPPER(BIZLINE) = 'RETAIL' AND UPPER(MEASURE) = 'TXN_AMT' AND UPPER(PRODUCT_LINE) = 'NFX' THEN 'TXN Amt'
             -- 26. NFX Ticket size
             WHEN UPPER(BIZLINE) = 'RETAIL' AND UPPER(MEASURE) = 'TICKET SIZE' AND UPPER(PRODUCT_LINE) = 'NFX' THEN 'Ticket size FX'
             -- 27. DPM AUM -> EOP Bal
             WHEN UPPER(BIZLINE) = 'RETAIL' AND UPPER(MEASURE) = 'AUM' AND UPPER(PRODUCT_LINE) = 'DPM' THEN 'EOP Bal'
             -- 31. SME EOP Cus Active -> Active Cus
             WHEN UPPER(SUB_MEASURE) = 'EOP CUS ACTIVE' AND UPPER(BIZLINE) = 'SME' THEN 'Active Cus'
             -- 2,3. CARD/BOND-FUND NFI shift
             WHEN (UPPER(PRODUCT_FAMILY) = 'CARD' AND UPPER(GROUP_MEASURE) = 'NFI' AND UPPER(MEASURE) IN ('FI','FE'))
                  OR (UPPER(PRODUCT_LINE) = 'BOND-FUND' AND UPPER(GROUP_MEASURE) = 'NFI') THEN GROUP_MEASURE
             -- 7. Lending NII
             WHEN UPPER(PRODUCT_FAMILY) IN ('LENDING') AND UPPER(GROUP_MEASURE) = 'NII' AND UPPER(MEASURE) <> 'NII' THEN 'NII'
             -- 8. Lending/Card NFI
             WHEN UPPER(PRODUCT_FAMILY) IN ('LENDING','CARD') AND UPPER(GROUP_MEASURE) = 'NFI' AND UPPER(MEASURE) <> 'NFI' THEN 'NFI'
             ELSE MEASURE
        END AS MEASURE
       -- SUB_MEASURE transformations (2,3,5,6,9,18,25,26,27,29,31,32)
       ,CASE
             -- 9. TD EOP Cus Distinct
             WHEN UPPER(PRODUCT_FAMILY) = 'TIME DEPOSIT' AND UPPER(PRODUCT_LINE) = 'ALL' AND UPPER(MEASURE) = 'EOP CUS DISTINCT'
                  THEN 'EOP Cus'
             -- 5. TD Liquidity Cost -> CI
             WHEN UPPER(PRODUCT_FAMILY) = 'TIME DEPOSIT' AND UPPER(GROUP_MEASURE) = 'NII' 
                  AND UPPER(SUB_MEASURE) IN ('LIQUIDITY COST','LIQUIDITY_COST') THEN 'CI'
             -- 6. TD/Lending NFI -> FI
             WHEN UPPER(PRODUCT_FAMILY) IN ('TIME DEPOSIT','LENDING') AND UPPER(SUB_MEASURE) = 'NFI' THEN 'FI'
             -- 18. FX Flow Ticket size (Retail)
             WHEN UPPER(PRODUCT_LINE) = 'FX FLOW' AND UPPER(BIZLINE) = 'RETAIL' THEN 'Ticket size FX'
             -- 25. NFX Txn_amt
             WHEN UPPER(BIZLINE) = 'RETAIL' AND UPPER(MEASURE) = 'TXN_AMT' AND UPPER(PRODUCT_LINE) = 'NFX' THEN 'TXN Amt'
             -- 26. NFX Ticket size
             WHEN UPPER(BIZLINE) = 'RETAIL' AND UPPER(MEASURE) = 'TICKET SIZE' AND UPPER(PRODUCT_LINE) = 'NFX' THEN 'Ticket size FX'
             -- 27. DPM AUM
             WHEN UPPER(BIZLINE) = 'RETAIL' AND UPPER(MEASURE) = 'AUM' AND UPPER(PRODUCT_LINE) = 'DPM' THEN 'EOP Bal'
             -- 29. Other Fee NFI -> FI
             WHEN UPPER(PRODUCT_LINE) = 'OTHER FEE' AND UPPER(MEASURE) = 'NFI' THEN 'FI'
             -- 31. SME EOP Cus Active
             WHEN UPPER(SUB_MEASURE) = 'EOP CUS ACTIVE' AND UPPER(BIZLINE) = 'SME' THEN 'Active Cus'
             -- 32. Bond/Bond-Fund NFI -> FI
             WHEN UPPER(SUB_MEASURE) = 'NFI' AND UPPER(PRODUCT_LINE) IN ('BOND','BOND-FUND') THEN 'FI'
             -- 2,3. CARD/BOND-FUND NFI shift
             WHEN (UPPER(PRODUCT_FAMILY) = 'CARD' AND UPPER(GROUP_MEASURE) = 'NFI' AND UPPER(MEASURE) IN ('FI','FE'))
                  OR (UPPER(PRODUCT_LINE) = 'BOND-FUND' AND UPPER(GROUP_MEASURE) = 'NFI') THEN MEASURE
             ELSE SUB_MEASURE
        END AS SUB_MEASURE
       -- SUB_MEASURE_2 transformations (2,3,4,5,9,18,19-21,25,26,27,28,31)
       ,CASE
             -- 9. TD EOP Cus Distinct
             WHEN UPPER(PRODUCT_FAMILY) = 'TIME DEPOSIT' AND UPPER(PRODUCT_LINE) = 'ALL' AND UPPER(MEASURE) = 'EOP CUS DISTINCT'
                  THEN 'EOP Cus'
             -- 5. TD Liquidity Cost
             WHEN UPPER(PRODUCT_FAMILY) = 'TIME DEPOSIT' AND UPPER(GROUP_MEASURE) = 'NII' 
                  AND UPPER(SUB_MEASURE) IN ('LIQUIDITY COST','LIQUIDITY_COST') THEN 'Liquidity Cost'
             -- 18. FX Flow Ticket size (Retail)
             WHEN UPPER(PRODUCT_LINE) = 'FX FLOW' AND UPPER(BIZLINE) = 'RETAIL' THEN 'Ticket size FX'
             -- 19-21. CASA&AE EOP Cus by INFO_1
             WHEN UPPER(PRODUCT_LINE) = 'CASA&AE' AND UPPER(MEASURE) = 'EOP CUS' AND UPPER(INFO_1) = 'MTB' AND UPPER(BIZLINE) = 'RETAIL'
                  THEN 'MTB Cus'
             WHEN UPPER(PRODUCT_LINE) = 'CASA&AE' AND UPPER(MEASURE) = 'EOP CUS' AND UPPER(INFO_1) = 'TRULY ACTIVE' AND UPPER(BIZLINE) = 'RETAIL'
                  THEN 'Truly Active Cus'
             WHEN UPPER(PRODUCT_LINE) = 'CASA&AE' AND UPPER(MEASURE) = 'EOP CUS' AND UPPER(INFO_1) = 'LOW ACTIVE' AND UPPER(BIZLINE) = 'RETAIL'
                  THEN 'Low Active Cus'
             -- 25. NFX Txn_amt
             WHEN UPPER(BIZLINE) = 'RETAIL' AND UPPER(MEASURE) = 'TXN_AMT' AND UPPER(PRODUCT_LINE) = 'NFX' THEN 'TXN Amt'
             -- 26. NFX Ticket size
             WHEN UPPER(BIZLINE) = 'RETAIL' AND UPPER(MEASURE) = 'TICKET SIZE' AND UPPER(PRODUCT_LINE) = 'NFX' THEN 'Ticket size FX'
             -- 27. DPM AUM
             WHEN UPPER(BIZLINE) = 'RETAIL' AND UPPER(MEASURE) = 'AUM' AND UPPER(PRODUCT_LINE) = 'DPM' THEN 'EOP Bal'
             -- 28. Bond-Fund NFI -> NFI
             WHEN UPPER(PRODUCT_LINE) = 'BOND-FUND' AND UPPER(MEASURE) = 'NFI' THEN 'NFI'
             -- 31. SME EOP Cus Active
             WHEN UPPER(SUB_MEASURE) = 'EOP CUS ACTIVE' AND UPPER(BIZLINE) = 'SME' THEN 'Active Cus'
             -- 2,3. CARD/BOND-FUND NFI shift
             WHEN (UPPER(PRODUCT_FAMILY) = 'CARD' AND UPPER(GROUP_MEASURE) = 'NFI' AND UPPER(MEASURE) IN ('FI','FE'))
                  OR (UPPER(PRODUCT_LINE) = 'BOND-FUND' AND UPPER(GROUP_MEASURE) = 'NFI') THEN SUB_MEASURE
             ELSE SUB_MEASURE_2
        END AS SUB_MEASURE_2
       ,INTERNAL
       -- 10. CCY 'All' -> ''
       ,CASE WHEN UPPER(CCY) = 'ALL' THEN '' ELSE CCY END AS CCY
       ,S_FLAG
       ,INFO_1
       -- 1. BANCA INFO_2: Remove '0' characters
       ,CASE WHEN UPPER(PRODUCT_LINE) = 'BANCA' AND INFO_2 LIKE '%0%'
             THEN REPLACE(INFO_2, '0', '') ELSE INFO_2 END AS INFO_2
       ,`VALUE`
       ,SOURCE_TBL
    FROM {TBL_FORECAST_2026}_tmp
""")

--Drop temp table
spark.sql(f"DROP TABLE IF EXISTS {TBL_FORECAST_2026}_tmp")

print(f"=== STEP 2c COMPLETE: {row_count:,} rows (2 transactions instead of 32) ===")

# Block 7: Backup with date stamp
 ============================================================
 STEP 3: BACKUP - Create dated copy of Forecast_2026_UnionAll
 Converted from: EXEC('SELECT * INTO ...'+@DATE_REPORT+'...')
 ============================================================
from datetime import date

DATE_REPORT = date.today().strftime('%Y-%m-%d')
MONTH_REPORT = date.today().strftime('%Y%m')

spark.sql(f"""
        DELETE FROM user_workbench_fin_priv.fin_bf_rah.rbg_f_forecast_2026_unionall
        WHERE DS_PARTITION_DATE = '{DATE_REPORT}'
        """)

spark.sql(f"""
        INSERT INTO  user_workbench_fin_priv.fin_bf_rah.rbg_f_forecast_2026_unionall
        SELECT *, '{MONTH_REPORT}' AS MONTH_FORECAST, '{DATE_REPORT}' AS DS_PARTITION_DATE
        from {TBL_FORECAST_2026}
          """)

print(f"=== BACKUP DONE:===")
