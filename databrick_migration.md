# Forecast 2026 Consolidation — Databricks Notebook

Migrate từ `forecast_engine.sql` (SP `sp_Forecast_2026_Consolidate`) theo `code_instruction.md`.

**Cách dùng:** mỗi `# Block N` = 1 cell Python trong notebook. Copy phần code bên trong khung ```` ```python ```` vào cell, chạy tuần tự từ Block 1 → Block 18.

**Trước khi chạy:** sửa các tên bảng có đánh dấu `TODO` ở **Block 1** (bảng chưa tồn tại / chưa xác nhận trên Databricks).

**Style:** mỗi câu SQL trong SP = 1 câu SQL viết **đầy đủ** trong notebook (không ghép từ constant / template) → sửa block nào chỉ cần sửa đúng chỗ đó, thêm measure mới thì copy 1 block tương tự rồi sửa.

| Block | Nội dung | SP gốc (line) | Method |
|---|---|---|---|
| 1 | Config + **tên bảng tập trung** | 13-18, 282-284 | — |
| 2 | Helper functions (Python) | — | Helpers |
| 3 | Pre-check input tables + columns | — | — |
| 4 | STEP 1 — Clean source views | — | M1 |
| 5 | STEP 2a — Create `Forecast_2026_UnionAll` | 22-50 | — |
| 6 | STEP 2b — Dynamic UNION ALL → temp view | 54-121 | M1 |
| 7 | STEP 2c — DELETE + 33 UPDATE (CTE chain) → insert | 123-264 | M2 |
| 8 | STEP 3 — Backup có date stamp | 266-277 | — |
| 9 | STEP 4a — `##FORECAST_CONSOL` | 309-343 | M3 |
| 10 | CALC table + executor (sequential / parallel stage) | 289-307 | M4 |
| 11 | CALC nhóm A — Leaf insert | 346-610 | M4 |
| 12 | CALC nhóm B — Hierarchical rollup | 614-1012 | M4 |
| 13 | CALC nhóm C — `_SUM` + ratio trước FY | 1015-1172 | M4 |
| 14 | CALC nhóm D — FY/MTD/YTD + ratios | 1175-1936 | M4 |
| 15 | CALC nhóm E — Variance | 1978-2072 | M4 |
| 16 | Run CALC — khai báo stage + chạy | — | M4 |
| 17 | PFS VIEW (dynamic PIVOT) | 2095-2142 | M5 |
| 18 | Validation | 2163-2167 | Validation |

> **Quy ước case-insensitive (thay cho CI collation SQL Server):**
> - `Forecast_2026_UnionAll` giữ nguyên case dữ liệu gốc → mọi điều kiện trong Block 7 dùng `UPPER(col) = 'LITERAL'`.
> - Từ CONSOL trở đi (Block 9+), mọi cột dimension được chuẩn hóa `UPPER(RTRIM(...))` 1 lần → toàn bộ SQL CALC dùng literal **viết HOA**. Lý do: SQL Server GROUP BY/JOIN gộp `'Total Deposit'` với `'TOTAL DEPOSIT'`; Spark thì không → nếu không chuẩn hóa, các block ratio (self-join A/B) sẽ tính sai.
> - Hệ quả: giá trị dimension trong `FORECAST_2026_CALC` là chữ HOA. Join với bảng mapping (X, Y, MAPPING_DAYS) chuẩn hóa phía mapping bằng `UPPER(RTRIM(...))`.
> - **Khi thêm measure / điều kiện mới vào CALC: luôn viết literal chữ HOA**, vd `MEASURE IN ('EOP BAL')`.

---

# Block 1: Parameters & Configuration

```python
# ============================================================
# BLOCK 1: PARAMETERS & CONFIGURATION
# Converted from T-SQL DECLARE statements (SP L13-18, L282-284)
# ============================================================
import time
from datetime import datetime
from zoneinfo import ZoneInfo

CATALOG = "user_workbench_fin_priv"
SCHEMA  = "fin_bf_rah_ai"
FQN     = f"{CATALOG}.{SCHEMA}"  # Fully Qualified Namespace

# ╔══════════════════════════════════════════════════════════╗
# ║            TABLE NAMES — THAY THẾ TẠI ĐÂY                ║
# ╚══════════════════════════════════════════════════════════╝

# --- (1) Source product tables (SP: ANALYSIS..[2026_TBL_NAME] + Forecast_2026_RHE) ---
SOURCE_PRODUCT_TABLES = {
    "tbl_cus":          "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_cus",
    "tbl_casa":         "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_casa",
    "tbl_credit_card":  "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_credit_card",
    "tbl_debit_card":   "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_debit_card",
    "tbl_lending":      "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_lending",
    "tbl_household":    "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_household",
    "tbl_td":           "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_td",
    "tbl_bond":         "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_bond",
    "tbl_banca":        "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_banca",
    "tbl_u2fp":         "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_u2fp",
    "tbl_fx":           "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_fx",
    "tbl_other_income": "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_other_income",
    "tbl_sme":          "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_sme",
    "tbl_rhe":          "user_workbench_fin_priv.fin_bf_rbg.rbg_f_forecast_2026_rhe",  # SP L111-121 (insert riêng)
}
RHE_LABEL = "tbl_rhe"   # label của bảng RHE trong dict trên (xử lý INFO_2 / SOURCE_TBL riêng như SP)

# --- (2) Consolidation sources (SP L282-284, L332) ---
TBL_BUDGET_2026 = "user_workbench_fin_priv.fin_bf_rah.rbg_f_budget_2026_unionall"          # SP: ANALYSIS..BUDGET_2026_UNIONALL
TBL_RV_BUDGET   = "user_workbench_fin_priv.fin_bf_rah.rbg_f_revised_budget_2026_unionall"  # SP: ANALYSIS..REVISED_BUDGET_2026_UNIONALL
TBL_FORECAST_LM = "user_workbench_fin_priv.fin_bf_rah.rbg_f_forecast_2026_unionall"        # SP: ANALYSIS..Forecast_2026_UnionAll_<yyyymmdd> (bảng backup, partition theo ngày)
TBL_FORECAST_BACKUP = TBL_FORECAST_LM                                                      # STEP 3 (Block 8) ghi backup vào đây
TBL_BUDGET = TBL_BUDGET_2026                                                               # alias giữ tương thích draft cũ

# --- (3) Mapping / reference tables ---
TBL_MAPPING_X    = f"{FQN}.mapping_2026_X"   # TODO: xác nhận — SP: ANALYSIS..REVISED_BUDGET_2026_X (row mapping)
TBL_MAPPING_Y    = f"{FQN}.FC_2026_Y"        # TODO: xác nhận — SP: Analysis..FC_2026_Y (column mapping)
TBL_MAPPING_DAYS = "user_workbench_fin_priv.fin_bf_rah.rbg_dim_mapping_days"  # SP: ANALYSIS..MAPPING_DAYS

# --- (4) Output tables (notebook tự tạo / ghi đè) ---
TBL_FORECAST_2026 = f"{FQN}.Forecast_2026_UnionAll"         # SP: ANALYSIS..Forecast_2026_UnionAll
TBL_CONSOL        = f"{FQN}.forecast_2026_consol"           # TODO: tên mới — SP: ##FORECAST_CONSOL (global temp)
TBL_CALC          = f"{FQN}.forecast_2026_calc"             # TODO: tên mới — SP: ANALYSIS..FORECAST_2026_CALC
TBL_CALC_STAGE    = f"{FQN}.forecast_2026_calc_stage"       # TODO: tên mới — bảng tạm cho stage parallel (Block 10), không có trong SP
TBL_PFS_VIEW      = f"{FQN}.forecast_2026_pfs_view_total"   # TODO: tên mới — SP: ANALYSIS..FORECAST_2026_PFS_VIEW_TOTAL

# ╔══════════════════════════════════════════════════════════╗
# ║                     PERIOD PARAMETERS                    ║
# ╚══════════════════════════════════════════════════════════╝
START_FC     = "202601"      # FC period begin (SP: chỉ dùng cho PBI)
END_FC       = "202612"      # FC period end
REPORT_MONTH = END_FC        # SP: @REPORT_MONTH
YTD_MONTH    = END_FC        # SP: @YTD_MONTH
UNION_MONTH_FROM = "202401"  # SP L105: MONTH BETWEEN '202401' AND @Report_Month

PARTITION_COL = "DS_PARTITION_DATE"   # cột snapshot của các bảng *_unionall trên Databricks
DATE_FC_LM    = "2026-08-29"          # snapshot Forecast LM (SP: Forecast_2026_UnionAll_20260227)
DATE_RV_BUD   = None                  # None = tự lấy MAX(DS_PARTITION_DATE) của TBL_RV_BUDGET
DATE_BUDGET   = None                  # None = tự lấy MAX(...) nếu TBL_BUDGET_2026 có cột partition; không có cột → không filter

TIMEZONE      = "Asia/Ho_Chi_Minh"    # dùng cho DATE_REPORT của backup (cluster mặc định chạy UTC)
MAX_PARALLEL  = 8                     # số block CALC chạy đồng thời trong stage "parallel" (1 = chạy lần lượt)
DEBUG_COUNT   = False                 # True: đếm thêm rows của view raw ở Block 6 (= scan lại 14 bảng nguồn) — chỉ bật khi debug

# Optimized write: gom file nhỏ khi ghi Delta → các block sau đọc CALC/CONSOL nhanh hơn
DELTA_TBLPROPERTIES = "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')"

print(f"Catalog.Schema   : {FQN}")
print(f"Report Month     : {REPORT_MONTH}")
print(f"YTD Month        : {YTD_MONTH}")
print(f"Forecast UnionAll: {TBL_FORECAST_2026}")
print(f"Forecast LM      : {TBL_FORECAST_LM} @ {DATE_FC_LM}")
print(f"CONSOL / CALC    : {TBL_CONSOL} / {TBL_CALC}")
print(f"PFS VIEW         : {TBL_PFS_VIEW}")
```

# Block 2: Helper Functions

```python
# ============================================================
# BLOCK 2: HELPER FUNCTIONS (Python thuần — không sinh SQL)
# ============================================================

def table_columns(tbl):
    """List tên cột (UPPER, strip) của 1 bảng."""
    return [f.name.strip().upper() for f in spark.table(tbl).schema.fields]


def resolve_snapshot_date(tbl, fixed_date=None, part_col=PARTITION_COL):
    """Ngày snapshot sẽ dùng cho 1 bảng *_unionall.
    - Bảng không có cột partition → None (không filter)
    - fixed_date = None → MAX(part_col)"""
    if part_col.upper() not in table_columns(tbl):
        return None
    d = fixed_date or spark.sql(f"SELECT MAX({part_col}) FROM {tbl}").first()[0]
    if d is None:
        raise ValueError(f"{tbl}: cột {part_col} không có dữ liệu")
    return str(d)


def inserted_rows(result_df):
    """Số rows vừa INSERT, lấy từ DataFrame mà spark.sql('INSERT ...') trả về trên Databricks
    (cột num_inserted_rows / num_affected_rows) → không phải chạy thêm count().
    Trả None nếu runtime không trả các cột này."""
    try:
        row = result_df.first()
    except Exception:
        return None
    if row is None:
        return None
    d = row.asDict()
    for k in ("num_inserted_rows", "num_affected_rows"):
        if d.get(k) is not None:
            return int(d[k])
    return None


print("Helpers loaded: table_columns, resolve_snapshot_date, inserted_rows")
```

# Block 3: Pre-check — Validate All Input Tables & Columns

```python
# ============================================================
# BLOCK 3: PRE-CHECK — validate input tables tồn tại + đủ cột
# ============================================================
input_tables = {
    **SOURCE_PRODUCT_TABLES,                      # STEP 2
    "TBL_BUDGET_2026":  TBL_BUDGET_2026,          # STEP 4a
    "TBL_RV_BUDGET":    TBL_RV_BUDGET,
    "TBL_FORECAST_LM":  TBL_FORECAST_LM,
    "TBL_MAPPING_X":    TBL_MAPPING_X,            # CALC 1.3 + PFS VIEW
    "TBL_MAPPING_Y":    TBL_MAPPING_Y,            # PFS VIEW
    "TBL_MAPPING_DAYS": TBL_MAPPING_DAYS,         # CALC ratio (365/NO_DAY)
}
# Lưu ý: TBL_FORECAST_2026 / CONSOL / CALC / PFS_VIEW là OUTPUT → không check.

missing, exists = [], []
for label, tbl in input_tables.items():
    if spark.catalog.tableExists(tbl):
        try:
            detail = spark.sql(f"DESCRIBE DETAIL {tbl}").select("lastModified").first()[0]
            last_mod = detail.strftime("%Y-%m-%d %H:%M:%S") if detail else "N/A"
        except Exception as e:
            last_mod = f"ERR: {str(e)[:40]}"
        exists.append((label, tbl, last_mod))
    else:
        missing.append((label, tbl))

print(f"\n[OK] {len(exists)} table(s) found:")
print(f"  {'LABEL':<20s}   {'TABLE':<75s}   LAST MODIFIED")
for label, tbl, last_mod in exists:
    print(f"  + {label:<20s} -> {tbl:<75s}   {last_mod}")

if missing:
    print(f"\n[FAIL] {len(missing)} table(s) MISSING:")
    for label, tbl in missing:
        print(f"  X {label:20s} -> {tbl}")
    raise RuntimeError(f"Pre-check FAILED: {len(missing)} table(s) not found.")

# --- Column check cho consol sources + mapping tables ---
CONSOL_SOURCE_COLS = {
    "BIZLINE", "MONTH", "CUS_TYPE", "SEGMENT", "CHANNEL", "PRODUCT_FAMILY", "PRODUCT_LINE",
    "SUB_PRODUCT", "GROUP_MEASURE", "MEASURE", "SUB_MEASURE", "SUB_MEASURE_2", "VALUE",
}
REQUIRED_COLS_BY_TABLE = {
    TBL_BUDGET_2026:  CONSOL_SOURCE_COLS,
    TBL_RV_BUDGET:    CONSOL_SOURCE_COLS,
    TBL_FORECAST_LM:  CONSOL_SOURCE_COLS,
    TBL_MAPPING_X:    {"ID", "BIZLINE", "PRODUCT_LV_4", "PRODUCT_LV_3", "PRODUCT_LV_2", "PRODUCT_LV_1", "MEASURE", "LEVEL"},
    TBL_MAPPING_Y:    {"ID", "YEARMONTH", "TYPE", "CUS_TYPE", "SEGMENT"},
    TBL_MAPPING_DAYS: {"YEARMONTH", "NO_DAY"},
}
col_issues = []
for tbl, req in REQUIRED_COLS_BY_TABLE.items():
    miss = req - set(table_columns(tbl))
    if miss:
        col_issues.append((tbl, sorted(miss)))
        print(f"  ✗ {tbl}: MISSING {sorted(miss)}")
    else:
        print(f"  ✓ {tbl}")

if col_issues:
    raise RuntimeError(f"Pre-check FAILED: {len(col_issues)} table(s) thiếu cột bắt buộc.")

print(f"\n{'=' * 60}\nALL INPUT TABLES AVAILABLE - Safe to proceed.\n{'=' * 60}")
```

# Block 4: STEP 1 — Clean & Standardize Source Tables

```python
# ============================================================
# BLOCK 4 — STEP 1: CLEAN SOURCE TABLES → temp views clean_<table>
# Method 1 (A): normalize column names → UPPER; VALUE → DOUBLE; còn lại → STRING
# ============================================================
VALUE_COL_TYPE = "DOUBLE"

print("=" * 70)
print("CLEANING SOURCE TABLES: Align column names & types")
print("=" * 70)

CLEAN_VIEW_MAP  = {}  # original_table -> view_name
CLEAN_VIEW_COLS = {}  # original_table -> [col_names_upper]

for tbl in SOURCE_PRODUCT_TABLES.values():
    tbl_short = tbl.split(".")[-1]
    view_name = f"clean_{tbl_short}"

    if not spark.catalog.tableExists(tbl):
        print(f"  SKIP: {tbl_short} (does not exist)")
        continue

    schema = spark.table(tbl).schema
    select_parts, fixes_applied, clean_cols = [], [], []

    for field in schema.fields:
        raw_name    = field.name
        clean_name  = raw_name.strip().upper()
        actual_type = field.dataType.simpleString()

        if clean_name.startswith("_"):          # bỏ cột _rescued_data ...
            continue

        col_ref = f"`{raw_name}`"
        target_type = "double" if clean_name == "VALUE" else "string"

        if actual_type != target_type:
            cast_fn = "TRY_CAST" if clean_name == "VALUE" else "CAST"   # VALUE: chịu được chuỗi 'NULL'
            expr = f"{cast_fn}({col_ref} AS {target_type.upper()}) AS `{clean_name}`"
            fixes_applied.append(f"{clean_name}: {actual_type}→{target_type.upper()}")
        elif raw_name != clean_name:
            expr = f"{col_ref} AS `{clean_name}`"
            fixes_applied.append(f"rename: '{raw_name}'→'{clean_name}'")
        else:
            expr = col_ref

        select_parts.append(expr)
        clean_cols.append(clean_name)

    spark.sql(f"CREATE OR REPLACE TEMP VIEW {view_name} AS SELECT {', '.join(select_parts)} FROM {tbl}")
    CLEAN_VIEW_MAP[tbl]  = view_name
    CLEAN_VIEW_COLS[tbl] = clean_cols

    msg = f"fixes: {', '.join(fixes_applied)}" if fixes_applied else "(no fixes needed)"
    print(f"  ✓ {tbl_short:<50} {msg}")

print(f"\nDONE: {len(CLEAN_VIEW_MAP)} cleaned temp views created.")

# ============================================================
# MISSING COLUMN CHECK — mỗi bảng sản phẩm có đủ cột cho STEP 2 không
# ============================================================
REQUIRED_COLS = {
    "MONTH", "BIZLINE", "CUS_TYPE", "SEGMENT", "CHANNEL",
    "CUSTOMER_TIER", "PRODUCT_FAMILY", "PRODUCT_LINE",
    "SUB_PRODUCT", "GROUP_MEASURE", "MEASURE",
    "SUB_MEASURE", "SUB_MEASURE_2", "VALUE",
}
# Cột có default trong STEP 2 (vắng mặt cũng OK — trừ khi được chọn làm INFO_1/INFO_2, xem Block 6)
OPTIONAL_COLS = {
    "SUB_CHANNEL", "PERSONA", "S_FLAG", "INTERNAL", "CCY",
    "INFO_1", "INFO_2", "MTB_FLAG", "BANCA_CHANNEL", "PAYMENT_MODE",
}

print(f"\n{'=' * 70}\nCOLUMN COMPLETENESS CHECK vs STEP 2 requirements\n{'=' * 70}")
has_issues = False
for tbl in SOURCE_PRODUCT_TABLES.values():
    tbl_short = tbl.split(".")[-1]
    if tbl not in CLEAN_VIEW_COLS:
        print(f"  SKIP: {tbl_short} (table not found)")
        continue
    cols = set(CLEAN_VIEW_COLS[tbl])
    missing_required = REQUIRED_COLS - cols
    missing_optional = OPTIONAL_COLS - cols
    if missing_required:
        has_issues = True
        print(f"  ✗ {tbl_short}\n      MISSING REQUIRED : {sorted(missing_required)}")
    else:
        opt_msg = f"  (optional absent: {sorted(missing_optional)})" if missing_optional else ""
        print(f"  ✓ {tbl_short}{opt_msg}")

print("=" * 70)
if has_issues:
    raise RuntimeError("Fix tables with missing REQUIRED columns before STEP 2.")
print("ALL source tables have required columns — safe to proceed to STEP 2.")
```

# Block 5: STEP 2a — Create Forecast_2026_UnionAll Table

```python
# ============================================================
# BLOCK 5 — STEP 2a: CREATE TABLE Forecast_2026_UnionAll
# SP L22-50: CREATE TABLE ANALYSIS..Forecast_2026_UnionAll
# ============================================================
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
    {DELTA_TBLPROPERTIES}
""")

print(f"=== Table created: {TBL_FORECAST_2026} ===")
```

# Block 6: STEP 2b — Dynamic UNION ALL from Product Tables

```python
# ============================================================
# BLOCK 6 — STEP 2b: UNION ALL các bảng sản phẩm → temp view v_forecast_2026_raw
# SP L54-121: WHILE loop + EXEC(INSERT ...) + insert riêng RHE
# Method 1: Dynamic Union (cột thay đổi theo schema từng bảng — giống EXEC dynamic SQL trong SP)
# Chưa ghi Delta ở bước này — Block 7 clean xong mới INSERT 1 lần.
# ============================================================
select_clauses, skipped_tables = [], []


def _pick(col, cols, tbl_name):
    """SP dùng thẳng tên cột trong EXEC → thiếu cột thì SP lỗi. Ở đây báo lỗi rõ ràng."""
    if col not in cols:
        raise ValueError(f"{tbl_name}: thiếu cột {col} (SP gốc cũng sẽ lỗi)")
    return col


for idx, (label, full_tbl) in enumerate(SOURCE_PRODUCT_TABLES.items(), start=1):
    tbl_name = full_tbl.split(".")[-1]
    if full_tbl not in CLEAN_VIEW_MAP:
        print(f"WARNING [{idx}]: {tbl_name} not cleaned in STEP 1 - SKIPPED")
        skipped_tables.append(full_tbl)
        continue

    use_tbl   = CLEAN_VIEW_MAP[full_tbl]
    cols      = CLEAN_VIEW_COLS[full_tbl]
    tbl_upper = tbl_name.upper()
    is_rhe    = (label == RHE_LABEL)

    # SP L71-72: @Tbl_Name LIKE '%TD' → INTERNAL / CCY lấy cột thật
    col_internal = "INTERNAL" if tbl_upper.endswith("TD") else "''"
    col_ccy      = "CCY"      if tbl_upper.endswith("TD") else "''"
    # SP L73-78: cột không tồn tại → default
    col_s_flag      = "S_FLAG"      if "S_FLAG" in cols      else "1"
    col_persona     = "PERSONA"     if "PERSONA" in cols     else "'All'"
    col_sub_channel = "SUB_CHANNEL" if "SUB_CHANNEL" in cols else "'All'"

    if is_rhe:
        # SP L111-121: RHE insert riêng — INFO_1 = '', INFO_2 = cột INFO_2, SOURCE_TBL = 'RHE'
        col_info_1 = "''"
        col_info_2 = _pick("INFO_2", cols, tbl_name)
        source_tbl = "RHE"
    else:
        # SP L79-90: INFO_1 / INFO_2 theo tên bảng
        if "BANCA" in tbl_upper:
            # NOTE: SP gốc viết LIKE '% BANCA%' (có dấu cách) cho INFO_2 → nhiều khả năng INFO_2 luôn = ''.
            #       Draft giữ PAYMENT_MODE theo ý đồ nghiệp vụ — đổi thành "''" nếu muốn khớp 100% SP.
            col_info_1 = _pick("BANCA_CHANNEL", cols, tbl_name)
            col_info_2 = _pick("PAYMENT_MODE", cols, tbl_name)
        elif "CASA" in tbl_upper or "CREDIT_CARD" in tbl_upper:
            col_info_1, col_info_2 = _pick("MTB_FLAG", cols, tbl_name), "''"
        elif "LENDING" in tbl_upper or "SME_MSME" in tbl_upper:
            col_info_1, col_info_2 = _pick("INFO_1", cols, tbl_name), "''"
        else:
            col_info_1, col_info_2 = "''", "''"
        source_tbl = tbl_name

    # SP L104: WHERE VALUE <> 0 AND S_FLAG <> 0
    s_flag_filter = f"AND TRY_CAST({col_s_flag} AS DOUBLE) <> 0" if col_s_flag == "S_FLAG" else ""

    select_clauses.append(f"""
    SELECT CAST(`MONTH` AS STRING)          AS `MONTH`
          ,BIZLINE
          ,CUS_TYPE
          ,SEGMENT
          ,CHANNEL
          ,{col_sub_channel}                AS SUB_CHANNEL
          ,CUSTOMER_TIER
          ,{col_persona}                    AS PERSONA
          ,PRODUCT_FAMILY
          ,PRODUCT_LINE
          ,SUB_PRODUCT
          ,GROUP_MEASURE
          ,MEASURE
          ,SUB_MEASURE
          ,SUB_MEASURE_2
          ,{col_internal}                   AS INTERNAL
          ,{col_ccy}                        AS CCY
          ,CAST({col_s_flag} AS STRING)     AS S_FLAG
          ,{col_info_1}                     AS INFO_1
          ,{col_info_2}                     AS INFO_2
          ,TRY_CAST(`VALUE` AS DOUBLE)      AS `VALUE`
          ,'{source_tbl}'                   AS SOURCE_TBL
    FROM {use_tbl}
    WHERE TRY_CAST(`VALUE` AS DOUBLE) <> 0
          {s_flag_filter}
          AND CAST(`MONTH` AS STRING) BETWEEN '{UNION_MONTH_FROM}' AND '{REPORT_MONTH}'
    """)
    print(f"Added [{idx:2d}]: {tbl_name:<45s} INTERNAL={col_internal:<8s} CCY={col_ccy:<4s} "
          f"S_FLAG={col_s_flag:<6s} INFO_1={col_info_1:<13s} INFO_2={col_info_2}")

if skipped_tables:
    print(f"\nWARNING: {len(skipped_tables)} table(s) skipped: {skipped_tables}")

union_query = "\nUNION ALL\n".join(select_clauses)
spark.sql(f"CREATE OR REPLACE TEMP VIEW v_forecast_2026_raw AS\n{union_query}")

# count view = scan lại toàn bộ 14 bảng nguồn → chỉ chạy khi DEBUG_COUNT = True
raw_cnt = spark.table("v_forecast_2026_raw").count() if DEBUG_COUNT else None
raw_msg = f" = {raw_cnt:,} rows" if raw_cnt is not None else ""
print(f"\n=== STEP 2b COMPLETE: v_forecast_2026_raw{raw_msg} from {len(select_clauses)} tables ===")
```

# Block 7: STEP 2c — Clean Data & Update Format (DELETE + 33 UPDATE)

```python
# ============================================================
# BLOCK 7 — STEP 2c: CLEAN DATA & UPDATE FORMAT
# SP L123-264: 1 DELETE + 33 UPDATE tuần tự trên Forecast_2026_UnionAll
# Method 2 (SQL version): CTE chain s00 → s33, mỗi step = 1 câu UPDATE gốc, GIỮ ĐÚNG THỨ TỰ
#   (vd: L156 'Prix' đọc SUB_MEASURE sau khi L150 đã shift; L230 đọc PRODUCT_LINE='NFX' sau L227).
#
# Pattern mỗi step (đọc như 1 câu UPDATE):
#   sNN AS (
#     SELECT * EXCEPT (_match, <các cột SET>),
#            CASE WHEN _match THEN <giá trị mới> ELSE <cột> END AS <cột>     -- = SET
#     FROM (SELECT *, (<điều kiện>) AS _match FROM s<NN-1>)                  -- = WHERE
#   )
#   → mọi CASE trong 1 step đọc giá trị của step trước, đúng ngữ nghĩa `SET a = b, b = c` của SQL Server.
#   → thêm UPDATE mới: copy 1 step, đổi tên sNN, sửa điều kiện/SET, đổi FROM của step kế tiếp + SELECT cuối.
#
# CI collation: so sánh UPPER(col) với literal viết HOA. Giá trị SET giữ nguyên case như SP.
# ============================================================
spark.sql("""
CREATE OR REPLACE TEMP VIEW v_forecast_2026_clean AS
WITH
-- SP L126-129: DELETE UNUSED DATA (giữ row khi điều kiện DELETE = NULL, giống SQL Server)
s00 AS (
  SELECT * FROM v_forecast_2026_raw
  WHERE NOT COALESCE((
           UPPER(MEASURE) IN ('EOP TICKET SIZE','%UNIQUE CUS HAVING NEW CONTRACT - BOND','%UNIQUE CUS HAVING NEW CONTRACT - ALL','% ACTIVE CUS'
                             ,'% CI','% CI_ADJ','% EOP BAL/AVG BAL','% FTP','% NIM','AVG BAL/ACTIVE CUS','BOP CUS','NFI/ ACTIVE CUS','NUMBER TNX/ACTIVE CUS')
        OR (UPPER(MEASURE) IN ('UNIQUE CUS HAVING NEW CONTRACT') AND UPPER(SUB_PRODUCT) IN ('BOND','FUND'))
  ), FALSE)
),
-- SP L133-138: BOND-FUND / Structured Notes 'EOP Cus by product' → 'EOP Cus'
s01 AS (
  SELECT * EXCEPT (_match, GROUP_MEASURE, MEASURE, SUB_MEASURE, SUB_MEASURE_2),
         CASE WHEN _match THEN 'EOP Cus' ELSE GROUP_MEASURE END AS GROUP_MEASURE,
         CASE WHEN _match THEN 'EOP Cus' ELSE MEASURE       END AS MEASURE,
         CASE WHEN _match THEN 'EOP Cus' ELSE SUB_MEASURE   END AS SUB_MEASURE,
         CASE WHEN _match THEN 'EOP Cus' ELSE SUB_MEASURE_2 END AS SUB_MEASURE_2
  FROM (SELECT *, (UPPER(BIZLINE) = 'RETAIL' AND UPPER(PRODUCT_LINE) IN ('BOND-FUND','STRUCTURED NOTES')
                   AND UPPER(SUB_PRODUCT) IN ('BOND-FUND','STRUCTURED NOTES') AND UPPER(MEASURE) = 'EOP CUS BY PRODUCT') AS _match
        FROM s00)
),
-- SP L140-142: BANCA INFO_2 bỏ ký tự '0'
s02 AS (
  SELECT * EXCEPT (_match, INFO_2),
         CASE WHEN _match THEN REPLACE(INFO_2, '0', '') ELSE INFO_2 END AS INFO_2
  FROM (SELECT *, (UPPER(PRODUCT_LINE) = 'BANCA' AND INFO_2 LIKE '%0%') AS _match
        FROM s01)
),
-- SP L144-148: Details of Card FI/FE — shift MEASURE = GROUP_MEASURE, SUB_MEASURE = MEASURE, SUB_MEASURE_2 = SUB_MEASURE
s03 AS (
  SELECT * EXCEPT (_match, MEASURE, SUB_MEASURE, SUB_MEASURE_2),
         CASE WHEN _match THEN GROUP_MEASURE ELSE MEASURE       END AS MEASURE,
         CASE WHEN _match THEN MEASURE       ELSE SUB_MEASURE   END AS SUB_MEASURE,
         CASE WHEN _match THEN SUB_MEASURE   ELSE SUB_MEASURE_2 END AS SUB_MEASURE_2
  FROM (SELECT *, (UPPER(PRODUCT_FAMILY) = 'CARD' AND UPPER(GROUP_MEASURE) = 'NFI' AND UPPER(MEASURE) IN ('FI','FE')) AS _match
        FROM s02)
),
-- SP L150-154: Details of BOND-FUND NFI — shift như trên
s04 AS (
  SELECT * EXCEPT (_match, MEASURE, SUB_MEASURE, SUB_MEASURE_2),
         CASE WHEN _match THEN GROUP_MEASURE ELSE MEASURE       END AS MEASURE,
         CASE WHEN _match THEN MEASURE       ELSE SUB_MEASURE   END AS SUB_MEASURE,
         CASE WHEN _match THEN SUB_MEASURE   ELSE SUB_MEASURE_2 END AS SUB_MEASURE_2
  FROM (SELECT *, (UPPER(PRODUCT_LINE) = 'BOND-FUND' AND UPPER(GROUP_MEASURE) = 'NFI') AS _match
        FROM s03)
),
-- SP L156-158: BOND-FUND Secondary Fee → SUB_PRODUCT 'Prix' (đọc SUB_MEASURE sau khi s04 đã shift)
s05 AS (
  SELECT * EXCEPT (_match, SUB_PRODUCT),
         CASE WHEN _match THEN 'Prix' ELSE SUB_PRODUCT END AS SUB_PRODUCT
  FROM (SELECT *, (UPPER(PRODUCT_LINE) = 'BOND-FUND' AND UPPER(SUB_MEASURE) = 'INCENTIVE FEE'
                   AND UPPER(SUB_MEASURE_2) = 'SECONDARY FEE' AND UPPER(SUB_PRODUCT) = 'OTHERS') AS _match
        FROM s04)
),
-- SP L160-163: TD Liquidity Cost
s06 AS (
  SELECT * EXCEPT (_match, SUB_MEASURE, SUB_MEASURE_2),
         CASE WHEN _match THEN 'CI'             ELSE SUB_MEASURE   END AS SUB_MEASURE,
         CASE WHEN _match THEN 'Liquidity Cost' ELSE SUB_MEASURE_2 END AS SUB_MEASURE_2
  FROM (SELECT *, (UPPER(PRODUCT_FAMILY) = 'TIME DEPOSIT' AND UPPER(GROUP_MEASURE) = 'NII'
                   AND UPPER(SUB_MEASURE) IN ('LIQUIDITY COST','LIQUIDITY_COST')) AS _match
        FROM s05)
),
-- SP L165-168: TD / Lending NFI → FI
s07 AS (
  SELECT * EXCEPT (_match, SUB_MEASURE, SUB_MEASURE_2),
         CASE WHEN _match THEN 'FI' ELSE SUB_MEASURE   END AS SUB_MEASURE,
         CASE WHEN _match THEN 'FI' ELSE SUB_MEASURE_2 END AS SUB_MEASURE_2
  FROM (SELECT *, (UPPER(PRODUCT_FAMILY) IN ('TIME DEPOSIT','LENDING') AND UPPER(SUB_MEASURE) = 'NFI') AS _match
        FROM s06)
),
-- SP L170-172: Lending NII
s08 AS (
  SELECT * EXCEPT (_match, MEASURE),
         CASE WHEN _match THEN 'NII' ELSE MEASURE END AS MEASURE
  FROM (SELECT *, (UPPER(PRODUCT_FAMILY) IN ('LENDING') AND UPPER(GROUP_MEASURE) = 'NII' AND UPPER(MEASURE) <> 'NII') AS _match
        FROM s07)
),
-- SP L174-176: Lending / Card NFI
s09 AS (
  SELECT * EXCEPT (_match, MEASURE),
         CASE WHEN _match THEN 'NFI' ELSE MEASURE END AS MEASURE
  FROM (SELECT *, (UPPER(PRODUCT_FAMILY) IN ('LENDING','CARD') AND UPPER(GROUP_MEASURE) = 'NFI' AND UPPER(MEASURE) <> 'NFI') AS _match
        FROM s08)
),
-- SP L178-183: TD 'EOP Cus Distinct' → 'EOP Cus'
s10 AS (
  SELECT * EXCEPT (_match, GROUP_MEASURE, MEASURE, SUB_MEASURE, SUB_MEASURE_2),
         CASE WHEN _match THEN 'EOP Cus' ELSE GROUP_MEASURE END AS GROUP_MEASURE,
         CASE WHEN _match THEN 'EOP Cus' ELSE MEASURE       END AS MEASURE,
         CASE WHEN _match THEN 'EOP Cus' ELSE SUB_MEASURE   END AS SUB_MEASURE,
         CASE WHEN _match THEN 'EOP Cus' ELSE SUB_MEASURE_2 END AS SUB_MEASURE_2
  FROM (SELECT *, (UPPER(PRODUCT_FAMILY) IN ('TIME DEPOSIT') AND UPPER(PRODUCT_LINE) = 'ALL' AND UPPER(MEASURE) = 'EOP CUS DISTINCT') AS _match
        FROM s09)
),
-- SP L185-186: CCY 'All' → ''
s11 AS (
  SELECT * EXCEPT (_match, CCY),
         CASE WHEN _match THEN '' ELSE CCY END AS CCY
  FROM (SELECT *, (UPPER(CCY) = 'ALL') AS _match
        FROM s10)
),
-- SP L188-189: CUSTOMER_TIER → 'Others'
s12 AS (
  SELECT * EXCEPT (_match, CUSTOMER_TIER),
         CASE WHEN _match THEN 'Others' ELSE CUSTOMER_TIER END AS CUSTOMER_TIER
  FROM (SELECT *, (UPPER(CUSTOMER_TIER) = 'ALL' OR CUSTOMER_TIER IS NULL OR UPPER(CUSTOMER_TIER) = 'NULL') AS _match
        FROM s11)
),
-- SP L191-192: CHANNEL → 'Others'
s13 AS (
  SELECT * EXCEPT (_match, CHANNEL),
         CASE WHEN _match THEN 'Others' ELSE CHANNEL END AS CHANNEL
  FROM (SELECT *, (UPPER(CHANNEL) = 'ALL' OR UPPER(CHANNEL) NOT IN ('BRANCH','DEVELOPER','PARTNERSHIP','DIGITAL','WINLIFE','OTHERS')) AS _match
        FROM s12)
),
-- SP L194-195: Loans to CIs → Other Loans
s14 AS (
  SELECT * EXCEPT (_match, PRODUCT_LINE, SUB_PRODUCT),
         CASE WHEN _match THEN 'Other Loans' ELSE PRODUCT_LINE END AS PRODUCT_LINE,
         CASE WHEN _match THEN 'Other Loans' ELSE SUB_PRODUCT  END AS SUB_PRODUCT
  FROM (SELECT *, (UPPER(SUB_PRODUCT) IN ('LOANS TO CIS')) AS _match
        FROM s13)
),
-- SP L197-198: ETB → Existing
s15 AS (
  SELECT * EXCEPT (_match, CUS_TYPE),
         CASE WHEN _match THEN 'Existing' ELSE CUS_TYPE END AS CUS_TYPE
  FROM (SELECT *, (UPPER(CUS_TYPE) = 'ETB') AS _match
        FROM s14)
),
-- SP L200-201: UPDATE BUDGET_2026_UNIONALL (không phải Forecast) → chuyển sang CONSOL Block 9 (step s01)
-- SP L203-204: QR Merchant
s16 AS (
  SELECT * EXCEPT (_match, PRODUCT_FAMILY),
         CASE WHEN _match THEN 'QR Merchant' ELSE PRODUCT_FAMILY END AS PRODUCT_FAMILY
  FROM (SELECT *, (UPPER(PRODUCT_LINE) = 'QR MERCHANT') AS _match
        FROM s15)
),
-- SP L206-207: Trading Income
s17 AS (
  SELECT * EXCEPT (_match, SUB_PRODUCT),
         CASE WHEN _match THEN 'Other Trading Income' ELSE SUB_PRODUCT END AS SUB_PRODUCT
  FROM (SELECT *, (UPPER(PRODUCT_LINE) = 'TRADING INCOME') AS _match
        FROM s16)
),
-- SP L208-210: UPDATE IN 20251122 — FX Flow Retail Ticket size
s18 AS (
  SELECT * EXCEPT (_match, MEASURE, SUB_MEASURE),
         CASE WHEN _match THEN 'Ticket size FX' ELSE MEASURE     END AS MEASURE,
         CASE WHEN _match THEN 'Ticket size FX' ELSE SUB_MEASURE END AS SUB_MEASURE
  FROM (SELECT *, (UPPER(PRODUCT_LINE) = 'FX FLOW' AND UPPER(BIZLINE) = 'RETAIL') AS _match
        FROM s17)
),
-- SP L212-213: CASA&AE MTB Cus
s19 AS (
  SELECT * EXCEPT (_match, SUB_MEASURE_2),
         CASE WHEN _match THEN 'MTB Cus' ELSE SUB_MEASURE_2 END AS SUB_MEASURE_2
  FROM (SELECT *, (UPPER(PRODUCT_LINE) = 'CASA&AE' AND UPPER(MEASURE) = 'EOP CUS' AND UPPER(INFO_1) = 'MTB' AND UPPER(BIZLINE) = 'RETAIL') AS _match
        FROM s18)
),
-- SP L215-216: CASA&AE Truly Active Cus
s20 AS (
  SELECT * EXCEPT (_match, SUB_MEASURE_2),
         CASE WHEN _match THEN 'Truly Active Cus' ELSE SUB_MEASURE_2 END AS SUB_MEASURE_2
  FROM (SELECT *, (UPPER(PRODUCT_LINE) = 'CASA&AE' AND UPPER(MEASURE) = 'EOP CUS' AND UPPER(INFO_1) = 'TRULY ACTIVE' AND UPPER(BIZLINE) = 'RETAIL') AS _match
        FROM s19)
),
-- SP L218-219: CASA&AE Low Active Cus
s21 AS (
  SELECT * EXCEPT (_match, SUB_MEASURE_2),
         CASE WHEN _match THEN 'Low Active Cus' ELSE SUB_MEASURE_2 END AS SUB_MEASURE_2
  FROM (SELECT *, (UPPER(PRODUCT_LINE) = 'CASA&AE' AND UPPER(MEASURE) = 'EOP CUS' AND UPPER(INFO_1) = 'LOW ACTIVE' AND UPPER(BIZLINE) = 'RETAIL') AS _match
        FROM s20)
),
-- SP L221-222: CD Self trade → CDST
s22 AS (
  SELECT * EXCEPT (_match, SUB_PRODUCT),
         CASE WHEN _match THEN 'CDST' ELSE SUB_PRODUCT END AS SUB_PRODUCT
  FROM (SELECT *, (UPPER(SUB_PRODUCT) = 'CD SELF TRADE' AND UPPER(BIZLINE) = 'RETAIL') AS _match
        FROM s21)
),
-- SP L224-225: NFX → Fee base (Retail)
s23 AS (
  SELECT * EXCEPT (_match, PRODUCT_FAMILY),
         CASE WHEN _match THEN 'Fee base' ELSE PRODUCT_FAMILY END AS PRODUCT_FAMILY
  FROM (SELECT *, (UPPER(PRODUCT_FAMILY) = 'NFX' AND UPPER(BIZLINE) = 'RETAIL') AS _match
        FROM s22)
),
-- SP L227-228: FX Flow → NFX (Retail)
s24 AS (
  SELECT * EXCEPT (_match, PRODUCT_LINE),
         CASE WHEN _match THEN 'NFX' ELSE PRODUCT_LINE END AS PRODUCT_LINE
  FROM (SELECT *, (UPPER(PRODUCT_LINE) = 'FX FLOW' AND UPPER(BIZLINE) = 'RETAIL') AS _match
        FROM s23)
),
-- SP L230-232: NFX Txn_amt → TXN Amt (đọc PRODUCT_LINE sau s24)
s25 AS (
  SELECT * EXCEPT (_match, MEASURE, SUB_MEASURE, SUB_MEASURE_2),
         CASE WHEN _match THEN 'TXN Amt' ELSE MEASURE       END AS MEASURE,
         CASE WHEN _match THEN 'TXN Amt' ELSE SUB_MEASURE   END AS SUB_MEASURE,
         CASE WHEN _match THEN 'TXN Amt' ELSE SUB_MEASURE_2 END AS SUB_MEASURE_2
  FROM (SELECT *, (UPPER(BIZLINE) = 'RETAIL' AND UPPER(MEASURE) = 'TXN_AMT' AND UPPER(PRODUCT_LINE) = 'NFX') AS _match
        FROM s24)
),
-- SP L234-236: NFX Ticket size → Ticket size FX
s26 AS (
  SELECT * EXCEPT (_match, MEASURE, SUB_MEASURE, SUB_MEASURE_2),
         CASE WHEN _match THEN 'Ticket size FX' ELSE MEASURE       END AS MEASURE,
         CASE WHEN _match THEN 'Ticket size FX' ELSE SUB_MEASURE   END AS SUB_MEASURE,
         CASE WHEN _match THEN 'Ticket size FX' ELSE SUB_MEASURE_2 END AS SUB_MEASURE_2
  FROM (SELECT *, (UPPER(BIZLINE) = 'RETAIL' AND UPPER(MEASURE) = 'TICKET SIZE' AND UPPER(PRODUCT_LINE) = 'NFX') AS _match
        FROM s25)
),
-- SP L238-240: DPM AUM → EOP Bal
s27 AS (
  SELECT * EXCEPT (_match, MEASURE, SUB_MEASURE, SUB_MEASURE_2),
         CASE WHEN _match THEN 'EOP Bal' ELSE MEASURE       END AS MEASURE,
         CASE WHEN _match THEN 'EOP Bal' ELSE SUB_MEASURE   END AS SUB_MEASURE,
         CASE WHEN _match THEN 'EOP Bal' ELSE SUB_MEASURE_2 END AS SUB_MEASURE_2
  FROM (SELECT *, (UPPER(BIZLINE) = 'RETAIL' AND UPPER(MEASURE) = 'AUM' AND UPPER(PRODUCT_LINE) = 'DPM') AS _match
        FROM s26)
),
-- SP L242-244: Bond-Fund NFI → SUB_MEASURE_2 'NFI'
s28 AS (
  SELECT * EXCEPT (_match, SUB_MEASURE_2),
         CASE WHEN _match THEN 'NFI' ELSE SUB_MEASURE_2 END AS SUB_MEASURE_2
  FROM (SELECT *, (UPPER(PRODUCT_LINE) = 'BOND-FUND' AND UPPER(MEASURE) = 'NFI') AS _match
        FROM s27)
),
-- SP L246-248: Other Fee NFI → SUB_MEASURE 'FI'
s29 AS (
  SELECT * EXCEPT (_match, SUB_MEASURE),
         CASE WHEN _match THEN 'FI' ELSE SUB_MEASURE END AS SUB_MEASURE
  FROM (SELECT *, (UPPER(PRODUCT_LINE) = 'OTHER FEE' AND UPPER(MEASURE) = 'NFI') AS _match
        FROM s28)
),
-- SP L250-252: FX Flow → NFX (mọi bizline)
s30 AS (
  SELECT * EXCEPT (_match, PRODUCT_LINE),
         CASE WHEN _match THEN 'NFX' ELSE PRODUCT_LINE END AS PRODUCT_LINE
  FROM (SELECT *, (UPPER(PRODUCT_LINE) = 'FX FLOW') AS _match
        FROM s29)
),
-- SP L254-256: SME 'EOP Cus Active' → 'Active Cus'
s31 AS (
  SELECT * EXCEPT (_match, MEASURE, SUB_MEASURE, SUB_MEASURE_2),
         CASE WHEN _match THEN 'Active Cus' ELSE MEASURE       END AS MEASURE,
         CASE WHEN _match THEN 'Active Cus' ELSE SUB_MEASURE   END AS SUB_MEASURE,
         CASE WHEN _match THEN 'Active Cus' ELSE SUB_MEASURE_2 END AS SUB_MEASURE_2
  FROM (SELECT *, (UPPER(SUB_MEASURE) = 'EOP CUS ACTIVE' AND UPPER(BIZLINE) = 'SME') AS _match
        FROM s30)
),
-- SP L258-260: UPDATE 20260226 — Bond / Bond-Fund NFI → FI
s32 AS (
  SELECT * EXCEPT (_match, SUB_MEASURE),
         CASE WHEN _match THEN 'FI' ELSE SUB_MEASURE END AS SUB_MEASURE
  FROM (SELECT *, (UPPER(SUB_MEASURE) = 'NFI' AND UPPER(PRODUCT_LINE) IN ('BOND','BOND-FUND')) AS _match
        FROM s31)
),
-- SP L262-264: UPDATE 20260226 — SME CA (NFI) → CASA
s33 AS (
  SELECT * EXCEPT (_match, PRODUCT_LINE),
         CASE WHEN _match THEN 'CASA' ELSE PRODUCT_LINE END AS PRODUCT_LINE
  FROM (SELECT *, (UPPER(GROUP_MEASURE) = 'NFI' AND UPPER(PRODUCT_LINE) IN ('CA') AND UPPER(BIZLINE) = 'SME') AS _match
        FROM s32)
)
SELECT `MONTH`, BIZLINE, CUS_TYPE, SEGMENT, CHANNEL, SUB_CHANNEL, CUSTOMER_TIER, PERSONA
      ,PRODUCT_FAMILY, PRODUCT_LINE, SUB_PRODUCT, GROUP_MEASURE, MEASURE, SUB_MEASURE, SUB_MEASURE_2
      ,INTERNAL, CCY, S_FLAG, INFO_1, INFO_2, `VALUE`, SOURCE_TBL
FROM s33
""")

res = spark.sql(f"""
    INSERT INTO {TBL_FORECAST_2026}
    SELECT * FROM v_forecast_2026_clean
""")

row_count = inserted_rows(res)
if row_count is None:
    row_count = spark.table(TBL_FORECAST_2026).count()   # fallback: count(*) không filter → Delta đọc từ metadata
msg = f"{TBL_FORECAST_2026} = {row_count:,} rows (1 transaction)"
if raw_cnt is not None:
    msg = f"raw {raw_cnt:,} → deleted {raw_cnt - row_count:,} → {msg}"
print(f"=== STEP 2c COMPLETE: {msg} ===")
```

# Block 8: STEP 3 — Backup with Date Stamp

```python
# ============================================================
# BLOCK 8 — STEP 3: BACKUP Forecast_2026_UnionAll theo ngày
# SP L266-275: SELECT * INTO Forecast_2026_UnionAll_<yyyymmdd>
# Databricks: 1 bảng backup partition theo DS_PARTITION_DATE (DELETE partition hôm nay → INSERT lại)
# ============================================================
_now = datetime.now(ZoneInfo(TIMEZONE))
DATE_REPORT  = _now.strftime("%Y-%m-%d")
MONTH_REPORT = _now.strftime("%Y%m")

if TBL_FORECAST_BACKUP == TBL_FORECAST_LM and DATE_REPORT == str(DATE_FC_LM):
    print(f"⚠ WARNING: DATE_REPORT = DATE_FC_LM = {DATE_REPORT} → snapshot Forecast LM sẽ bị ghi đè bằng forecast hiện tại!")

spark.sql(f"""
    DELETE FROM {TBL_FORECAST_BACKUP}
    WHERE {PARTITION_COL} = '{DATE_REPORT}'
""")

res = spark.sql(f"""
    INSERT INTO {TBL_FORECAST_BACKUP}
    SELECT *, '{MONTH_REPORT}' AS MONTH_FORECAST, '{DATE_REPORT}' AS {PARTITION_COL}
    FROM {TBL_FORECAST_2026}
""")

bk_cnt = inserted_rows(res)
print(f"=== BACKUP DONE: {TBL_FORECAST_BACKUP} @ {DATE_REPORT} = {bk_cnt:,} rows ===" if bk_cnt is not None
      else f"=== BACKUP DONE: {TBL_FORECAST_BACKUP} @ {DATE_REPORT} ===")
```

# Block 9: STEP 4a — Consolidate Budget / Forecast / Forecast LM / Revised Budget

```python
# ============================================================
# BLOCK 9 — STEP 4a: CONSOL RAW DATA (SP L309-343: ##FORECAST_CONSOL)
# Method 3: UNION nhiều nguồn + TYPE discriminator + post-union UPDATE → Delta table TBL_CONSOL
# Chuẩn hóa UPPER(RTRIM(...)) cho mọi dimension → các block CALC dùng literal viết HOA
#   (mô phỏng CI collation + trailing-space rule của SQL Server cho GROUP BY / JOIN / WHERE).
# ============================================================
date_budget = resolve_snapshot_date(TBL_BUDGET_2026, DATE_BUDGET)
date_rv     = resolve_snapshot_date(TBL_RV_BUDGET,   DATE_RV_BUD)
date_lm     = resolve_snapshot_date(TBL_FORECAST_LM, DATE_FC_LM)

# Bảng budget có thể không có cột partition → bỏ filter
budget_partition_filter = f"AND {PARTITION_COL} = '{date_budget}'" if date_budget else ""

print(f"BUDGET snapshot         : {date_budget or '(không có cột partition — lấy toàn bảng)'}")
print(f"REVISED_BUDGET snapshot : {date_rv}")
print(f"FORECAST_LM snapshot    : {date_lm}")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_forecast_consol AS
WITH
-- SP L316-334: SELECT * INTO ##FORECAST_CONSOL FROM (BUDGET UNION ALL FORECAST UNION ALL FORECAST_LM UNION ALL REVISED_BUDGET)
src AS (
    -- BUDGET (SP L318-320)
    SELECT CAST(`MONTH` AS STRING)                     AS `MONTH`
          ,'BUDGET'                                    AS TYPE
          ,UPPER(RTRIM(CAST(BIZLINE        AS STRING))) AS BIZLINE
          ,UPPER(RTRIM(CAST(CUS_TYPE       AS STRING))) AS CUS_TYPE
          ,UPPER(RTRIM(CAST(SEGMENT        AS STRING))) AS SEGMENT
          ,''                                          AS SUB_CHANNEL
          ,UPPER(RTRIM(CAST(CHANNEL        AS STRING))) AS CHANNEL
          ,UPPER(RTRIM(CAST(PRODUCT_FAMILY AS STRING))) AS PRODUCT_FAMILY
          ,UPPER(RTRIM(CAST(PRODUCT_LINE   AS STRING))) AS PRODUCT_LINE
          ,UPPER(RTRIM(CAST(SUB_PRODUCT    AS STRING))) AS SUB_PRODUCT
          ,UPPER(RTRIM(CAST(GROUP_MEASURE  AS STRING))) AS GROUP_MEASURE
          ,UPPER(RTRIM(CAST(MEASURE        AS STRING))) AS MEASURE
          ,UPPER(RTRIM(CAST(SUB_MEASURE    AS STRING))) AS SUB_MEASURE
          ,UPPER(RTRIM(CAST(SUB_MEASURE_2  AS STRING))) AS SUB_MEASURE_2
          ,TRY_CAST(`VALUE` AS DOUBLE)                 AS `VALUE`
    FROM {TBL_BUDGET_2026}
    WHERE CAST(`MONTH` AS STRING) LIKE '2026%'
          {budget_partition_filter}

    UNION ALL
    -- FORECAST (SP L323-324 — không filter MONTH)
    SELECT CAST(`MONTH` AS STRING)                     AS `MONTH`
          ,'FORECAST'                                  AS TYPE
          ,UPPER(RTRIM(CAST(BIZLINE        AS STRING))) AS BIZLINE
          ,UPPER(RTRIM(CAST(CUS_TYPE       AS STRING))) AS CUS_TYPE
          ,UPPER(RTRIM(CAST(SEGMENT        AS STRING))) AS SEGMENT
          ,UPPER(RTRIM(CAST(SUB_CHANNEL    AS STRING))) AS SUB_CHANNEL
          ,UPPER(RTRIM(CAST(CHANNEL        AS STRING))) AS CHANNEL
          ,UPPER(RTRIM(CAST(PRODUCT_FAMILY AS STRING))) AS PRODUCT_FAMILY
          ,UPPER(RTRIM(CAST(PRODUCT_LINE   AS STRING))) AS PRODUCT_LINE
          ,UPPER(RTRIM(CAST(SUB_PRODUCT    AS STRING))) AS SUB_PRODUCT
          ,UPPER(RTRIM(CAST(GROUP_MEASURE  AS STRING))) AS GROUP_MEASURE
          ,UPPER(RTRIM(CAST(MEASURE        AS STRING))) AS MEASURE
          ,UPPER(RTRIM(CAST(SUB_MEASURE    AS STRING))) AS SUB_MEASURE
          ,UPPER(RTRIM(CAST(SUB_MEASURE_2  AS STRING))) AS SUB_MEASURE_2
          ,TRY_CAST(`VALUE` AS DOUBLE)                 AS `VALUE`
    FROM {TBL_FORECAST_2026}

    UNION ALL
    -- FORECAST_LM (SP L327-328)
    SELECT CAST(`MONTH` AS STRING)                     AS `MONTH`
          ,'FORECAST_LM'                               AS TYPE
          ,UPPER(RTRIM(CAST(BIZLINE        AS STRING))) AS BIZLINE
          ,UPPER(RTRIM(CAST(CUS_TYPE       AS STRING))) AS CUS_TYPE
          ,UPPER(RTRIM(CAST(SEGMENT        AS STRING))) AS SEGMENT
          ,''                                          AS SUB_CHANNEL
          ,UPPER(RTRIM(CAST(CHANNEL        AS STRING))) AS CHANNEL
          ,UPPER(RTRIM(CAST(PRODUCT_FAMILY AS STRING))) AS PRODUCT_FAMILY
          ,UPPER(RTRIM(CAST(PRODUCT_LINE   AS STRING))) AS PRODUCT_LINE
          ,UPPER(RTRIM(CAST(SUB_PRODUCT    AS STRING))) AS SUB_PRODUCT
          ,UPPER(RTRIM(CAST(GROUP_MEASURE  AS STRING))) AS GROUP_MEASURE
          ,UPPER(RTRIM(CAST(MEASURE        AS STRING))) AS MEASURE
          ,UPPER(RTRIM(CAST(SUB_MEASURE    AS STRING))) AS SUB_MEASURE
          ,UPPER(RTRIM(CAST(SUB_MEASURE_2  AS STRING))) AS SUB_MEASURE_2
          ,TRY_CAST(`VALUE` AS DOUBLE)                 AS `VALUE`
    FROM {TBL_FORECAST_LM}
    WHERE CAST(`MONTH` AS STRING) LIKE '2026%'
      AND {PARTITION_COL} = '{date_lm}'

    UNION ALL
    -- REVISED_BUDGET (SP L331-333)
    SELECT CAST(`MONTH` AS STRING)                     AS `MONTH`
          ,'REVISED_BUDGET'                            AS TYPE
          ,UPPER(RTRIM(CAST(BIZLINE        AS STRING))) AS BIZLINE
          ,UPPER(RTRIM(CAST(CUS_TYPE       AS STRING))) AS CUS_TYPE
          ,UPPER(RTRIM(CAST(SEGMENT        AS STRING))) AS SEGMENT
          ,''                                          AS SUB_CHANNEL
          ,UPPER(RTRIM(CAST(CHANNEL        AS STRING))) AS CHANNEL
          ,UPPER(RTRIM(CAST(PRODUCT_FAMILY AS STRING))) AS PRODUCT_FAMILY
          ,UPPER(RTRIM(CAST(PRODUCT_LINE   AS STRING))) AS PRODUCT_LINE
          ,UPPER(RTRIM(CAST(SUB_PRODUCT    AS STRING))) AS SUB_PRODUCT
          ,UPPER(RTRIM(CAST(GROUP_MEASURE  AS STRING))) AS GROUP_MEASURE
          ,UPPER(RTRIM(CAST(MEASURE        AS STRING))) AS MEASURE
          ,UPPER(RTRIM(CAST(SUB_MEASURE    AS STRING))) AS SUB_MEASURE
          ,UPPER(RTRIM(CAST(SUB_MEASURE_2  AS STRING))) AS SUB_MEASURE_2
          ,TRY_CAST(`VALUE` AS DOUBLE)                 AS `VALUE`
    FROM {TBL_RV_BUDGET}
    WHERE CAST(`MONTH` AS STRING) LIKE '2026%'
      AND {PARTITION_COL} = '{date_rv}'
),
-- SP L200-201: UPDATE ANALYSIS..BUDGET_2026_UNIONALL SET SEGMENT = 'Others' WHERE SEGMENT IS NULL ("update tạm 26/12")
--   → không sửa bảng nguồn, áp cho nhánh BUDGET (idempotent nếu bảng nguồn đã được update)
s01 AS (
  SELECT * EXCEPT (_match, SEGMENT),
         CASE WHEN _match THEN 'OTHERS' ELSE SEGMENT END AS SEGMENT
  FROM (SELECT *, (TYPE = 'BUDGET' AND SEGMENT IS NULL) AS _match
        FROM src)
),
-- SP L337-339: Update 20251125 — SME 'Other NII' → CI
s02 AS (
  SELECT * EXCEPT (_match, SUB_MEASURE, SUB_MEASURE_2),
         CASE WHEN _match THEN 'CI' ELSE SUB_MEASURE   END AS SUB_MEASURE,
         CASE WHEN _match THEN 'CI' ELSE SUB_MEASURE_2 END AS SUB_MEASURE_2
  FROM (SELECT *, (SUB_MEASURE = 'OTHER NII' AND BIZLINE = 'SME') AS _match
        FROM s01)
),
-- SP L341-343: Update 20251125 — Retail CASA&AE NFI → FI
s03 AS (
  SELECT * EXCEPT (_match, SUB_MEASURE),
         CASE WHEN _match THEN 'FI' ELSE SUB_MEASURE END AS SUB_MEASURE
  FROM (SELECT *, (SUB_MEASURE = 'NFI' AND BIZLINE = 'RETAIL' AND PRODUCT_LINE = 'CASA&AE') AS _match
        FROM s02)
)
SELECT BIZLINE, `MONTH`, TYPE, CUS_TYPE, SEGMENT, SUB_CHANNEL, CHANNEL
      ,PRODUCT_FAMILY, PRODUCT_LINE, SUB_PRODUCT, GROUP_MEASURE, MEASURE, SUB_MEASURE, SUB_MEASURE_2, `VALUE`
FROM s03
""")

spark.sql(f"""
    CREATE OR REPLACE TABLE {TBL_CONSOL} USING DELTA
    {DELTA_TBLPROPERTIES}
    AS SELECT * FROM v_forecast_consol
""")

# --- Summary theo TYPE ---
type_cnt = {r.TYPE: r.cnt for r in spark.sql(f"SELECT TYPE, COUNT(*) AS cnt FROM {TBL_CONSOL} GROUP BY TYPE").collect()}
print(f"\n=== CONSOL DONE: {TBL_CONSOL} ===")
for t in ["BUDGET", "FORECAST", "FORECAST_LM", "REVISED_BUDGET"]:
    flag = "" if type_cnt.get(t, 0) > 0 else "   ⚠ 0 rows — kiểm tra snapshot / bảng nguồn!"
    print(f"  {t:<15s}: {type_cnt.get(t, 0):>12,}{flag}")
```

# Block 10: CALC Table + Executor (Method 4)

```python
# ============================================================
# BLOCK 10 — CALC TABLE DDL + EXECUTOR
# SP L289-307: CREATE TABLE ANALYSIS..FORECAST_2026_CALC
#
# Executor chạy theo STAGE (danh sách stage ở Block 16):
#   - "sequential": chạy lần lượt từng block, INSERT thẳng vào CALC (như SP)
#   - "parallel"  : các block KHÔNG đọc output của nhau chạy đồng thời (tối đa MAX_PARALLEL)
#
# Vì sao stage parallel cần bảng CALC_STAGE:
#   Các block đều là INSERT INTO CALC ... SELECT ... FROM CALC (đọc chính bảng đích) → Delta coi là
#   "non-blind append". 2 transaction như vậy chạy song song trên bảng không partition sẽ lỗi
#   ConcurrentAppendException.
#   → Trong stage parallel, executor chỉ đổi đích "INSERT INTO <CALC>" thành "INSERT INTO <CALC_STAGE>";
#     phần SELECT vẫn đọc CALC đúng như SQL gốc. Ghi vào STAGE là blind append → không conflict.
#   → Hết stage: 1 câu INSERT INTO CALC SELECT * FROM CALC_STAGE.
#   → Kết quả giống chạy tuần tự vì block trong cùng stage không đọc output của nhau.
# ============================================================
from concurrent.futures import ThreadPoolExecutor, as_completed


def reset_calc_table():
    spark.sql(f"""
        CREATE OR REPLACE TABLE {TBL_CALC}
        (
             BIZLINE        STRING
            ,YEARMONTH      STRING
            ,TYPE           STRING   -- BUDGET / FORECAST / FORECAST_LM / REVISED_BUDGET / variance types
            ,CUS_TYPE       STRING
            ,SEGMENT        STRING
            ,PRODUCT_LV_4   STRING   -- TOTAL LENDING/CARD, TOTAL DEPOSIT, FEE BASE ...
            ,PRODUCT_LV_3   STRING   -- PRODUCT_FAMILY (LENDING, CARD ...)
            ,PRODUCT_LV_2   STRING   -- PRODUCT_LINE (MORTGAGE ...)
            ,PRODUCT_LV_1   STRING   -- SUB_PRODUCT (MORTGAGE PRIMARY ...)
            ,MEASURE        STRING
            ,LEVEL          STRING
            ,VALUE          DOUBLE
        )
        USING DELTA
        {DELTA_TBLPROPERTIES}
    """)
    # Bảng tạm cho stage parallel — cùng schema với CALC
    spark.sql(f"""
        CREATE OR REPLACE TABLE {TBL_CALC_STAGE} USING DELTA
        {DELTA_TBLPROPERTIES}
        AS SELECT * FROM {TBL_CALC} WHERE 1 = 0
    """)
    print(f"=== CALC table reset: {TBL_CALC} (+ stage {TBL_CALC_STAGE}) ===")


def _run_block(name, sql_text):
    """Chạy 1 block → (rows inserted | None, giây). Lỗi → raise kèm tên block + SQL."""
    t0 = time.time()
    try:
        res = spark.sql(sql_text)
    except Exception as e:
        raise RuntimeError(f"Block {name} FAILED: {e}\n--- SQL ---\n{sql_text}") from e
    return inserted_rows(res), time.time() - t0


def _fmt_rows(n):
    return f"{n:>12,}" if n is not None else f"{'n/a':>12}"


def execute_stages(stages, start_from_stage=None, only_stages=None):
    """Chạy list (stage_name, mode, blocks) theo thứ tự. mode = 'sequential' | 'parallel'.
    - start_from_stage : bỏ qua các stage trước (dùng khi stage đó FAIL ở lần chạy trước)
    - only_stages      : list tên stage chỉ chạy riêng
    Log: thời gian + số rows mỗi block (từ kết quả INSERT, không chạy thêm count), tổng CALC sau mỗi stage.
    """
    stage_names = [s for s, _, _ in stages]
    if start_from_stage and start_from_stage not in stage_names:
        raise ValueError(f"start_from_stage '{start_from_stage}' không tồn tại")
    if only_stages and set(only_stages) - set(stage_names):
        raise ValueError(f"only_stages có tên không tồn tại: {sorted(set(only_stages) - set(stage_names))}")
    block_names = [n for _, _, blocks in stages for n, _ in blocks]
    dups = sorted({n for n in block_names if block_names.count(n) > 1})
    if dups:
        raise ValueError(f"Trùng tên block: {dups}")

    target_calc  = f"INSERT INTO {TBL_CALC}"
    target_stage = f"INSERT INTO {TBL_CALC_STAGE}"
    started, t_all = start_from_stage is None, time.time()

    for s_idx, (stage_name, mode, blocks) in enumerate(stages, 1):
        if not started:
            if stage_name != start_from_stage:
                continue
            started = True
        if only_stages and stage_name not in only_stages:
            continue

        t_stage = time.time()
        print(f"--- Stage {s_idx}/{len(stages)}: {stage_name} [{mode}] — {len(blocks)} block(s)")

        if mode == "sequential":
            for name, sql_text in blocks:
                rows, sec = _run_block(name, sql_text)
                print(f"    {name:50s} ({sec:6.1f}s) +{_fmt_rows(rows)} rows")

        elif mode == "parallel":
            spark.sql(f"TRUNCATE TABLE {TBL_CALC_STAGE}")
            staged = []
            for name, sql_text in blocks:
                if sql_text.count(target_calc) != 1:
                    raise ValueError(f"{name}: cần đúng 1 câu '{target_calc}' để chạy trong stage parallel")
                staged.append((name, sql_text.replace(target_calc, target_stage)))

            errors = []
            with ThreadPoolExecutor(max_workers=max(1, min(MAX_PARALLEL, len(staged)))) as pool:
                futures = {pool.submit(_run_block, name, sql_text): name for name, sql_text in staged}
                for fut in as_completed(futures):
                    name = futures[fut]
                    try:
                        rows, sec = fut.result()
                        print(f"    {name:50s} ({sec:6.1f}s) +{_fmt_rows(rows)} rows")
                    except Exception as e:
                        errors.append(e)
                        print(f"    {name:50s} FAILED")
            if errors:
                # Stage lỗi → KHÔNG merge vào CALC; CALC giữ nguyên trạng thái trước stage
                raise errors[0]

            res = spark.sql(f"INSERT INTO {TBL_CALC} SELECT * FROM {TBL_CALC_STAGE}")
            print(f"    {'→ merge CALC_STAGE into CALC':50s}          +{_fmt_rows(inserted_rows(res))} rows")

        else:
            raise ValueError(f"Stage {stage_name}: mode '{mode}' không hợp lệ")

        total = spark.table(TBL_CALC).count()   # count(*) không filter → Delta đọc từ metadata
        print(f"    stage done ({time.time() - t_stage:,.1f}s) — CALC = {total:,} rows")

    print(f"=== DONE in {time.time() - t_all:,.1f}s ===")


def show_block(blocks, name):
    """In SQL của 1 block để debug / đối chiếu SP."""
    print(dict(blocks)[name])


print("CALC helpers loaded: reset_calc_table, execute_stages, show_block")
```

# Block 11: CALC Nhóm A — Leaf Insert (CONSOL → CALC)

```python
# ============================================================
# BLOCK 11 — CALC NHÓM A: LEAF INSERT từ CONSOL (SP L346-610)
# Literal viết HOA vì CONSOL đã chuẩn hóa UPPER(RTRIM()).
# ============================================================

def _calc_blocks_a_leaf():
    return [
        (
            "A01_L1_1.1_sub_measure_nii_nfi",
            f"""
-- SP L346-372: L1 - 1.1. DETAILS SUB_MEASURE OF NII, NFI: CI,FTP,INT_REV,CI_ADJ, FI,FE
INSERT INTO {TBL_CALC}
SELECT   BIZLINE, `MONTH`
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,CASE WHEN PRODUCT_FAMILY IN ('CARD','LENDING') THEN 'TOTAL LENDING/CARD'
              WHEN PRODUCT_FAMILY IN ('TIME DEPOSIT','CASA','DEPOSIT','CASA&AE') THEN 'TOTAL DEPOSIT'
              ELSE PRODUCT_FAMILY END
        ,PRODUCT_FAMILY
        ,PRODUCT_LINE
        ,SUB_PRODUCT
        ,CASE WHEN GROUP_MEASURE IN ('NFI') AND SUB_MEASURE NOT IN ('FI','FE') THEN 'FI' ELSE SUB_MEASURE END
        ,'L1' LEVEL
        ,SUM(VALUE) VALUE
FROM {TBL_CONSOL}
WHERE GROUP_MEASURE IN ('NII','NFI') AND SUB_MEASURE <> GROUP_MEASURE
GROUP BY BIZLINE, `MONTH`
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,CASE WHEN PRODUCT_FAMILY IN ('CARD','LENDING') THEN 'TOTAL LENDING/CARD'
              WHEN PRODUCT_FAMILY IN ('TIME DEPOSIT','CASA','DEPOSIT','CASA&AE') THEN 'TOTAL DEPOSIT'
              ELSE PRODUCT_FAMILY END
        ,PRODUCT_FAMILY
        ,PRODUCT_LINE
        ,SUB_PRODUCT
        ,CASE WHEN GROUP_MEASURE IN ('NFI') AND SUB_MEASURE NOT IN ('FI','FE') THEN 'FI' ELSE SUB_MEASURE END
""",
        ),
        (
            "A02_L1_1.1.2_sub_measure_2_cd",
            f"""
-- SP L375-401: L1 - 1.1.2. DETAILS SUB_MEASURE_2 OF CD (update 20251212)
INSERT INTO {TBL_CALC}
SELECT   BIZLINE, `MONTH`
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,CASE WHEN PRODUCT_FAMILY IN ('CARD','LENDING') THEN 'TOTAL LENDING/CARD'
              WHEN PRODUCT_FAMILY IN ('TIME DEPOSIT','CASA','DEPOSIT') THEN 'TOTAL DEPOSIT'
              ELSE PRODUCT_FAMILY END
        ,PRODUCT_FAMILY
        ,PRODUCT_LINE
        ,SUB_PRODUCT
        ,CASE WHEN SUB_MEASURE_2 IN ('CI') THEN 'CI_NORMAL' ELSE SUB_MEASURE_2 END
        ,'L1' LEVEL
        ,SUM(VALUE) VALUE
FROM {TBL_CONSOL}
WHERE GROUP_MEASURE IN ('NII') AND PRODUCT_LINE IN ('CD','CD SELF TRADE','CDST','CD AE') AND SUB_MEASURE_2 IN ('CI','LIQUIDITY COST')
GROUP BY BIZLINE, `MONTH`
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,CASE WHEN PRODUCT_FAMILY IN ('CARD','LENDING') THEN 'TOTAL LENDING/CARD'
              WHEN PRODUCT_FAMILY IN ('TIME DEPOSIT','CASA','DEPOSIT') THEN 'TOTAL DEPOSIT'
              ELSE PRODUCT_FAMILY END
        ,PRODUCT_FAMILY
        ,PRODUCT_LINE
        ,SUB_PRODUCT
        ,CASE WHEN SUB_MEASURE_2 IN ('CI') THEN 'CI_NORMAL' ELSE SUB_MEASURE_2 END
""",
        ),
        (
            "A03_L1_1.2_measure_detail",
            f"""
-- SP L403-455: L1 - 1.2. DETAILS MEASURE: NII, AVG BAL, EOP BAL, NEW BAL, PAIDOFF, NEW CARD, IB BAL, OUT BAL, APE, NEW ACCT ...
INSERT INTO {TBL_CALC}
SELECT   BIZLINE, `MONTH`
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,CASE WHEN PRODUCT_FAMILY IN ('CARD','LENDING') THEN 'TOTAL LENDING/CARD'
              WHEN PRODUCT_FAMILY IN ('TIME DEPOSIT','CASA','DEPOSIT','CASA&AE') THEN 'TOTAL DEPOSIT'
              ELSE PRODUCT_FAMILY END
        ,PRODUCT_FAMILY
        ,PRODUCT_LINE
        ,SUB_PRODUCT
        ,CASE WHEN GROUP_MEASURE IN ('PAIDOFF','NII','NFI') THEN GROUP_MEASURE
              WHEN SUB_MEASURE IN ('PURCHASE AMT','PURCHASE NO') THEN SUB_MEASURE
              ELSE MEASURE END
        ,'L1' LEVEL
        ,SUM(VALUE) VALUE
FROM {TBL_CONSOL}
WHERE (MEASURE IN (
                  'NII','NFI'
                 ,'AVG BAL','EOP BAL','NEW BAL'
                 ,'IB BAL','OUT BAL'
                 ,'PAIDOFF'
                 ,'NEW CARD'
                 ,'APE','FYP','GWP','NEW ACCT'
                 ,'LIQUIDITY RECHARGED','TRADING INCOME','OTHER INCOME'
                 ,'CUS HAVING NEW CONTRACT','REGISTER CUS'
                 ,'UNIQUE CUS HAVING NEW CONTRACT'
                 ,'NEW CUS'
                 ,'EOP CUS BY PRODUCT'
                 ,'FYP'
                 ,'EOP CARD','ACTIVE CARD'
                 ,'OPEX','PROVISION'
                 ,'LIMIT APPROVED','LIMIT USED','CUS HAVING NEW BAL','EOP CUS HAVING LIMIT','LIMIT'
                 ,'MARGIN POINT','TICKET SIZE FX'
                 )
        OR GROUP_MEASURE IN ('PAIDOFF','NII','NFI')
        OR (    MEASURE IN ('TNX NO','TNX AMT','TXN AMT','TXN NO','TTN')
            AND (PRODUCT_FAMILY IN ('CASA','FEE BASE','CASA&AE')
                 OR SUB_MEASURE IN ('PURCHASE AMT','PURCHASE NO')) )
      ) AND PRODUCT_LINE <> 'OTHER ASSET'   -- update 20260226
GROUP BY BIZLINE, `MONTH`
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,CASE WHEN PRODUCT_FAMILY IN ('CARD','LENDING') THEN 'TOTAL LENDING/CARD'
              WHEN PRODUCT_FAMILY IN ('TIME DEPOSIT','CASA','DEPOSIT','CASA&AE') THEN 'TOTAL DEPOSIT'
              ELSE PRODUCT_FAMILY END
        ,PRODUCT_FAMILY
        ,PRODUCT_LINE
        ,SUB_PRODUCT
        ,CASE WHEN GROUP_MEASURE IN ('PAIDOFF','NII','NFI') THEN GROUP_MEASURE
              WHEN SUB_MEASURE IN ('PURCHASE AMT','PURCHASE NO') THEN SUB_MEASURE
              ELSE MEASURE END
""",
        ),
        (
            "A04_1.3_eop_cus_holding_active",
            f"""
-- SP L458-507: 1.3. 'EOP CUS','EOP CUS HOLDING','EOP CUS ACTIVE'
-- LEVEL lấy từ mapping X (SP để cột LEVEL không qualify → thuộc bảng X). Không match X → LEVEL NULL.
-- X có nhiều dòng cùng key → row bị nhân bản (giống SP).
-- Phía X chuẩn hóa UPPER(RTRIM()) + bỏ non-breaking space char(160) vì CALC/CONSOL đã viết HOA.
INSERT INTO {TBL_CALC}
SELECT A.BIZLINE, A.`MONTH`, A.TYPE, A.CUS_TYPE, A.SEGMENT
      ,A.PRODUCT_LV_4, A.PRODUCT_LV_3, A.PRODUCT_LV_2, A.PRODUCT_LV_1, A.MEASURE
      ,UPPER(RTRIM(CAST(B.LEVEL AS STRING))) AS LEVEL
      ,A.VALUE
FROM
(
    SELECT   BIZLINE, `MONTH`
            ,TYPE
            ,CUS_TYPE, SEGMENT
            ,CASE WHEN PRODUCT_FAMILY IN ('CARD','LENDING') THEN 'TOTAL LENDING/CARD'
                  WHEN PRODUCT_FAMILY IN ('TIME DEPOSIT','CASA','CASA&AE') THEN 'TOTAL DEPOSIT'
                  ELSE PRODUCT_FAMILY END AS PRODUCT_LV_4
            ,PRODUCT_FAMILY AS PRODUCT_LV_3
            ,CASE WHEN PRODUCT_FAMILY = 'CASA' THEN 'ALL'
                  ELSE PRODUCT_LINE END AS PRODUCT_LV_2
            ,CASE WHEN PRODUCT_FAMILY IN ('CASA','CASA&AE','TIME DEPOSIT') THEN 'ALL'
                  WHEN PRODUCT_FAMILY IN ('CARD','LENDING') AND BIZLINE = 'SME' THEN 'ALL'
                  WHEN PRODUCT_FAMILY IN ('CARD') AND BIZLINE = 'RETAIL' THEN 'ALL'
                  WHEN PRODUCT_LINE IN ('BOND-FUND','STRUCTURED NOTES') AND BIZLINE = 'RETAIL' THEN 'ALL'
                  ELSE SUB_PRODUCT END AS PRODUCT_LV_1
            ,MEASURE
            ,SUM(VALUE) VALUE
    FROM {TBL_CONSOL}
    WHERE MEASURE IN ('EOP CUS','EOP CUS HOLDING','EOP CUS ACTIVE','ACTIVE CUS') OR MEASURE IN ('TRULY ACTIVE CUS','LOW ACTIVE CUS','MTB CUS')
    GROUP BY BIZLINE, `MONTH`
            ,TYPE
            ,CUS_TYPE, SEGMENT
            ,CASE WHEN PRODUCT_FAMILY IN ('CARD','LENDING') THEN 'TOTAL LENDING/CARD'
                  WHEN PRODUCT_FAMILY IN ('TIME DEPOSIT','CASA','CASA&AE') THEN 'TOTAL DEPOSIT'
                  ELSE PRODUCT_FAMILY END
            ,PRODUCT_FAMILY
            ,CASE WHEN PRODUCT_FAMILY = 'CASA' THEN 'ALL'
                  ELSE PRODUCT_LINE END
            ,CASE WHEN PRODUCT_FAMILY IN ('CASA','CASA&AE','TIME DEPOSIT') THEN 'ALL'
                  WHEN PRODUCT_FAMILY IN ('CARD','LENDING') AND BIZLINE = 'SME' THEN 'ALL'
                  WHEN PRODUCT_FAMILY IN ('CARD') AND BIZLINE = 'RETAIL' THEN 'ALL'
                  WHEN PRODUCT_LINE IN ('BOND-FUND','STRUCTURED NOTES') AND BIZLINE = 'RETAIL' THEN 'ALL'
                  ELSE SUB_PRODUCT END
            ,MEASURE
) A
LEFT JOIN {TBL_MAPPING_X} B
       ON  A.PRODUCT_LV_4 = UPPER(RTRIM(REPLACE(CAST(B.PRODUCT_LV_4 AS STRING), char(160), ' ')))
       AND A.PRODUCT_LV_3 = UPPER(RTRIM(REPLACE(CAST(B.PRODUCT_LV_3 AS STRING), char(160), ' ')))
       AND A.PRODUCT_LV_2 = UPPER(RTRIM(REPLACE(CAST(B.PRODUCT_LV_2 AS STRING), char(160), ' ')))
       AND A.PRODUCT_LV_1 = UPPER(RTRIM(REPLACE(CAST(B.PRODUCT_LV_1 AS STRING), char(160), ' ')))
       AND A.MEASURE      = UPPER(RTRIM(REPLACE(CAST(B.MEASURE      AS STRING), char(160), ' ')))
       AND A.BIZLINE      = UPPER(RTRIM(REPLACE(CAST(B.BIZLINE      AS STRING), char(160), ' ')))
""",
        ),
        (
            "A05_L0_new_cus_by_channel",
            f"""
-- SP L509-531: L0. 'NEW CUS' BY CUS_GROUP
INSERT INTO {TBL_CALC}
SELECT   BIZLINE, `MONTH`
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,PRODUCT_FAMILY
        ,PRODUCT_FAMILY
        ,PRODUCT_LINE
        ,SUB_PRODUCT
        ,CHANNEL AS MEASURE
        ,'L0' LEVEL
        ,SUM(VALUE) VALUE
FROM {TBL_CONSOL}
WHERE MEASURE IN ('NEW CUS') AND PRODUCT_FAMILY = 'CUS'
GROUP BY BIZLINE, `MONTH`
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,PRODUCT_FAMILY
        ,PRODUCT_FAMILY
        ,PRODUCT_LINE
        ,SUB_PRODUCT
        ,CHANNEL
""",
        ),
        (
            "A06_L0.1_new_cus_by_sub_channel",
            f"""
-- SP L533-555: L0.1 'NEW CUS' BY SUB-CHANNEL
INSERT INTO {TBL_CALC}
SELECT   BIZLINE, `MONTH`
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,PRODUCT_FAMILY
        ,PRODUCT_FAMILY
        ,PRODUCT_LINE
        ,SUB_PRODUCT
        ,SUB_CHANNEL AS MEASURE
        ,'L0' LEVEL
        ,SUM(VALUE) VALUE
FROM {TBL_CONSOL}
WHERE BIZLINE = 'RETAIL' AND MEASURE IN ('NEW CUS') AND PRODUCT_FAMILY = 'CUS'
GROUP BY BIZLINE, `MONTH`
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,PRODUCT_FAMILY
        ,PRODUCT_FAMILY
        ,PRODUCT_LINE
        ,SUB_PRODUCT
        ,SUB_CHANNEL
""",
        ),
        (
            "A07_L0.2_new_cus_casa_ae",
            f"""
-- SP L557-576: L0.2 'NEW CUS' CASA&AE
INSERT INTO {TBL_CALC}
SELECT   BIZLINE, `MONTH`
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,'TOTAL DEPOSIT' PRODUCT_LV_4
        ,PRODUCT_FAMILY
        ,PRODUCT_LINE
        ,'ALL'
        ,'NEW CUS CASA&AE' MEASURE
        ,'L2' LEVEL
        ,SUM(VALUE) VALUE
FROM {TBL_CONSOL}
WHERE BIZLINE = 'RETAIL' AND MEASURE IN ('EOP CUS') AND CUS_TYPE = 'NEW' AND SUB_PRODUCT = 'CASA&AE'
GROUP BY BIZLINE, `MONTH`
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,PRODUCT_FAMILY
        ,PRODUCT_FAMILY
        ,PRODUCT_LINE
""",
        ),
        (
            "A08_L1.5_fi_fe_detail",
            f"""
-- SP L579-610: 20251125 Update FI, FE (không GROUP BY — insert từng row)
INSERT INTO {TBL_CALC}
SELECT BIZLINE, `MONTH`, TYPE
      ,' ALL' CUS_TYPE, ' ALL' SEGMENT
      ,CASE WHEN PRODUCT_LINE IN ('CASA','CASA&AE') OR SUB_PRODUCT IN ('HOME BANKING','REMITTANCE','ECHANNEL','MQR','SOUNDBOX','POS') THEN 'CASA GROUP'
            WHEN PRODUCT_FAMILY = 'TIME DEPOSIT' THEN 'TOTAL DEPOSIT'
            WHEN PRODUCT_FAMILY IN ('LENDING') OR PRODUCT_LINE = 'CREDIT CARD' THEN 'TOTAL LENDING/CARD'
            WHEN PRODUCT_LINE = 'DEBIT CARD' THEN 'CASA GROUP'
            ELSE PRODUCT_FAMILY END AS PRODUCT_LV_4
      ,CASE WHEN SUB_PRODUCT IN ('POS','MQR','SOUNDBOX') AND BIZLINE = 'RETAIL' THEN 'CASA&AE'
            WHEN PRODUCT_LINE = 'DEBIT CARD' THEN 'CARD'
            ELSE PRODUCT_LINE END AS PRODUCT_LV_3
      ,CASE WHEN PRODUCT_LINE = 'TD NORMAL' THEN 'ALL'
            WHEN SUB_PRODUCT IN ('HOME BANKING','REMITTANCE','ECHANNEL','POS','MQR','SOUNDBOX') THEN SUB_PRODUCT
            ELSE PRODUCT_LINE END AS PRODUCT_LV_2
      ,'ALL' PRODUCT_LV_1
      ,SUB_MEASURE MEASURE
      ,'L1.5' LEVEL
      ,VALUE
FROM {TBL_CONSOL}
WHERE 1=1 AND
     (BIZLINE = 'RETAIL'
      AND SUB_MEASURE IN ('FI','FE')
      AND PRODUCT_LINE IN ('CREDIT CARD','DEBIT CARD','CASA','BOND-FUND','STRUCTURED NOTES','HOME BANKING','REMITTANCE','ECHANNEL','ATM','OTHER FEE','CASA&AE','TD NORMAL')
     ) OR
     (BIZLINE = 'SME'
      AND SUB_MEASURE IN ('FI','FE')
      AND PRODUCT_LINE IN ('CREDIT CARD','DEBIT CARD','CASA','CA')
     )  -- Update 20260226
""",
        ),
    ]

print(f"Nhóm A: {len(_calc_blocks_a_leaf())} blocks")
```

# Block 12: CALC Nhóm B — Hierarchical Rollup

```python
# ============================================================
# BLOCK 12 — CALC NHÓM B: SUM-UP L2 → L5, CUS_TYPE/SEGMENT/BIZLINE ALL (SP L614-1012)
# Self-read từ CALC: thứ tự block BẮT BUỘC giữ nguyên.
# ============================================================

def _calc_blocks_b_rollup():
    return [
        (
            "B01_L2_sumup",
            f"""
-- SP L618-642: L2 SUM-UP CALCULATION (update 20260226)
INSERT INTO {TBL_CALC}
SELECT  BIZLINE, YEARMONTH
       ,TYPE
       ,CUS_TYPE, SEGMENT
       ,CASE WHEN PRODUCT_LV_4 = 'CASA&AE' AND MEASURE IN ('CI','FTP') THEN 'TOTAL DEPOSIT'
             ELSE PRODUCT_LV_4 END AS PRODUCT_LV_4
       ,PRODUCT_LV_3
       ,PRODUCT_LV_2
       ,'ALL'
       ,MEASURE
       ,'L2' LEVEL
       ,SUM(VALUE) VALUE
FROM {TBL_CALC}
WHERE LEVEL = 'L1'
  AND ( MEASURE NOT IN ('EOP CUS','EOP CUS HOLDING','EOP CUS ACTIVE') )
GROUP BY BIZLINE, YEARMONTH
       ,TYPE
       ,CUS_TYPE, SEGMENT
       ,CASE WHEN PRODUCT_LV_4 = 'CASA&AE' AND MEASURE IN ('CI','FTP') THEN 'TOTAL DEPOSIT'
             ELSE PRODUCT_LV_4 END
       ,PRODUCT_LV_3
       ,PRODUCT_LV_2
       ,MEASURE
""",
        ),
        (
            "B02_L2.5_casa_group_fi_fe",
            f"""
-- SP L645-667: L2.5 CASA GROUP (NFI) SUM-UP (update 20260226)
INSERT INTO {TBL_CALC}
SELECT  BIZLINE, YEARMONTH
       ,TYPE
       ,CUS_TYPE, SEGMENT
       ,PRODUCT_LV_4
       ,PRODUCT_LV_3
       ,PRODUCT_LV_2
       ,'ALL' PRODUCT_LV_1
       ,MEASURE
       ,'L2.5' LEVEL
       ,SUM(VALUE) VALUE
FROM {TBL_CALC}
WHERE LEVEL = 'L1.5'
  AND MEASURE IN ('FI','FE')
  AND PRODUCT_LV_2 IN ('CASA&AE','HOME BANKING','ECHANNEL','REMITTANCE','CASA','DEBIT CARD','POS','MQR','SOUNDBOX')
GROUP BY BIZLINE, YEARMONTH
       ,TYPE
       ,CUS_TYPE, SEGMENT
       ,PRODUCT_LV_4
       ,PRODUCT_LV_3
       ,PRODUCT_LV_2
       ,MEASURE
""",
        ),
        (
            "B03_L3.5_casa_group_fi_fe",
            f"""
-- SP L669-687
INSERT INTO {TBL_CALC}
SELECT  BIZLINE, YEARMONTH
       ,TYPE
       ,CUS_TYPE, SEGMENT
       ,'CASA GROUP'
       ,'CASA GROUP'
       ,'ALL'
       ,'ALL'
       ,MEASURE
       ,'L3.5' LEVEL
       ,SUM(VALUE) VALUE
FROM {TBL_CALC}
WHERE LEVEL = 'L2.5'
  AND MEASURE IN ('FI','FE')
  AND PRODUCT_LV_2 IN ('CASA&AE','CASA','HOME BANKING','ECHANNEL','REMITTANCE','DEBIT CARD','POS','MQR','SOUNDBOX','CA')
GROUP BY BIZLINE, YEARMONTH
       ,TYPE
       ,CUS_TYPE, SEGMENT
       ,MEASURE
""",
        ),
        (
            "B04_L2.5_casa_group_nfi",
            f"""
-- SP L689-712 (GROUP BY có MEASURE FI/FE → 2 row 'NFI' riêng, giống SP)
INSERT INTO {TBL_CALC}
SELECT  BIZLINE, YEARMONTH
       ,TYPE
       ,CUS_TYPE, SEGMENT
       ,PRODUCT_LV_4
       ,CASE WHEN PRODUCT_LV_3 = 'CASA' AND BIZLINE = 'RETAIL' THEN 'CASA&AE'
             ELSE PRODUCT_LV_3 END AS PRODUCT_LV_3
       ,PRODUCT_LV_2
       ,'ALL'
       ,'NFI' MEASURE
       ,'L2.5' LEVEL
       ,SUM(VALUE) VALUE
FROM {TBL_CALC}
WHERE LEVEL = 'L1.5'
  AND MEASURE IN ('FI','FE')
  AND PRODUCT_LV_2 IN ('CASA&AE','HOME BANKING','ECHANNEL','REMITTANCE','CASA','DEBIT CARD','POS','MQR','SOUNDBOX')
GROUP BY BIZLINE, YEARMONTH
       ,TYPE
       ,CUS_TYPE, SEGMENT
       ,PRODUCT_LV_4
       ,CASE WHEN PRODUCT_LV_3 = 'CASA' AND BIZLINE = 'RETAIL' THEN 'CASA&AE'
             ELSE PRODUCT_LV_3 END
       ,PRODUCT_LV_2
       ,MEASURE
""",
        ),
        (
            "B05_L3.5_casa_group_nfi",
            f"""
-- SP L714-731 (Update 20260226)
INSERT INTO {TBL_CALC}
SELECT  BIZLINE, YEARMONTH
       ,TYPE
       ,CUS_TYPE, SEGMENT
       ,'CASA GROUP'
       ,'CASA GROUP'
       ,'ALL'
       ,'ALL'
       ,'NFI' MEASURE
       ,'L3.5' LEVEL
       ,SUM(VALUE) VALUE
FROM {TBL_CALC}
WHERE LEVEL = 'L1.5'
  AND MEASURE IN ('FI','FE')
  AND PRODUCT_LV_2 IN ('CASA&AE','CASA','HOME BANKING','ECHANNEL','REMITTANCE','DEBIT CARD','POS','MQR','SOUNDBOX','CA')
GROUP BY BIZLINE, YEARMONTH
       ,TYPE
       ,CUS_TYPE, SEGMENT
""",
        ),
        (
            "B06_L3.5_sme_trade_service_fx_flow",
            f"""
-- SP L734-759: 20251125 Update TRADE SERVICE & FX FLOW
INSERT INTO {TBL_CALC}
SELECT BIZLINE, YEARMONTH
       ,TYPE
       ,' ALL' CUS_TYPE, ' ALL' SEGMENT
       ,'FEE BASE'
       ,'FEE BASE'
       ,CASE WHEN PRODUCT_LV_1 IN ('LC FEE','LG FEE') AND MEASURE = 'NFI' THEN 'TRADE SERVICE FEE'
             WHEN PRODUCT_LV_1 IN ('LC INTEREST','LG INTEREST') AND MEASURE = 'NII' THEN 'TRADE SERVICE INTEREST'
             WHEN PRODUCT_LV_1 IN ('FX FLOW','FX DERIVATIVES') THEN 'FX FLOW'
             END
       ,'ALL'
       ,MEASURE
       ,'L3.5' LEVEL
       ,SUM(VALUE) VALUE
FROM {TBL_CALC}
WHERE LEVEL = 'L1' AND BIZLINE = 'SME'
  AND MEASURE IN ('NFI','NII','TRADING INCOME')
  AND ( PRODUCT_LV_2 IN ('LC','LG') OR PRODUCT_LV_1 IN ('FX FLOW','FX DERIVATIVES') )
GROUP BY BIZLINE, YEARMONTH
       ,TYPE
       ,CASE WHEN PRODUCT_LV_1 IN ('LC FEE','LG FEE') AND MEASURE = 'NFI' THEN 'TRADE SERVICE FEE'
             WHEN PRODUCT_LV_1 IN ('LC INTEREST','LG INTEREST') AND MEASURE = 'NII' THEN 'TRADE SERVICE INTEREST'
             WHEN PRODUCT_LV_1 IN ('FX FLOW','FX DERIVATIVES') THEN 'FX FLOW'
             END
       ,MEASURE
""",
        ),
        (
            "B07_L3_sumup",
            f"""
-- SP L761-783: L3 SUM-UP CALCULATION
INSERT INTO {TBL_CALC}
SELECT  BIZLINE, YEARMONTH
       ,TYPE
       ,CUS_TYPE, SEGMENT
       ,PRODUCT_LV_4
       ,PRODUCT_LV_3
       ,CASE WHEN PRODUCT_LV_2 IN ('BOND-FUND','STRUCTURED NOTES') THEN 'INVESTMENT PRODUCT' ELSE 'ALL' END AS PRODUCT_LV_2
       ,'ALL'
       ,MEASURE
       ,'L3' LEVEL
       ,SUM(VALUE) VALUE
FROM {TBL_CALC}
WHERE LEVEL = 'L2'
  AND MEASURE NOT IN ('EOP CUS','EOP CUS HOLDING','EOP CUS ACTIVE')
GROUP BY BIZLINE, YEARMONTH
       ,TYPE
       ,CUS_TYPE, SEGMENT
       ,PRODUCT_LV_4
       ,PRODUCT_LV_3
       ,CASE WHEN PRODUCT_LV_2 IN ('BOND-FUND','STRUCTURED NOTES') THEN 'INVESTMENT PRODUCT' ELSE 'ALL' END
       ,MEASURE
""",
        ),
        (
            "B08_L4_sumup",
            f"""
-- SP L786-806: L4 SUM-UP CALCULATION
INSERT INTO {TBL_CALC}
SELECT   BIZLINE, YEARMONTH
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,PRODUCT_LV_4
        ,'ALL'
        ,'ALL'
        ,'ALL'
        ,MEASURE
        ,'L4' LEVEL
        ,SUM(VALUE) VALUE
FROM {TBL_CALC}
WHERE LEVEL = 'L3'
  AND ( MEASURE NOT IN ('EOP CUS','EOP CUS HOLDING','EOP CUS ACTIVE') )
GROUP BY BIZLINE, YEARMONTH
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,PRODUCT_LV_4
        ,MEASURE
""",
        ),
        (
            "B09_L5.1_income_all_product",
            f"""
-- SP L811-829: 5.1 'NII','NFI','TRADING INCOME','OTHER INCOME','PROVISION','OPEX','FI','FE' ALL PRODUCT
INSERT INTO {TBL_CALC}
SELECT   BIZLINE, YEARMONTH
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,'ALL'
        ,'ALL'
        ,'ALL'
        ,'ALL'
        ,MEASURE
        ,'L5' LEVEL
        ,SUM(VALUE) VALUE
FROM {TBL_CALC}
WHERE LEVEL = 'L4'
  AND MEASURE IN ('NII','NFI','TRADING INCOME','OTHER INCOME','PROVISION','OPEX','FI','FE')
GROUP BY BIZLINE, YEARMONTH
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,MEASURE
""",
        ),
        (
            "B10_L4.5_fi_fe_all_product",
            f"""
-- SP L832-855: 5.2 TOI ALL PRODUCT — phần FI/FE (L4.5)
INSERT INTO {TBL_CALC}
SELECT  BIZLINE, YEARMONTH
       ,TYPE
       ,' ALL', ' ALL'
       ,'ALL'
       ,'ALL'
       ,'ALL'
       ,'ALL'
       ,MEASURE
       ,'L4.5' LEVEL
       ,SUM(VALUE) VALUE
FROM {TBL_CALC}
WHERE 1=1
  AND ( MEASURE IN ('FI','FE') AND ((PRODUCT_LV_3 = 'CASA GROUP' AND LEVEL = 'L3.5')
                                 OR (PRODUCT_LV_2 IN ('BOND-FUND','STRUCTURED NOTES') AND PRODUCT_LV_1 = 'ALL' AND LEVEL = 'L2')
                                 OR (PRODUCT_LV_2 IN ('ATM','MERCHANT PAYMENT COLLECTION','OTHER FEE') AND LEVEL = 'L1.5')
                                 OR (PRODUCT_LV_3 = 'TIME DEPOSIT' AND LEVEL = 'L1.5')
                                 OR (PRODUCT_LV_2 = 'CREDIT CARD' AND PRODUCT_LV_1 = 'ALL' AND LEVEL = 'L1.5')
                                 ))
  OR (MEASURE = 'FI' AND PRODUCT_LV_2 = 'BANCA' AND PRODUCT_LV_1 IN ('BANCA LIFE','BANCA NON_LIFE') AND LEVEL = 'L1')
GROUP BY BIZLINE, YEARMONTH
       ,TYPE
       ,MEASURE
""",
        ),
        (
            "B11_L4.5_nfi_all_product",
            f"""
-- SP L857-878: L4.5 NFI
INSERT INTO {TBL_CALC}
SELECT  BIZLINE, YEARMONTH
       ,TYPE
       ,' ALL', ' ALL'
       ,'ALL'
       ,'ALL'
       ,'ALL'
       ,'ALL'
       ,'NFI'
       ,'L4.5' LEVEL
       ,SUM(VALUE) VALUE
FROM {TBL_CALC}
WHERE 1=1
  AND ( MEASURE IN ('FI','FE') AND ((PRODUCT_LV_3 = 'CASA GROUP' AND LEVEL = 'L3.5')
                                 OR (PRODUCT_LV_2 IN ('BOND-FUND','STRUCTURED NOTES') AND LEVEL = 'L2')
                                 OR (PRODUCT_LV_2 IN ('CREDIT CARD','ATM','MERCHANT PAYMENT COLLECTION','OTHER FEE') AND LEVEL = 'L1.5')
                                 OR (PRODUCT_LV_3 = 'TIME DEPOSIT' AND LEVEL = 'L1.5')
                                 )
        OR MEASURE = 'NFI' AND PRODUCT_LV_2 IN ('BANCA','DPM','GOLD') AND LEVEL = 'L1'
      )
GROUP BY BIZLINE, YEARMONTH
       ,TYPE
""",
        ),
        (
            "B12_L5_toi",
            f"""
-- SP L880-896: L5 TOI
INSERT INTO {TBL_CALC}
SELECT  BIZLINE, YEARMONTH
       ,TYPE
       ,CUS_TYPE, SEGMENT
       ,'ALL'
       ,'ALL'
       ,'ALL'
       ,'ALL'
       ,'TOI'
       ,'L5' LEVEL
       ,SUM(VALUE) VALUE
FROM {TBL_CALC}
WHERE LEVEL = 'L4'
  AND MEASURE IN ('NII','NFI','TRADING INCOME','OTHER INCOME')
GROUP BY BIZLINE, YEARMONTH
       ,TYPE
       ,CUS_TYPE, SEGMENT
""",
        ),
        (
            "B13_L5.4_aum",
            f"""
-- SP L899-935: 5.4 'AUM' ALL PRODUCT (Update 20250925) — CALC L1 EOP BAL + Other Asset từ CONSOL
INSERT INTO {TBL_CALC}
SELECT BIZLINE, YEARMONTH, TYPE, CUS_TYPE, SEGMENT, PRODUCT_LV_4, PRODUCT_LV_3, PRODUCT_LV_2, PRODUCT_LV_1, MEASURE, LEVEL, SUM(VALUE) VALUE
FROM (
    SELECT  BIZLINE, YEARMONTH
           ,TYPE
           ,CUS_TYPE, SEGMENT
           ,'ALL' PRODUCT_LV_4
           ,'ALL' PRODUCT_LV_3
           ,'ALL' PRODUCT_LV_2
           ,'ALL' PRODUCT_LV_1
           ,'AUM' MEASURE
           ,'L5' LEVEL
           ,SUM(VALUE) VALUE
    FROM {TBL_CALC}
    WHERE LEVEL = 'L1'
      AND MEASURE IN ('EOP BAL') AND (PRODUCT_LV_3 IN ('CASA','TIME DEPOSIT','CASA&AE','DPM') OR PRODUCT_LV_2 IN ('BOND-FUND','STRUCTURED NOTES'))
    GROUP BY BIZLINE, YEARMONTH
           ,TYPE
           ,CUS_TYPE, SEGMENT

    UNION ALL

    SELECT  BIZLINE, `MONTH`, TYPE
           ,CUS_TYPE, SEGMENT
           ,'ALL' PRODUCT_LV_4
           ,'ALL' PRODUCT_LV_3
           ,'ALL' PRODUCT_LV_2
           ,'ALL' PRODUCT_LV_1
           ,'AUM' MEASURE
           ,'L5' LEVEL
           ,SUM(VALUE) VALUE
    FROM {TBL_CONSOL}
    WHERE PRODUCT_LINE = 'OTHER ASSET' AND MEASURE = 'EOP BAL'
    GROUP BY BIZLINE, `MONTH`, TYPE, CUS_TYPE, SEGMENT
) A
GROUP BY BIZLINE, YEARMONTH, TYPE, CUS_TYPE, SEGMENT, PRODUCT_LV_4, PRODUCT_LV_3, PRODUCT_LV_2, PRODUCT_LV_1, MEASURE, LEVEL
""",
        ),
        (
            "B14_cus_type_all",
            f"""
-- SP L938-961: CUS_TYPE ALL
INSERT INTO {TBL_CALC}
SELECT   BIZLINE, YEARMONTH
        ,TYPE
        ,' ALL'
        ,SEGMENT
        ,PRODUCT_LV_4
        ,PRODUCT_LV_3
        ,PRODUCT_LV_2
        ,PRODUCT_LV_1
        ,MEASURE
        ,LEVEL
        ,SUM(VALUE) VALUE
FROM {TBL_CALC}
WHERE LEVEL NOT IN ('L1.5','L2.5','L3.5','L4.5')
GROUP BY BIZLINE, YEARMONTH
        ,TYPE
        ,SEGMENT
        ,PRODUCT_LV_4
        ,PRODUCT_LV_3
        ,PRODUCT_LV_2
        ,PRODUCT_LV_1
        ,MEASURE
        ,LEVEL
""",
        ),
        (
            "B15_segment_all",
            f"""
-- SP L963-986: SEGMENT ALL
INSERT INTO {TBL_CALC}
SELECT   BIZLINE, YEARMONTH
        ,TYPE
        ,CUS_TYPE
        ,' ALL'
        ,PRODUCT_LV_4
        ,PRODUCT_LV_3
        ,PRODUCT_LV_2
        ,PRODUCT_LV_1
        ,MEASURE
        ,LEVEL
        ,SUM(VALUE) VALUE
FROM {TBL_CALC}
WHERE LEVEL NOT IN ('L1.5','L2.5','L3.5','L4.5')
GROUP BY BIZLINE, YEARMONTH
        ,TYPE
        ,CUS_TYPE
        ,PRODUCT_LV_4
        ,PRODUCT_LV_3
        ,PRODUCT_LV_2
        ,PRODUCT_LV_1
        ,MEASURE
        ,LEVEL
""",
        ),
        (
            "B16_bizline_all",
            f"""
-- SP L988-1012: BIZLINE ALL
INSERT INTO {TBL_CALC}
SELECT   'ALL', YEARMONTH
        ,TYPE
        ,CUS_TYPE
        ,SEGMENT
        ,PRODUCT_LV_4
        ,PRODUCT_LV_3
        ,PRODUCT_LV_2
        ,PRODUCT_LV_1
        ,MEASURE
        ,LEVEL
        ,SUM(VALUE) VALUE
FROM {TBL_CALC}
WHERE LEVEL NOT IN ('L1.5','L2.5','L3.5','L4.5')
GROUP BY YEARMONTH
        ,TYPE
        ,CUS_TYPE
        ,SEGMENT
        ,PRODUCT_LV_4
        ,PRODUCT_LV_3
        ,PRODUCT_LV_2
        ,PRODUCT_LV_1
        ,MEASURE
        ,LEVEL
""",
        ),
    ]

print(f"Nhóm B: {len(_calc_blocks_b_rollup())} blocks")
```

# Block 13: CALC Nhóm C — `_SUM` Measures + Ratios trước FY

```python
# ============================================================
# BLOCK 13 — CALC NHÓM C: _SUM + %Register/%MTB/... + %Utilization (SP L1015-1172)
# Chạy TRƯỚC FY rollup (giống SP) → FY của các % này lấy tháng 12 (xem block D01).
# ============================================================

def _calc_blocks_c_supplementary():
    return [
        (
            "C01_sum_eop_cus_active_cus_casa_ae",
            f"""
-- SP L1015-1039: AVG BAL & EOP CUS_SUM (20251125)
INSERT INTO {TBL_CALC}
SELECT   BIZLINE, YEARMONTH
        ,TYPE
        ,CUS_TYPE
        ,SEGMENT
        ,PRODUCT_LV_4
        ,PRODUCT_LV_3
        ,PRODUCT_LV_2
        ,PRODUCT_LV_1
        ,MEASURE || '_SUM'
        ,LEVEL
        ,SUM(VALUE) VALUE
FROM {TBL_CALC}
WHERE (MEASURE IN ('EOP CUS','ACTIVE CUS') AND PRODUCT_LV_3 = 'CASA&AE')
GROUP BY BIZLINE, YEARMONTH
        ,TYPE
        ,CUS_TYPE
        ,SEGMENT
        ,PRODUCT_LV_4
        ,PRODUCT_LV_3
        ,PRODUCT_LV_2
        ,PRODUCT_LV_1
        ,MEASURE
        ,LEVEL
""",
        ),
        (
            "C02_sum_sme_avg_bal_eop_cus_active_cus",
            f"""
-- SP L1042-1066: Update 20251125 SME - EOP CUS_SUM ALL PRODUCT
INSERT INTO {TBL_CALC}
SELECT  BIZLINE, YEARMONTH
       ,TYPE
       ,CUS_TYPE
       ,SEGMENT
       ,PRODUCT_LV_4
       ,PRODUCT_LV_3
       ,PRODUCT_LV_2
       ,PRODUCT_LV_1
       ,MEASURE || '_SUM'
       ,LEVEL
       ,SUM(VALUE) VALUE
FROM {TBL_CALC}
WHERE (MEASURE IN ('AVG BAL','EOP CUS','ACTIVE CUS') AND BIZLINE = 'SME')
GROUP BY BIZLINE, YEARMONTH
       ,TYPE
       ,CUS_TYPE
       ,SEGMENT
       ,PRODUCT_LV_4
       ,PRODUCT_LV_3
       ,PRODUCT_LV_2
       ,PRODUCT_LV_1
       ,MEASURE
       ,LEVEL
""",
        ),
        (
            "C03_pct_register_mtb_active_casa_ae",
            f"""
-- SP L1069-1104: 20250925 %Register cus, %MTB, %Truly Active, %Low Active CASA&AE (cap 100%)
INSERT INTO {TBL_CALC}
SELECT   A.BIZLINE
        ,A.YEARMONTH
        ,A.TYPE
        ,A.CUS_TYPE
        ,A.SEGMENT
        ,A.PRODUCT_LV_4
        ,A.PRODUCT_LV_3
        ,A.PRODUCT_LV_2
        ,A.PRODUCT_LV_1
        ,'%' || A.MEASURE
        ,'L2.5' LEVEL
        ,CASE WHEN B.VALUE = 0 THEN 0
              WHEN A.VALUE > B.VALUE THEN 1
              ELSE A.VALUE / B.VALUE END VALUE
FROM
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('REGISTER CUS','TRULY ACTIVE CUS','LOW ACTIVE CUS','MTB CUS')
       AND PRODUCT_LV_3 = 'CASA&AE' AND BIZLINE = 'RETAIL'
    ) A
    INNER JOIN
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('EOP CUS') AND PRODUCT_LV_3 = 'CASA&AE' AND BIZLINE = 'RETAIL'
    ) B
        ON  A.PRODUCT_LV_4 = B.PRODUCT_LV_4
        AND A.PRODUCT_LV_3 = B.PRODUCT_LV_3
        AND A.PRODUCT_LV_2 = B.PRODUCT_LV_2
        AND A.PRODUCT_LV_1 = B.PRODUCT_LV_1
        AND A.LEVEL        = B.LEVEL
        AND A.YEARMONTH    = B.YEARMONTH
        AND A.TYPE         = B.TYPE
        AND A.CUS_TYPE     = B.CUS_TYPE
        AND A.SEGMENT      = B.SEGMENT
        AND A.BIZLINE      = B.BIZLINE
""",
        ),
        (
            "C04_pct_utilization_sme",
            f"""
-- SP L1106-1138: 20250925 %Utilization SME
INSERT INTO {TBL_CALC}
SELECT   A.BIZLINE
        ,A.YEARMONTH
        ,A.TYPE
        ,A.CUS_TYPE
        ,A.SEGMENT
        ,A.PRODUCT_LV_4
        ,A.PRODUCT_LV_3
        ,A.PRODUCT_LV_2
        ,A.PRODUCT_LV_1
        ,'%UTILIZATION'
        ,A.LEVEL
        ,CASE WHEN B.VALUE = 0 THEN NULL ELSE (A.VALUE / B.VALUE) END VALUE
FROM
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('EOP BAL') AND BIZLINE IN ('SME')
    ) A
    INNER JOIN
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('LIMIT APPROVED') AND BIZLINE IN ('SME')
    ) B
        ON  A.PRODUCT_LV_4 = B.PRODUCT_LV_4
        AND A.PRODUCT_LV_3 = B.PRODUCT_LV_3
        AND A.PRODUCT_LV_2 = B.PRODUCT_LV_2
        AND A.PRODUCT_LV_1 = B.PRODUCT_LV_1
        AND A.LEVEL        = B.LEVEL
        AND A.YEARMONTH    = B.YEARMONTH
        AND A.TYPE         = B.TYPE
        AND A.CUS_TYPE     = B.CUS_TYPE
        AND A.SEGMENT      = B.SEGMENT
        AND A.BIZLINE      = B.BIZLINE
""",
        ),
        (
            "C05_pct_utilization_retail",
            f"""
-- SP L1140-1172: 20250925 %Utilization Retail
INSERT INTO {TBL_CALC}
SELECT   A.BIZLINE
        ,A.YEARMONTH
        ,A.TYPE
        ,A.CUS_TYPE
        ,A.SEGMENT
        ,A.PRODUCT_LV_4
        ,A.PRODUCT_LV_3
        ,A.PRODUCT_LV_2
        ,A.PRODUCT_LV_1
        ,'%UTILIZATION'
        ,A.LEVEL
        ,CASE WHEN B.VALUE = 0 THEN NULL ELSE (A.VALUE / B.VALUE) END VALUE
FROM
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('EOP BAL') AND BIZLINE IN ('RETAIL')
    ) A
    INNER JOIN
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('LIMIT') AND BIZLINE IN ('RETAIL')
    ) B
        ON  A.PRODUCT_LV_4 = B.PRODUCT_LV_4
        AND A.PRODUCT_LV_3 = B.PRODUCT_LV_3
        AND A.PRODUCT_LV_2 = B.PRODUCT_LV_2
        AND A.PRODUCT_LV_1 = B.PRODUCT_LV_1
        AND A.LEVEL        = B.LEVEL
        AND A.YEARMONTH    = B.YEARMONTH
        AND A.TYPE         = B.TYPE
        AND A.CUS_TYPE     = B.CUS_TYPE
        AND A.SEGMENT      = B.SEGMENT
        AND A.BIZLINE      = B.BIZLINE
""",
        ),
    ]

print(f"Nhóm C: {len(_calc_blocks_c_supplementary())} blocks")
```

# Block 14: CALC Nhóm D — FY / MTD / YTD + Ratios

```python
# ============================================================
# BLOCK 14 — CALC NHÓM D: FY + MTD + YTD + % DRIVERS (SP L1175-1936)
# FY rollup phân biệt 3 loại: Flow (SUM), Stock (tháng 12 / tháng YTD), Average (SUM/12, SUM/tháng YTD)
# Thêm measure stock mới → thêm vào CẢ list NOT IN và list IN tương ứng trong D01 (FY + YTD).
# MAPPING_DAYS: phía C chuẩn hóa UPPER(RTRIM()) để join với YEARMONTH của CALC.
# ============================================================

def _calc_blocks_d_fy_ratios():
    return [
        (
            "D01_fy_mtd_ytd",
            f"""
-- SP L1175-1263: FULL YEAR + MTD + YTD (1 INSERT, 3 nhánh UNION ALL đọc cùng snapshot CALC)
INSERT INTO {TBL_CALC}
----------------------------------- FY -----------------------------------
SELECT   BIZLINE, LEFT(YEARMONTH,4) || ' FY' YEARMONTH
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,PRODUCT_LV_4
        ,PRODUCT_LV_3
        ,PRODUCT_LV_2
        ,PRODUCT_LV_1
        ,MEASURE
        ,LEVEL
        ,SUM( CASE WHEN MEASURE NOT IN ('AVG BAL','AVG BAL_SUM','EOP BAL','EOP CUS','EOP CUS ACTIVE','EOP CUS HOLDING','IB BAL','AUM','EOP CUS BY PRODUCT','ACTIVE CUS','EOP CUS HAVING LIMIT','EOP CUS_SUM'
                                       ,'REGISTER CUS','TRULY ACTIVE CUS','LOW ACTIVE CUS','MTB CUS','NEW CUS CASA&AE','LIMIT','LIMIT APPROVED','%REGISTER CUS','%MTB CUS','%TRULY ACTIVE CUS','%LOW ACTIVE CUS','%UTILIZATION') THEN VALUE
                   WHEN (MEASURE IN ('EOP BAL','AUM','EOP CUS','ACTIVE CUS','REGISTER CUS','TRULY ACTIVE CUS','LOW ACTIVE CUS','MTB CUS','NEW CUS CASA&AE','LIMIT','LIMIT APPROVED'
                                    ,'%REGISTER CUS','%MTB CUS','%TRULY ACTIVE CUS','%LOW ACTIVE CUS','%UTILIZATION') OR MEASURE LIKE 'EOP CUS%') AND RIGHT(YEARMONTH,2) = '12' THEN VALUE
                   WHEN MEASURE IN ('AVG BAL','IB BAL','AVG BAL_SUM') THEN VALUE / 12
                   ELSE NULL
              END )
FROM {TBL_CALC}
GROUP BY BIZLINE, LEFT(YEARMONTH,4) || ' FY'
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,PRODUCT_LV_4
        ,PRODUCT_LV_3
        ,PRODUCT_LV_2
        ,PRODUCT_LV_1
        ,MEASURE
        ,LEVEL
----------------------------------- MTD ----------------------------------
UNION ALL
SELECT   BIZLINE, YEARMONTH || ' MTD' YEARMONTH
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,PRODUCT_LV_4
        ,PRODUCT_LV_3
        ,PRODUCT_LV_2
        ,PRODUCT_LV_1
        ,MEASURE
        ,LEVEL
        ,SUM(VALUE) VALUE
FROM {TBL_CALC}
WHERE RIGHT(YEARMONTH,2) = RIGHT('{YTD_MONTH}',2)   -- 20260423: YTD
GROUP BY BIZLINE, YEARMONTH || ' MTD'
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,PRODUCT_LV_4
        ,PRODUCT_LV_3
        ,PRODUCT_LV_2
        ,PRODUCT_LV_1
        ,MEASURE
        ,LEVEL
----------------------------------- YTD ----------------------------------
UNION ALL
SELECT   BIZLINE, LEFT(YEARMONTH,4) || RIGHT('{YTD_MONTH}',2) || ' YTD' YEARMONTH
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,PRODUCT_LV_4
        ,PRODUCT_LV_3
        ,PRODUCT_LV_2
        ,PRODUCT_LV_1
        ,MEASURE
        ,LEVEL
        ,SUM( CASE WHEN MEASURE NOT IN ('AVG BAL','AVG BAL_SUM','EOP BAL','EOP CUS','EOP CUS ACTIVE','EOP CUS HOLDING','IB BAL','AUM','EOP CUS BY PRODUCT','ACTIVE CUS','EOP CUS HAVING LIMIT','EOP CUS_SUM'
                                       ,'REGISTER CUS','TRULY ACTIVE CUS','LOW ACTIVE CUS','MTB CUS','NEW CUS CASA&AE','LIMIT','LIMIT APPROVED','%REGISTER CUS','%MTB CUS','%TRULY ACTIVE CUS','%LOW ACTIVE CUS','%UTILIZATION') THEN VALUE
                   WHEN (MEASURE IN ('EOP BAL','AUM','EOP CUS','ACTIVE CUS','REGISTER CUS','TRULY ACTIVE CUS','LOW ACTIVE CUS','MTB CUS','NEW CUS CASA&AE','LIMIT','LIMIT APPROVED'
                                    ,'%REGISTER CUS','%MTB CUS','%TRULY ACTIVE CUS','%LOW ACTIVE CUS','%UTILIZATION') OR MEASURE LIKE 'EOP CUS%') AND RIGHT(YEARMONTH,2) = '12' AND TRY_CAST(LEFT(YEARMONTH,4) AS INT) < 2026 THEN VALUE
                   WHEN (MEASURE IN ('EOP BAL','AUM','EOP CUS','ACTIVE CUS','REGISTER CUS','TRULY ACTIVE CUS','LOW ACTIVE CUS','MTB CUS','NEW CUS CASA&AE','LIMIT','LIMIT APPROVED'
                                    ,'%REGISTER CUS','%MTB CUS','%TRULY ACTIVE CUS','%LOW ACTIVE CUS','%UTILIZATION') OR MEASURE LIKE 'EOP CUS%') AND RIGHT(YEARMONTH,2) = RIGHT('{YTD_MONTH}',2) AND TRY_CAST(LEFT(YEARMONTH,4) AS INT) = 2026 THEN VALUE
                   WHEN MEASURE IN ('AVG BAL','IB BAL') THEN VALUE / CAST(RIGHT('{YTD_MONTH}',2) AS INT)
                   ELSE NULL
              END )
FROM {TBL_CALC}
WHERE YEARMONTH NOT IN ('2024 FY','2025 FY','2026 FY') AND TRY_CAST(YEARMONTH AS INT) <= CAST('{YTD_MONTH}' AS INT)   -- 20260423: YTD
GROUP BY BIZLINE, LEFT(YEARMONTH,4) || RIGHT('{YTD_MONTH}',2) || ' YTD'
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,PRODUCT_LV_4
        ,PRODUCT_LV_3
        ,PRODUCT_LV_2
        ,PRODUCT_LV_1
        ,MEASURE
        ,LEVEL
""",
        ),
        (
            "D02_pct_nim_ci_ftp_int_rev",
            f"""
-- SP L1265-1301: CALCULATION 1.2 %NIM,%CI,%CI_ADJ,%INT_REV,%FTP
INSERT INTO {TBL_CALC}
SELECT   A.BIZLINE
        ,A.YEARMONTH
        ,A.TYPE
        ,A.CUS_TYPE
        ,A.SEGMENT
        ,A.PRODUCT_LV_4
        ,A.PRODUCT_LV_3
        ,A.PRODUCT_LV_2
        ,A.PRODUCT_LV_1
        ,'%' || A.MEASURE
        ,A.LEVEL
        ,CASE WHEN B.VALUE = 0 THEN 0 ELSE (A.VALUE / B.VALUE) * (365 / C.NO_DAY) END VALUE
FROM
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('CI','CI_ADJ','INT_REV','FTP','CI_NORMAL','LIQUIDITY COST')
       AND PRODUCT_LV_4 IN ('TOTAL LENDING/CARD','TOTAL DEPOSIT')
    ) A
    INNER JOIN
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE = 'AVG BAL'
       AND PRODUCT_LV_4 IN ('TOTAL LENDING/CARD','TOTAL DEPOSIT')
    ) B
        ON  A.PRODUCT_LV_4 = B.PRODUCT_LV_4
        AND A.PRODUCT_LV_3 = B.PRODUCT_LV_3
        AND A.PRODUCT_LV_2 = B.PRODUCT_LV_2
        AND A.PRODUCT_LV_1 = B.PRODUCT_LV_1
        AND A.LEVEL        = B.LEVEL
        AND A.YEARMONTH    = B.YEARMONTH
        AND A.TYPE         = B.TYPE
        AND A.CUS_TYPE     = B.CUS_TYPE
        AND A.SEGMENT      = B.SEGMENT
        AND A.BIZLINE      = B.BIZLINE
    LEFT JOIN {TBL_MAPPING_DAYS} C ON A.YEARMONTH = UPPER(RTRIM(CAST(C.YEARMONTH AS STRING)))
""",
        ),
        (
            "D03_pct_nim_budget_line",
            f"""
-- SP L1303-1338: %NIM (Budget line adjustment) 20260210 — FI không đưa vào %NIM, CI_adj đưa vào OPEX
INSERT INTO {TBL_CALC}
SELECT   A.BIZLINE
        ,A.YEARMONTH
        ,A.TYPE
        ,A.CUS_TYPE
        ,A.SEGMENT
        ,A.PRODUCT_LV_4
        ,A.PRODUCT_LV_3
        ,A.PRODUCT_LV_2
        ,A.PRODUCT_LV_1
        ,'%NIM'
        ,A.LEVEL
        ,CASE WHEN B.VALUE = 0 THEN 0 ELSE (A.VALUE / B.VALUE) * (365 / C.NO_DAY) END VALUE
FROM
    (SELECT * FROM {TBL_CALC}
     WHERE (MEASURE IN ('NII') AND PRODUCT_LV_4 IN ('TOTAL DEPOSIT'))
        OR (MEASURE IN ('CI','INT_REV','FTP') AND PRODUCT_LV_4 IN ('TOTAL LENDING/CARD'))
    ) A
    INNER JOIN
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE = 'AVG BAL'
       AND PRODUCT_LV_4 IN ('TOTAL LENDING/CARD','TOTAL DEPOSIT')
    ) B
        ON  A.PRODUCT_LV_4 = B.PRODUCT_LV_4
        AND A.PRODUCT_LV_3 = B.PRODUCT_LV_3
        AND A.PRODUCT_LV_2 = B.PRODUCT_LV_2
        AND A.PRODUCT_LV_1 = B.PRODUCT_LV_1
        AND A.LEVEL        = B.LEVEL
        AND A.YEARMONTH    = B.YEARMONTH
        AND A.TYPE         = B.TYPE
        AND A.CUS_TYPE     = B.CUS_TYPE
        AND A.SEGMENT      = B.SEGMENT
        AND A.BIZLINE      = B.BIZLINE
    LEFT JOIN {TBL_MAPPING_DAYS} C ON A.YEARMONTH = UPPER(RTRIM(CAST(C.YEARMONTH AS STRING)))
""",
        ),
        (
            "D04_pct_margin_sme",
            f"""
-- SP L1340-1415: SME %Margin (Update 20250925)
-- NOTE: SP dùng literal 'LC Interest ' / 'LG Interest ' (có trailing space) — SQL Server bỏ qua trailing space khi so sánh
INSERT INTO {TBL_CALC}
SELECT   A.BIZLINE
        ,A.YEARMONTH
        ,A.TYPE
        ,A.CUS_TYPE
        ,A.SEGMENT
        ,A.PRODUCT_LV_4
        ,A.PRODUCT_LV_3
        ,A.PRODUCT_LV_2
        ,A.PRODUCT_LV_1
        ,'%MARGIN'
        ,A.LEVEL
        ,CASE WHEN B.VALUE = 0 THEN 0 ELSE (A.VALUE / B.VALUE) * (365 / C.NO_DAY) END VALUE
FROM
    (SELECT * FROM {TBL_CALC}
     WHERE (MEASURE = 'NII' AND PRODUCT_LV_1 IN ('LC INTEREST','LG INTEREST'))
       AND BIZLINE = 'SME'
    ) A
    INNER JOIN
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE = 'AVG BAL' AND PRODUCT_LV_1 IN ('LC INTEREST','LG INTEREST')
       AND BIZLINE = 'SME'
    ) B
        ON  A.PRODUCT_LV_4 = B.PRODUCT_LV_4
        AND A.PRODUCT_LV_3 = B.PRODUCT_LV_3
        AND A.PRODUCT_LV_2 = B.PRODUCT_LV_2
        AND A.PRODUCT_LV_1 = B.PRODUCT_LV_1
        AND A.LEVEL        = B.LEVEL
        AND A.YEARMONTH    = B.YEARMONTH
        AND A.TYPE         = B.TYPE
        AND A.CUS_TYPE     = B.CUS_TYPE
        AND A.SEGMENT      = B.SEGMENT
        AND A.BIZLINE      = B.BIZLINE
    LEFT JOIN {TBL_MAPPING_DAYS} C ON A.YEARMONTH = UPPER(RTRIM(CAST(C.YEARMONTH AS STRING)))

UNION ALL

SELECT   A.BIZLINE
        ,A.YEARMONTH
        ,A.TYPE
        ,A.CUS_TYPE
        ,A.SEGMENT
        ,A.PRODUCT_LV_4
        ,A.PRODUCT_LV_3
        ,A.PRODUCT_LV_2
        ,A.PRODUCT_LV_1
        ,'%MARGIN'
        ,A.LEVEL
        ,CASE WHEN B.VALUE = 0 THEN 0 ELSE (A.VALUE / B.VALUE) * (365 / C.NO_DAY) END VALUE
FROM
    (SELECT * FROM {TBL_CALC}
     WHERE (MEASURE = 'NFI' AND PRODUCT_LV_1 IN ('LG FEE','LC FEE'))
       AND BIZLINE = 'SME'
    ) A
    INNER JOIN
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE = 'AVG BAL' AND PRODUCT_LV_1 IN ('LG FEE','LC FEE')
       AND BIZLINE = 'SME'
    ) B
        ON  A.PRODUCT_LV_4 = B.PRODUCT_LV_4
        AND A.PRODUCT_LV_3 = B.PRODUCT_LV_3
        AND A.PRODUCT_LV_2 = B.PRODUCT_LV_2
        AND A.PRODUCT_LV_1 = B.PRODUCT_LV_1
        AND A.LEVEL        = B.LEVEL
        AND A.YEARMONTH    = B.YEARMONTH
        AND A.TYPE         = B.TYPE
        AND A.CUS_TYPE     = B.CUS_TYPE
        AND A.SEGMENT      = B.SEGMENT
        AND A.BIZLINE      = B.BIZLINE
    LEFT JOIN {TBL_MAPPING_DAYS} C ON A.YEARMONTH = UPPER(RTRIM(CAST(C.YEARMONTH AS STRING)))
""",
        ),
        (
            "D05_pct_active_casa_sme",
            f"""
-- SP L1417-1452: % ACTIVE CASA (20250925)
INSERT INTO {TBL_CALC}
SELECT   A.BIZLINE
        ,A.YEARMONTH
        ,A.TYPE
        ,A.CUS_TYPE
        ,A.SEGMENT
        ,A.PRODUCT_LV_4
        ,A.PRODUCT_LV_3
        ,A.PRODUCT_LV_2
        ,A.PRODUCT_LV_1
        ,'%ACTIVE CASA'
        ,A.LEVEL
        ,CASE WHEN B.VALUE = 0 THEN 0
              WHEN A.VALUE > B.VALUE THEN 1
              ELSE A.VALUE / B.VALUE END VALUE
FROM
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('ACTIVE CUS_SUM') AND BIZLINE = 'SME' AND PRODUCT_LV_3 IN ('CASA')
    ) A
    INNER JOIN
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('EOP CUS_SUM') AND PRODUCT_LV_3 IN ('CASA') AND BIZLINE = 'SME'
    ) B
        ON  A.PRODUCT_LV_4 = B.PRODUCT_LV_4
        AND A.PRODUCT_LV_3 = B.PRODUCT_LV_3
        AND A.PRODUCT_LV_2 = B.PRODUCT_LV_2
        AND A.PRODUCT_LV_1 = B.PRODUCT_LV_1
        AND A.LEVEL        = B.LEVEL
        AND A.YEARMONTH    = B.YEARMONTH
        AND A.TYPE         = B.TYPE
        AND A.CUS_TYPE     = B.CUS_TYPE
        AND A.SEGMENT      = B.SEGMENT
        AND A.BIZLINE      = B.BIZLINE
""",
        ),
        (
            "D06_pct_active_card",
            f"""
-- SP L1455-1485: % ACTIVE CARD
INSERT INTO {TBL_CALC}
SELECT   A.BIZLINE
        ,A.YEARMONTH
        ,A.TYPE
        ,A.CUS_TYPE
        ,A.SEGMENT
        ,A.PRODUCT_LV_4
        ,A.PRODUCT_LV_3
        ,A.PRODUCT_LV_2
        ,A.PRODUCT_LV_1
        ,'%ACTIVE CARD'
        ,A.LEVEL
        ,CASE WHEN B.VALUE = 0 THEN 0 ELSE (A.VALUE / B.VALUE) END VALUE
FROM
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('ACTIVE CARD') AND PRODUCT_LV_3 IN ('CARD')
    ) A
    INNER JOIN
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('EOP CARD') AND PRODUCT_LV_3 IN ('CARD')
    ) B
        ON  A.PRODUCT_LV_4 = B.PRODUCT_LV_4
        AND A.PRODUCT_LV_3 = B.PRODUCT_LV_3
        AND A.PRODUCT_LV_2 = B.PRODUCT_LV_2
        AND A.PRODUCT_LV_1 = B.PRODUCT_LV_1
        AND A.LEVEL        = B.LEVEL
        AND A.YEARMONTH    = B.YEARMONTH
        AND A.TYPE         = B.TYPE
        AND A.CUS_TYPE     = B.CUS_TYPE
        AND A.SEGMENT      = B.SEGMENT
        AND A.BIZLINE      = B.BIZLINE
""",
        ),
        (
            "D07_ticket_size_sme",
            f"""
-- SP L1487-1519: 20260410 Ticket size SME
INSERT INTO {TBL_CALC}
SELECT   A.BIZLINE
        ,A.YEARMONTH
        ,A.TYPE
        ,A.CUS_TYPE
        ,A.SEGMENT
        ,A.PRODUCT_LV_4
        ,A.PRODUCT_LV_3
        ,A.PRODUCT_LV_2
        ,A.PRODUCT_LV_1
        ,'TICKET SIZE'
        ,A.LEVEL
        ,CASE WHEN B.VALUE = 0 THEN NULL ELSE (A.VALUE / B.VALUE) END VALUE
FROM
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('AVG BAL_SUM') AND BIZLINE IN ('SME')
    ) A
    INNER JOIN
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('EOP CUS_SUM') AND BIZLINE IN ('SME')
    ) B
        ON  A.PRODUCT_LV_4 = B.PRODUCT_LV_4
        AND A.PRODUCT_LV_3 = B.PRODUCT_LV_3
        AND A.PRODUCT_LV_2 = B.PRODUCT_LV_2
        AND A.PRODUCT_LV_1 = B.PRODUCT_LV_1
        AND A.LEVEL        = B.LEVEL
        AND A.YEARMONTH    = B.YEARMONTH
        AND A.TYPE         = B.TYPE
        AND A.CUS_TYPE     = B.CUS_TYPE
        AND A.SEGMENT      = B.SEGMENT
        AND A.BIZLINE      = B.BIZLINE
""",
        ),
        (
            "D08_pct_nfi_txn_amt_sme",
            f"""
-- SP L1521-1553: 20251125 %NFI/Txn Amt SME
INSERT INTO {TBL_CALC}
SELECT   A.BIZLINE
        ,A.YEARMONTH
        ,A.TYPE
        ,A.CUS_TYPE
        ,A.SEGMENT
        ,A.PRODUCT_LV_4
        ,A.PRODUCT_LV_3
        ,A.PRODUCT_LV_2
        ,A.PRODUCT_LV_1
        ,'%NFI/TXN AMT'
        ,A.LEVEL
        ,CASE WHEN B.VALUE = 0 THEN NULL ELSE (A.VALUE / B.VALUE) END VALUE
FROM
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('NFI') AND BIZLINE IN ('SME')
    ) A
    INNER JOIN
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('TNX AMT','TXN AMT') AND BIZLINE IN ('SME')
    ) B
        ON  A.PRODUCT_LV_4 = B.PRODUCT_LV_4
        AND A.PRODUCT_LV_3 = B.PRODUCT_LV_3
        AND A.PRODUCT_LV_2 = B.PRODUCT_LV_2
        AND A.PRODUCT_LV_1 = B.PRODUCT_LV_1
        AND A.LEVEL        = B.LEVEL
        AND A.YEARMONTH    = B.YEARMONTH
        AND A.TYPE         = B.TYPE
        AND A.CUS_TYPE     = B.CUS_TYPE
        AND A.SEGMENT      = B.SEGMENT
        AND A.BIZLINE      = B.BIZLINE
""",
        ),
        (
            "D09_pct_trading_income_txn_amt_sme",
            f"""
-- SP L1555-1587: 20250925 %Trading Income/Txn Amt SME
INSERT INTO {TBL_CALC}
SELECT   A.BIZLINE
        ,A.YEARMONTH
        ,A.TYPE
        ,A.CUS_TYPE
        ,A.SEGMENT
        ,A.PRODUCT_LV_4
        ,A.PRODUCT_LV_3
        ,A.PRODUCT_LV_2
        ,A.PRODUCT_LV_1
        ,'%TRADING INCOME/TXN AMT'
        ,A.LEVEL
        ,CASE WHEN B.VALUE = 0 THEN NULL ELSE (A.VALUE / B.VALUE) END VALUE
FROM
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('TRADING INCOME') AND BIZLINE IN ('SME')
    ) A
    INNER JOIN
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('TNX AMT','TXN AMT') AND BIZLINE IN ('SME')
    ) B
        ON  A.PRODUCT_LV_4 = B.PRODUCT_LV_4
        AND A.PRODUCT_LV_3 = B.PRODUCT_LV_3
        AND A.PRODUCT_LV_2 = B.PRODUCT_LV_2
        AND A.PRODUCT_LV_1 = B.PRODUCT_LV_1
        AND A.LEVEL        = B.LEVEL
        AND A.YEARMONTH    = B.YEARMONTH
        AND A.TYPE         = B.TYPE
        AND A.CUS_TYPE     = B.CUS_TYPE
        AND A.SEGMENT      = B.SEGMENT
        AND A.BIZLINE      = B.BIZLINE
""",
        ),
        (
            "D10_pct_nfi_new_bal",
            f"""
-- SP L1589-1623: 20250925 %NFI/New Bal for investment product
INSERT INTO {TBL_CALC}
SELECT   A.BIZLINE
        ,A.YEARMONTH
        ,A.TYPE
        ,A.CUS_TYPE
        ,A.SEGMENT
        ,A.PRODUCT_LV_4
        ,A.PRODUCT_LV_3
        ,A.PRODUCT_LV_2
        ,A.PRODUCT_LV_1
        ,'%NFI/NEW BAL'
        ,A.LEVEL
        ,CASE WHEN B.VALUE = 0 THEN 0
              WHEN A.VALUE > B.VALUE THEN 1
              ELSE A.VALUE / B.VALUE END VALUE
FROM
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('NFI') AND BIZLINE IN ('RETAIL')
    ) A
    INNER JOIN
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('NEW BAL') AND BIZLINE IN ('RETAIL')
    ) B
        ON  A.PRODUCT_LV_4 = B.PRODUCT_LV_4
        AND A.PRODUCT_LV_3 = B.PRODUCT_LV_3
        AND A.PRODUCT_LV_2 = B.PRODUCT_LV_2
        AND A.PRODUCT_LV_1 = B.PRODUCT_LV_1
        AND A.LEVEL        = B.LEVEL
        AND A.YEARMONTH    = B.YEARMONTH
        AND A.TYPE         = B.TYPE
        AND A.CUS_TYPE     = B.CUS_TYPE
        AND A.SEGMENT      = B.SEGMENT
        AND A.BIZLINE      = B.BIZLINE
""",
        ),
        (
            "D11_pct_fee_dpm_gold",
            f"""
-- SP L1625-1659: 20250925 %Fee DPM and Gold
INSERT INTO {TBL_CALC}
SELECT   A.BIZLINE
        ,A.YEARMONTH
        ,A.TYPE
        ,A.CUS_TYPE
        ,A.SEGMENT
        ,A.PRODUCT_LV_4
        ,A.PRODUCT_LV_3
        ,A.PRODUCT_LV_2
        ,A.PRODUCT_LV_1
        ,'%FEE'
        ,A.LEVEL
        ,CASE WHEN B.VALUE = 0 THEN 0
              WHEN A.VALUE > B.VALUE THEN 1
              ELSE A.VALUE / B.VALUE END VALUE
FROM
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('NFI') AND BIZLINE IN ('RETAIL')
    ) A
    INNER JOIN
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('EOP BAL') AND BIZLINE IN ('RETAIL')
    ) B
        ON  A.PRODUCT_LV_4 = B.PRODUCT_LV_4
        AND A.PRODUCT_LV_3 = B.PRODUCT_LV_3
        AND A.PRODUCT_LV_2 = B.PRODUCT_LV_2
        AND A.PRODUCT_LV_1 = B.PRODUCT_LV_1
        AND A.LEVEL        = B.LEVEL
        AND A.YEARMONTH    = B.YEARMONTH
        AND A.TYPE         = B.TYPE
        AND A.CUS_TYPE     = B.CUS_TYPE
        AND A.SEGMENT      = B.SEGMENT
        AND A.BIZLINE      = B.BIZLINE
""",
        ),
        (
            "D12_pct_nfi_ape_banca",
            f"""
-- SP L1661-1695: 20250925 %NFI/APE for Banca
INSERT INTO {TBL_CALC}
SELECT   A.BIZLINE
        ,A.YEARMONTH
        ,A.TYPE
        ,A.CUS_TYPE
        ,A.SEGMENT
        ,A.PRODUCT_LV_4
        ,A.PRODUCT_LV_3
        ,A.PRODUCT_LV_2
        ,A.PRODUCT_LV_1
        ,'%NFI/APE'
        ,A.LEVEL
        ,CASE WHEN B.VALUE = 0 THEN 0
              WHEN A.VALUE > B.VALUE THEN 1
              ELSE A.VALUE / B.VALUE END VALUE
FROM
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('NFI')
    ) A
    INNER JOIN
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('APE')
    ) B
        ON  A.PRODUCT_LV_4 = B.PRODUCT_LV_4
        AND A.PRODUCT_LV_3 = B.PRODUCT_LV_3
        AND A.PRODUCT_LV_2 = B.PRODUCT_LV_2
        AND A.PRODUCT_LV_1 = B.PRODUCT_LV_1
        AND A.LEVEL        = B.LEVEL
        AND A.YEARMONTH    = B.YEARMONTH
        AND A.TYPE         = B.TYPE
        AND A.CUS_TYPE     = B.CUS_TYPE
        AND A.SEGMENT      = B.SEGMENT
        AND A.BIZLINE      = B.BIZLINE
""",
        ),
        (
            "D13_pct_nfi_gwp_banca",
            f"""
-- SP L1697-1731: 20250925 %NFI/GWP for Banca
INSERT INTO {TBL_CALC}
SELECT   A.BIZLINE
        ,A.YEARMONTH
        ,A.TYPE
        ,A.CUS_TYPE
        ,A.SEGMENT
        ,A.PRODUCT_LV_4
        ,A.PRODUCT_LV_3
        ,A.PRODUCT_LV_2
        ,A.PRODUCT_LV_1
        ,'%NFI/GWP'
        ,A.LEVEL
        ,CASE WHEN B.VALUE = 0 THEN 0
              WHEN A.VALUE > B.VALUE THEN 1
              ELSE A.VALUE / B.VALUE END VALUE
FROM
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('NFI')
    ) A
    INNER JOIN
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('GWP')
    ) B
        ON  A.PRODUCT_LV_4 = B.PRODUCT_LV_4
        AND A.PRODUCT_LV_3 = B.PRODUCT_LV_3
        AND A.PRODUCT_LV_2 = B.PRODUCT_LV_2
        AND A.PRODUCT_LV_1 = B.PRODUCT_LV_1
        AND A.LEVEL        = B.LEVEL
        AND A.YEARMONTH    = B.YEARMONTH
        AND A.TYPE         = B.TYPE
        AND A.CUS_TYPE     = B.CUS_TYPE
        AND A.SEGMENT      = B.SEGMENT
        AND A.BIZLINE      = B.BIZLINE
""",
        ),
        (
            "D14_nfi_pv_card",
            f"""
-- SP L1735-1768: 20250925 NFI/PV
INSERT INTO {TBL_CALC}
SELECT   A.BIZLINE
        ,A.YEARMONTH
        ,A.TYPE
        ,A.CUS_TYPE
        ,A.SEGMENT
        ,A.PRODUCT_LV_4
        ,A.PRODUCT_LV_3
        ,A.PRODUCT_LV_2
        ,A.PRODUCT_LV_1
        ,'NFI/PV'
        ,A.LEVEL
        ,CASE WHEN B.VALUE = 0 THEN 0
              WHEN A.VALUE > B.VALUE THEN 1
              ELSE A.VALUE / B.VALUE END VALUE
FROM
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('NFI') AND PRODUCT_LV_3 = 'CARD' AND BIZLINE = 'RETAIL'
    ) A
    INNER JOIN
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('PURCHASE AMT') AND PRODUCT_LV_3 = 'CARD' AND BIZLINE = 'RETAIL'
    ) B
        ON  A.PRODUCT_LV_4 = B.PRODUCT_LV_4
        AND A.PRODUCT_LV_3 = B.PRODUCT_LV_3
        AND A.PRODUCT_LV_2 = B.PRODUCT_LV_2
        AND A.PRODUCT_LV_1 = B.PRODUCT_LV_1
        AND A.LEVEL        = B.LEVEL
        AND A.YEARMONTH    = B.YEARMONTH
        AND A.TYPE         = B.TYPE
        AND A.CUS_TYPE     = B.CUS_TYPE
        AND A.SEGMENT      = B.SEGMENT
        AND A.BIZLINE      = B.BIZLINE
""",
        ),
        (
            "D15_pct_penetration",
            f"""
-- SP L1771-1803: CALCULATION 1.3 %PENETRATION (join 5 keys — giống SP)
INSERT INTO {TBL_CALC}
SELECT   A.BIZLINE
        ,A.YEARMONTH
        ,A.TYPE
        ,A.CUS_TYPE
        ,A.SEGMENT
        ,A.PRODUCT_LV_4
        ,A.PRODUCT_LV_3
        ,A.PRODUCT_LV_2
        ,A.PRODUCT_LV_1
        ,'%PENETRATION'
        ,A.LEVEL
        ,CASE WHEN B.VALUE = 0 THEN 0
              WHEN A.VALUE > B.VALUE THEN 1
              ELSE A.VALUE / B.VALUE END VALUE
FROM
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('EOP CUS')
       AND PRODUCT_LV_4 <> 'CUS'
    ) A
    INNER JOIN
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('EOP CUS HOLDING')
       AND PRODUCT_LV_4 = 'CUS'
       AND LEVEL = 'L1'   -- 20260410: Update
    ) B
        ON  A.YEARMONTH = B.YEARMONTH
        AND A.TYPE      = B.TYPE
        AND A.CUS_TYPE  = B.CUS_TYPE
        AND A.SEGMENT   = B.SEGMENT
        AND A.BIZLINE   = B.BIZLINE
""",
        ),
        (
            "D16_pct_penetration_cd",
            f"""
-- SP L1807-1839: CALCULATION 1.3.2 %PENETRATION OF CD
INSERT INTO {TBL_CALC}
SELECT   A.BIZLINE
        ,A.YEARMONTH
        ,A.TYPE
        ,A.CUS_TYPE
        ,A.SEGMENT
        ,A.PRODUCT_LV_4
        ,A.PRODUCT_LV_3
        ,A.PRODUCT_LV_2
        ,A.PRODUCT_LV_1
        ,'%PENETRATION'
        ,A.LEVEL
        ,CASE WHEN B.VALUE = 0 THEN 0
              WHEN A.VALUE > B.VALUE THEN 1
              ELSE A.VALUE / B.VALUE END VALUE
FROM
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('EOP CUS BY PRODUCT')
       AND PRODUCT_LV_2 = 'CD'
    ) A
    INNER JOIN
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('EOP CUS HOLDING')
       AND PRODUCT_LV_4 = 'CUS'
       AND LEVEL = 'L1'   -- 20260410: Update
    ) B
        ON  A.YEARMONTH = B.YEARMONTH
        AND A.TYPE      = B.TYPE
        AND A.CUS_TYPE  = B.CUS_TYPE
        AND A.SEGMENT   = B.SEGMENT
        AND A.BIZLINE   = B.BIZLINE
""",
        ),
        (
            "D17_other_asset_eop_bal_l1.5",
            f"""
-- SP L1842-1851: 20250925 Other asset EOP Bal (L1.5, không GROUP BY)
INSERT INTO {TBL_CALC}
SELECT BIZLINE, `MONTH`, TYPE
      ,' ALL' CUS_TYPE, ' ALL' SEGMENT
      ,'TOTAL DEPOSIT', PRODUCT_FAMILY, PRODUCT_LINE, SUB_PRODUCT
      ,MEASURE
      ,'L1.5'
      ,VALUE
FROM {TBL_CONSOL}
WHERE PRODUCT_LINE = 'OTHER ASSET' AND MEASURE = 'EOP BAL'
""",
        ),
        (
            "D18_other_asset_eop_bal_l2.5",
            f"""
-- SP L1853-1862
INSERT INTO {TBL_CALC}
SELECT BIZLINE, `MONTH`, TYPE
      ,' ALL' CUS_TYPE, ' ALL' SEGMENT
      ,'TOTAL DEPOSIT', PRODUCT_FAMILY, PRODUCT_LINE, 'ALL'
      ,MEASURE
      ,'L2.5'
      ,SUM(VALUE)
FROM {TBL_CONSOL}
WHERE PRODUCT_LINE = 'OTHER ASSET' AND MEASURE = 'EOP BAL'
GROUP BY BIZLINE, `MONTH`, TYPE, PRODUCT_FAMILY, PRODUCT_LINE, MEASURE
""",
        ),
        (
            "D19_casa_ae_bal_w_other_asset",
            f"""
-- SP L1864-1876 (GROUP BY có PRODUCT_LINE → mỗi product line 1 row, giống SP)
INSERT INTO {TBL_CALC}
SELECT BIZLINE, `MONTH`, TYPE
      ,' ALL' CUS_TYPE, ' ALL' SEGMENT
      ,'TOTAL DEPOSIT', PRODUCT_FAMILY, 'ALL', 'ALL'
      ,CASE WHEN MEASURE = 'EOP BAL' THEN 'EOP BAL W OTHER ASSET'
            WHEN MEASURE = 'AVG BAL' THEN 'AVG BAL W OTHER ASSET'
            ELSE MEASURE END AS MEASURE
      ,'L2.5'
      ,SUM(VALUE)
FROM {TBL_CONSOL}
WHERE PRODUCT_FAMILY = 'CASA&AE'
  AND PRODUCT_LINE IN ('CASA','CD AE','OTHER ASSET') AND MEASURE IN ('EOP BAL','AVG BAL')
GROUP BY BIZLINE, `MONTH`, TYPE, PRODUCT_FAMILY, PRODUCT_LINE, MEASURE
""",
        ),
        (
            "D20_ytd_bal_w_other_asset",
            f"""
-- SP L1878-1903: YTD cho 'EOP/AVG BAL W OTHER ASSET' (20260226)
-- NOTE: giống SP, block này insert thêm row VALUE = NULL cho mọi measure khác (CASE → NULL); PIVOT SUM bỏ qua NULL.
INSERT INTO {TBL_CALC}
SELECT   BIZLINE, LEFT(YEARMONTH,4) || RIGHT('{YTD_MONTH}',2) || ' YTD' YEARMONTH
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,PRODUCT_LV_4
        ,PRODUCT_LV_3
        ,PRODUCT_LV_2
        ,PRODUCT_LV_1
        ,MEASURE
        ,LEVEL
        ,SUM( CASE WHEN MEASURE IN ('EOP BAL W OTHER ASSET') AND RIGHT(YEARMONTH,2) = '12' AND TRY_CAST(LEFT(YEARMONTH,4) AS INT) < 2026 THEN VALUE
                   WHEN MEASURE IN ('EOP BAL W OTHER ASSET') AND RIGHT(YEARMONTH,2) = RIGHT('{YTD_MONTH}',2) AND TRY_CAST(LEFT(YEARMONTH,4) AS INT) = 2026 THEN VALUE
                   WHEN MEASURE IN ('AVG BAL W OTHER ASSET') THEN VALUE / CAST(RIGHT('{YTD_MONTH}',2) AS INT)
                   ELSE NULL END ) VALUE
FROM {TBL_CALC}
WHERE YEARMONTH NOT IN ('2024 FY','2025 FY','2026 FY') AND TRY_CAST(LEFT(YEARMONTH,6) AS INT) <= CAST('{YTD_MONTH}' AS INT)   -- 20260226: YTD
GROUP BY BIZLINE, LEFT(YEARMONTH,4) || RIGHT('{YTD_MONTH}',2) || ' YTD'
        ,TYPE
        ,CUS_TYPE, SEGMENT
        ,PRODUCT_LV_4
        ,PRODUCT_LV_3
        ,PRODUCT_LV_2
        ,PRODUCT_LV_1
        ,MEASURE
        ,LEVEL
""",
        ),
        (
            "D21_txn_amt_per_cus_fx_flow",
            f"""
-- SP L1905-1936: CALCULATION 1.4 NFX: TRANSACTION PER CUS (unit: Mil USD)
INSERT INTO {TBL_CALC}
SELECT   A.BIZLINE
        ,A.YEARMONTH
        ,A.TYPE
        ,A.CUS_TYPE
        ,A.SEGMENT
        ,A.PRODUCT_LV_4
        ,A.PRODUCT_LV_3
        ,A.PRODUCT_LV_2
        ,A.PRODUCT_LV_1
        ,'TXN AMT PER CUS'
        ,A.LEVEL
        ,CASE WHEN B.VALUE = 0 THEN NULL ELSE (A.VALUE / B.VALUE) * 1E3 END VALUE
FROM
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('TXN AMT','TNX AMT') AND PRODUCT_LV_1 IN ('FX FLOW')
    ) A
    INNER JOIN
    (SELECT * FROM {TBL_CALC}
     WHERE MEASURE IN ('EOP CUS') AND PRODUCT_LV_1 IN ('FX FLOW')
    ) B
        ON  A.PRODUCT_LV_4 = B.PRODUCT_LV_4
        AND A.PRODUCT_LV_3 = B.PRODUCT_LV_3
        AND A.PRODUCT_LV_2 = B.PRODUCT_LV_2
        AND A.PRODUCT_LV_1 = B.PRODUCT_LV_1
        AND A.LEVEL        = B.LEVEL
        AND A.YEARMONTH    = B.YEARMONTH
        AND A.TYPE         = B.TYPE
        AND A.CUS_TYPE     = B.CUS_TYPE
        AND A.SEGMENT      = B.SEGMENT
        AND A.BIZLINE      = B.BIZLINE
""",
        ),
    ]

print(f"Nhóm D: {len(_calc_blocks_d_fy_ratios())} blocks")
```

# Block 15: CALC Nhóm E — Variance (Forecast vs Budget / Revised Budget / Forecast LM)

```python
# ============================================================
# BLOCK 15 — CALC NHÓM E: VARIANCE (SP L1978-2072) — chạy sau FY rollup
# SQL Server PIVOT ([FORECAST],[BUDGET]) → Spark PIVOT (... IN ('FORECAST' AS FORECAST, ...))
# SP L1938-1976 (2026 - 2025, 2026 / 2025) đang comment trong SP → không migrate.
# ============================================================

def _calc_blocks_e_variance():
    return [
        (
            "E01_forecast_minus_budget",
            f"""
-- SP L1978-1992: Calculation 1.6 Forecast 2026 - Budget 2026
INSERT INTO {TBL_CALC}
SELECT   BIZLINE, YEARMONTH
        ,'FORECAST - BUDGET'
        ,CUS_TYPE, SEGMENT, PRODUCT_LV_4, PRODUCT_LV_3, PRODUCT_LV_2, PRODUCT_LV_1, MEASURE, LEVEL
        ,CASE WHEN FORECAST <> 0 THEN COALESCE(FORECAST,0) - COALESCE(BUDGET,0) END
FROM (  SELECT BIZLINE, YEARMONTH, TYPE, CUS_TYPE, SEGMENT, PRODUCT_LV_4, PRODUCT_LV_3, PRODUCT_LV_2, PRODUCT_LV_1, MEASURE, LEVEL, VALUE
        FROM {TBL_CALC}
        WHERE (YEARMONTH LIKE '2026%' OR YEARMONTH LIKE '2025%')
          AND TYPE IN ('BUDGET','FORECAST')
     ) A
PIVOT (SUM(VALUE) FOR TYPE IN ('FORECAST' AS FORECAST, 'BUDGET' AS BUDGET))
""",
        ),
        (
            "E02_forecast_div_budget",
            f"""
-- SP L1994-2008: CALCULATION 1.6 Forecast 2026 / Budget 2026
INSERT INTO {TBL_CALC}
SELECT   BIZLINE, YEARMONTH
        ,'FORECAST / BUDGET'
        ,CUS_TYPE, SEGMENT, PRODUCT_LV_4, PRODUCT_LV_3, PRODUCT_LV_2, PRODUCT_LV_1, MEASURE, LEVEL
        ,CASE WHEN FORECAST <> 0 AND BUDGET <> 0 THEN (COALESCE(FORECAST,0) - COALESCE(BUDGET,0)) / ABS(BUDGET) END
FROM (  SELECT BIZLINE, YEARMONTH, TYPE, CUS_TYPE, SEGMENT, PRODUCT_LV_4, PRODUCT_LV_3, PRODUCT_LV_2, PRODUCT_LV_1, MEASURE, LEVEL, VALUE
        FROM {TBL_CALC}
        WHERE (YEARMONTH LIKE '2026%' OR YEARMONTH LIKE '2025%')
          AND TYPE IN ('BUDGET','FORECAST')
     ) A
PIVOT (SUM(VALUE) FOR TYPE IN ('FORECAST' AS FORECAST, 'BUDGET' AS BUDGET))
""",
        ),
        (
            "E03_forecast_minus_revised_budget",
            f"""
-- SP L2012-2026: Calculation 1.7 Forecast 2026 - Revised_Budget 2026
INSERT INTO {TBL_CALC}
SELECT   BIZLINE, YEARMONTH
        ,'FORECAST - REVISED_BUDGET'
        ,CUS_TYPE, SEGMENT, PRODUCT_LV_4, PRODUCT_LV_3, PRODUCT_LV_2, PRODUCT_LV_1, MEASURE, LEVEL
        ,CASE WHEN FORECAST <> 0 AND REVISED_BUDGET <> 0 THEN COALESCE(FORECAST,0) - COALESCE(REVISED_BUDGET,0) END
FROM (  SELECT BIZLINE, YEARMONTH, TYPE, CUS_TYPE, SEGMENT, PRODUCT_LV_4, PRODUCT_LV_3, PRODUCT_LV_2, PRODUCT_LV_1, MEASURE, LEVEL, VALUE
        FROM {TBL_CALC}
        WHERE (YEARMONTH LIKE '2026%' OR YEARMONTH LIKE '2025%')
          AND TYPE IN ('REVISED_BUDGET','FORECAST')
     ) A
PIVOT (SUM(VALUE) FOR TYPE IN ('FORECAST' AS FORECAST, 'REVISED_BUDGET' AS REVISED_BUDGET))
""",
        ),
        (
            "E04_forecast_div_revised_budget",
            f"""
-- SP L2029-2043: CALCULATION 1.7 Forecast 2026 / Revised_Budget 2026
INSERT INTO {TBL_CALC}
SELECT   BIZLINE, YEARMONTH
        ,'FORECAST / REVISED_BUDGET'
        ,CUS_TYPE, SEGMENT, PRODUCT_LV_4, PRODUCT_LV_3, PRODUCT_LV_2, PRODUCT_LV_1, MEASURE, LEVEL
        ,CASE WHEN FORECAST <> 0 AND REVISED_BUDGET <> 0 THEN (COALESCE(FORECAST,0) - COALESCE(REVISED_BUDGET,0)) / ABS(REVISED_BUDGET) END
FROM (  SELECT BIZLINE, YEARMONTH, TYPE, CUS_TYPE, SEGMENT, PRODUCT_LV_4, PRODUCT_LV_3, PRODUCT_LV_2, PRODUCT_LV_1, MEASURE, LEVEL, VALUE
        FROM {TBL_CALC}
        WHERE (YEARMONTH LIKE '2026%' OR YEARMONTH LIKE '2025%')
          AND TYPE IN ('REVISED_BUDGET','FORECAST')
     ) A
PIVOT (SUM(VALUE) FOR TYPE IN ('FORECAST' AS FORECAST, 'REVISED_BUDGET' AS REVISED_BUDGET))
""",
        ),
        (
            "E05_forecast_vs_forecast_lm",
            f"""
-- SP L2046-2072: CALCULATION 1.13 Forecast - Forecast LM && Forecast / Forecast LM
-- (CTE RAW của SP viết lại trong từng nhánh UNION ALL)
INSERT INTO {TBL_CALC}
SELECT BIZLINE, YEARMONTH
      ,'FORECAST - FORECAST_LM'
      ,CUS_TYPE, SEGMENT, PRODUCT_LV_4, PRODUCT_LV_3, PRODUCT_LV_2, PRODUCT_LV_1, MEASURE, LEVEL
      ,CASE WHEN FORECAST_LM <> 0 THEN COALESCE(FORECAST,0) - COALESCE(FORECAST_LM,0) END
FROM (  SELECT BIZLINE, YEARMONTH, TYPE, CUS_TYPE, SEGMENT, PRODUCT_LV_4, PRODUCT_LV_3, PRODUCT_LV_2, PRODUCT_LV_1, MEASURE, LEVEL, VALUE
        FROM {TBL_CALC}
        WHERE YEARMONTH LIKE '2026%' AND TYPE IN ('FORECAST','FORECAST_LM')   -- 20260423: YTD
     ) RAW
PIVOT (SUM(VALUE) FOR TYPE IN ('FORECAST' AS FORECAST, 'FORECAST_LM' AS FORECAST_LM))

UNION ALL

SELECT BIZLINE, YEARMONTH
      ,'FORECAST / FORECAST_LM'
      ,CUS_TYPE, SEGMENT, PRODUCT_LV_4, PRODUCT_LV_3, PRODUCT_LV_2, PRODUCT_LV_1, MEASURE, LEVEL
      ,CASE WHEN FORECAST_LM <> 0 THEN (COALESCE(FORECAST,0) - COALESCE(FORECAST_LM,0)) / ABS(FORECAST_LM) END
FROM (  SELECT BIZLINE, YEARMONTH, TYPE, CUS_TYPE, SEGMENT, PRODUCT_LV_4, PRODUCT_LV_3, PRODUCT_LV_2, PRODUCT_LV_1, MEASURE, LEVEL, VALUE
        FROM {TBL_CALC}
        WHERE YEARMONTH LIKE '2026%' AND TYPE IN ('FORECAST','FORECAST_LM')   -- 20260423: YTD
     ) RAW
PIVOT (SUM(VALUE) FOR TYPE IN ('FORECAST' AS FORECAST, 'FORECAST_LM' AS FORECAST_LM))
""",
        ),
    ]


def all_calc_blocks():
    """Gộp tất cả nhóm theo ĐÚNG thứ tự SP gốc."""
    return (
        _calc_blocks_a_leaf()
        + _calc_blocks_b_rollup()
        + _calc_blocks_c_supplementary()
        + _calc_blocks_d_fy_ratios()
        + _calc_blocks_e_variance()
    )

print(f"Nhóm E: {len(_calc_blocks_e_variance())} blocks | TOTAL: {len(all_calc_blocks())} blocks")
```

# Block 16: Run CALC Pipeline

```python
# ============================================================
# BLOCK 16 — RUN CALC (SP PART 1 - CALCULATION)
#
# Stage theo đúng thứ tự SP. Stage "parallel" chỉ gồm block KHÔNG đọc output của nhau:
#   A01-A08 : chỉ đọc CONSOL (+ mapping X)
#   C01-C05 : đọc measure gốc; ghi *_SUM / %REGISTER... / %UTILIZATION — không block nào trong nhóm đọc lại
#   D02-D16 : đọc measure gốc + *_SUM (đã có từ stage C); ghi %... / TICKET SIZE / NFI/PV
#   D17-D19 : chỉ đọc CONSOL — PHẢI chạy SAU D02-D16 (D11 %FEE đọc EOP BAL; D17/D18 ghi thêm EOP BAL)
#   E01-E05 : mỗi block lọc TYPE riêng, ghi TYPE mới
# Sequential: B (L1 → L2 → … → ALL đọc nối tiếp), D01 (FY đọc toàn CALC), D20 (YTD đọc toàn CALC), D21 (sau D20 như SP)
#
# Thêm block mới: nếu block đó đọc measure/level mà block khác CÙNG stage parallel ghi ra
#   → đưa vào stage sequential (hoặc stage riêng phía sau).
# ============================================================

def _blocks(group, first, last):
    """Các block từ `first` đến `last` (theo tên, giữ thứ tự) trong 1 nhóm."""
    names = [n for n, _ in group]
    return group[names.index(first): names.index(last) + 1]


A_BLOCKS = _calc_blocks_a_leaf()
B_BLOCKS = _calc_blocks_b_rollup()
C_BLOCKS = _calc_blocks_c_supplementary()
D_BLOCKS = _calc_blocks_d_fy_ratios()
E_BLOCKS = _calc_blocks_e_variance()

CALC_STAGES = [
    # (stage name,            mode,         blocks)
    ("A_leaf",                "parallel",   A_BLOCKS),
    ("B_rollup",              "sequential", B_BLOCKS),
    ("C_sum_and_ratio",       "parallel",   C_BLOCKS),
    ("D01_fy_mtd_ytd",        "sequential", _blocks(D_BLOCKS, "D01_fy_mtd_ytd", "D01_fy_mtd_ytd")),
    ("D02_D16_ratios",        "parallel",   _blocks(D_BLOCKS, "D02_pct_nim_ci_ftp_int_rev", "D16_pct_penetration_cd")),
    ("D17_D19_other_asset",   "parallel",   _blocks(D_BLOCKS, "D17_other_asset_eop_bal_l1.5", "D19_casa_ae_bal_w_other_asset")),
    ("D20_D21",               "sequential", _blocks(D_BLOCKS, "D20_ytd_bal_w_other_asset", "D21_txn_amt_per_cus_fx_flow")),
    ("E_variance",            "parallel",   E_BLOCKS),
]

# Kiểm tra: stages chứa đủ mọi block, mỗi block đúng 1 lần, đúng thứ tự SP
assert [n for _, _, blks in CALC_STAGES for n, _ in blks] == [n for n, _ in all_calc_blocks()], \
    "CALC_STAGES lệch với all_calc_blocks() — kiểm tra lại khi thêm/bớt block"

reset_calc_table()
execute_stages(CALC_STAGES)

# --- Debug ---
# show_block(all_calc_blocks(), "B01_L2_sumup")                   # in SQL 1 block
# execute_stages(CALC_STAGES, start_from_stage="D02_D16_ratios")
#   → Stage parallel lỗi: chưa merge vào CALC → chạy lại từ stage đó được (CALC_STAGE tự TRUNCATE).
#   → Stage sequential lỗi giữa chừng: các block trước trong stage đã ghi vào CALC
#     → reset_calc_table() rồi chạy lại từ đầu (chạy lại block đã ghi sẽ double).
# Chạy lần lượt toàn bộ để đối chiếu: đặt MAX_PARALLEL = 1 ở Block 1.
```

# Block 17: PFS VIEW — Output theo Template Excel (Dynamic PIVOT)

```python
# ============================================================
# BLOCK 17 — PART 2: PFS VIEW (SP L2095-2142) — Method 5
# - SP UPDATE trực tiếp FC_2026_Y (L2097-2099) → ở đây KHÔNG sửa bảng mapping, dùng temp view v_fc_2026_y
# - Dynamic PIVOT list [1]..[MAX(ID)] như SP (L2101-2111)
# - RIGHT JOIN X (giữ đủ rows template) + LEFT JOIN Y
# - CALC đã viết HOA → chỉ chuẩn hóa phía mapping: UPPER(RTRIM()) + bỏ non-breaking space char(160)
# ============================================================

# 1. FC_2026_Y: YEARMONTH '%YTD' / '%MTD' → LEFT(4) + RIGHT(@YTD_MONTH,2) + RIGHT(4)   (SP L2097-2099)
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW v_fc_2026_y AS
    SELECT * EXCEPT (YEARMONTH),
           CASE WHEN UPPER(RTRIM(CAST(YEARMONTH AS STRING))) LIKE '%YTD'
                  OR UPPER(RTRIM(CAST(YEARMONTH AS STRING))) LIKE '%MTD'
                THEN LEFT(CAST(YEARMONTH AS STRING),4) || RIGHT('{YTD_MONTH}',2) || RIGHT(CAST(YEARMONTH AS STRING),4)
                ELSE CAST(YEARMONTH AS STRING) END AS YEARMONTH
    FROM {TBL_MAPPING_Y}
""")

# 2. Dynamic PIVOT column list: 1..MAX(ID)   (SP L2101-2111)
max_id = spark.sql(f"SELECT MAX(CAST(ID AS BIGINT)) FROM {TBL_MAPPING_Y}").first()[0]
if max_id is None:
    raise ValueError(f"{TBL_MAPPING_Y} không có ID")
pivot_cols = ", ".join(str(i) for i in range(1, int(max_id) + 1))
print(f"PIVOT columns: 1..{max_id}")

# 3. PIVOT   (SP L2116-2142)
spark.sql(f"""
    CREATE OR REPLACE TABLE {TBL_PFS_VIEW} USING DELTA AS
    SELECT * FROM (
        SELECT CAST(X.ID AS BIGINT) AS ID_ROW
              ,CAST(Y.ID AS BIGINT) AS ID_COLUMN
              ,A.VALUE
        FROM {TBL_CALC} A
        RIGHT JOIN {TBL_MAPPING_X} X
                ON  A.PRODUCT_LV_4 = UPPER(RTRIM(REPLACE(CAST(X.PRODUCT_LV_4 AS STRING), char(160), ' ')))
                AND A.PRODUCT_LV_3 = UPPER(RTRIM(REPLACE(CAST(X.PRODUCT_LV_3 AS STRING), char(160), ' ')))
                AND A.PRODUCT_LV_2 = UPPER(RTRIM(REPLACE(CAST(X.PRODUCT_LV_2 AS STRING), char(160), ' ')))
                AND A.PRODUCT_LV_1 = UPPER(RTRIM(REPLACE(CAST(X.PRODUCT_LV_1 AS STRING), char(160), ' ')))
                AND A.LEVEL        = UPPER(RTRIM(REPLACE(CAST(X.LEVEL        AS STRING), char(160), ' ')))
                AND A.MEASURE      = UPPER(RTRIM(REPLACE(CAST(X.MEASURE      AS STRING), char(160), ' ')))
                AND A.BIZLINE      = UPPER(RTRIM(REPLACE(CAST(X.BIZLINE      AS STRING), char(160), ' ')))
        LEFT JOIN v_fc_2026_y Y
                ON  A.YEARMONTH    = UPPER(RTRIM(REPLACE(CAST(Y.YEARMONTH    AS STRING), char(160), ' ')))
                AND A.TYPE         = UPPER(RTRIM(REPLACE(CAST(Y.TYPE         AS STRING), char(160), ' ')))
                AND A.CUS_TYPE     = UPPER(RTRIM(REPLACE(CAST(Y.CUS_TYPE     AS STRING), char(160), ' ')))
                AND A.SEGMENT      = UPPER(RTRIM(REPLACE(CAST(Y.SEGMENT      AS STRING), char(160), ' ')))
    )
    PIVOT (SUM(VALUE) FOR ID_COLUMN IN ({pivot_cols}))
    ORDER BY ID_ROW ASC
""")

cnt = spark.table(TBL_PFS_VIEW).count()
print(f"=== PFS VIEW DONE: {TBL_PFS_VIEW} = {cnt:,} rows x {max_id} columns ===")
print("EXCEL CALCULATION DONE")

# Delta không đảm bảo thứ tự lưu → luôn ORDER BY khi đọc / export Excel
display(spark.sql(f"SELECT * FROM {TBL_PFS_VIEW} ORDER BY ID_ROW"))
```

# Block 18: Validation — Cross-check

```python
# ============================================================
# BLOCK 18 — VALIDATION (tham khảo SP L2163-2167 FORECAST_LOG)
# So sánh aggregate nguồn (CONSOL) vs output (CALC) bằng mắt
# ============================================================

# 1. TOI: CONSOL (cùng logic chọn measure của block A03) vs CALC L5 TOI tổng (BIZLINE ALL, CUS_TYPE/SEGMENT ' ALL')
print("1. TOI cross-check (DIFF kỳ vọng ≈ 0):")
display(spark.sql(f"""
    WITH src AS (
        SELECT TYPE, LEFT(`MONTH`,4) AS YEAR, SUM(VALUE) AS SRC_TOI
        FROM {TBL_CONSOL}
        WHERE CASE WHEN GROUP_MEASURE IN ('PAIDOFF','NII','NFI') THEN GROUP_MEASURE ELSE MEASURE END
                  IN ('NII','NFI','TRADING INCOME','OTHER INCOME')
          AND PRODUCT_LINE <> 'OTHER ASSET'
        GROUP BY TYPE, LEFT(`MONTH`,4)
    ),
    out AS (
        SELECT TYPE, LEFT(YEARMONTH,4) AS YEAR, SUM(VALUE) AS CALC_TOI
        FROM {TBL_CALC}
        WHERE LEVEL = 'L5' AND MEASURE = 'TOI'
          AND BIZLINE = 'ALL' AND CUS_TYPE = ' ALL' AND SEGMENT = ' ALL'
          AND LENGTH(YEARMONTH) = 6            -- chỉ tháng, loại FY/MTD/YTD tránh double-count
        GROUP BY TYPE, LEFT(YEARMONTH,4)
    )
    SELECT COALESCE(s.TYPE, o.TYPE) AS TYPE
          ,COALESCE(s.YEAR, o.YEAR) AS YEAR
          ,s.SRC_TOI
          ,o.CALC_TOI
          ,ROUND(o.CALC_TOI - s.SRC_TOI, 2) AS DIFF
    FROM src s
    FULL OUTER JOIN out o ON s.TYPE = o.TYPE AND s.YEAR = o.YEAR
    ORDER BY 1, 2
"""))

# 2. Phân bố CALC theo TYPE / LEVEL
print("2. CALC rows by TYPE / LEVEL:")
display(spark.sql(f"""
    SELECT TYPE, LEVEL, COUNT(*) AS ROWS, COUNT(VALUE) AS NON_NULL_ROWS
    FROM {TBL_CALC}
    GROUP BY TYPE, LEVEL
    ORDER BY TYPE, LEVEL
"""))

# 3. Data quality: giá trị chỉ khác nhau về HOA/thường hoặc trailing space trong UnionAll
#    (SQL Server coi là 1 giá trị; CALC đã gộp nhờ UPPER(RTRIM()) ở CONSOL — ở đây chỉ để biết)
print("3. Case/trailing-space variants trong Forecast_2026_UnionAll (VARIANTS > 0 = có biến thể):")
display(spark.sql(f"""
    SELECT COL, RAW_DISTINCT, CI_DISTINCT, RAW_DISTINCT - CI_DISTINCT AS VARIANTS
    FROM (
        SELECT 'BIZLINE'        AS COL, COUNT(DISTINCT BIZLINE)        AS RAW_DISTINCT, COUNT(DISTINCT UPPER(RTRIM(BIZLINE)))        AS CI_DISTINCT FROM {TBL_FORECAST_2026}
        UNION ALL
        SELECT 'CUS_TYPE'       AS COL, COUNT(DISTINCT CUS_TYPE)       AS RAW_DISTINCT, COUNT(DISTINCT UPPER(RTRIM(CUS_TYPE)))       AS CI_DISTINCT FROM {TBL_FORECAST_2026}
        UNION ALL
        SELECT 'SEGMENT'        AS COL, COUNT(DISTINCT SEGMENT)        AS RAW_DISTINCT, COUNT(DISTINCT UPPER(RTRIM(SEGMENT)))        AS CI_DISTINCT FROM {TBL_FORECAST_2026}
        UNION ALL
        SELECT 'CHANNEL'        AS COL, COUNT(DISTINCT CHANNEL)        AS RAW_DISTINCT, COUNT(DISTINCT UPPER(RTRIM(CHANNEL)))        AS CI_DISTINCT FROM {TBL_FORECAST_2026}
        UNION ALL
        SELECT 'PRODUCT_FAMILY' AS COL, COUNT(DISTINCT PRODUCT_FAMILY) AS RAW_DISTINCT, COUNT(DISTINCT UPPER(RTRIM(PRODUCT_FAMILY))) AS CI_DISTINCT FROM {TBL_FORECAST_2026}
        UNION ALL
        SELECT 'PRODUCT_LINE'   AS COL, COUNT(DISTINCT PRODUCT_LINE)   AS RAW_DISTINCT, COUNT(DISTINCT UPPER(RTRIM(PRODUCT_LINE)))   AS CI_DISTINCT FROM {TBL_FORECAST_2026}
        UNION ALL
        SELECT 'SUB_PRODUCT'    AS COL, COUNT(DISTINCT SUB_PRODUCT)    AS RAW_DISTINCT, COUNT(DISTINCT UPPER(RTRIM(SUB_PRODUCT)))    AS CI_DISTINCT FROM {TBL_FORECAST_2026}
        UNION ALL
        SELECT 'GROUP_MEASURE'  AS COL, COUNT(DISTINCT GROUP_MEASURE)  AS RAW_DISTINCT, COUNT(DISTINCT UPPER(RTRIM(GROUP_MEASURE)))  AS CI_DISTINCT FROM {TBL_FORECAST_2026}
        UNION ALL
        SELECT 'MEASURE'        AS COL, COUNT(DISTINCT MEASURE)        AS RAW_DISTINCT, COUNT(DISTINCT UPPER(RTRIM(MEASURE)))        AS CI_DISTINCT FROM {TBL_FORECAST_2026}
        UNION ALL
        SELECT 'SUB_MEASURE'    AS COL, COUNT(DISTINCT SUB_MEASURE)    AS RAW_DISTINCT, COUNT(DISTINCT UPPER(RTRIM(SUB_MEASURE)))    AS CI_DISTINCT FROM {TBL_FORECAST_2026}
        UNION ALL
        SELECT 'SUB_MEASURE_2'  AS COL, COUNT(DISTINCT SUB_MEASURE_2)  AS RAW_DISTINCT, COUNT(DISTINCT UPPER(RTRIM(SUB_MEASURE_2)))  AS CI_DISTINCT FROM {TBL_FORECAST_2026}
    )
    ORDER BY VARIANTS DESC
"""))
```

---

## Ghi chú migration (khác biệt so với SP / draft cũ — cần review)

| # | Vị trí | Nội dung | Hành động |
|---|---|---|---|
| 1 | Block 7 | Draft cũ gộp 32 UPDATE vào 1 `CASE` → sai thứ tự (L156 Prix, L230 TXN Amt, L234 Ticket size phụ thuộc UPDATE trước), thiếu L133, L262, `SUB_MEASURE_2` của L165, `GROUP_MEASURE` của L178, thừa `SUB_MEASURE_2` ở L209 | Viết lại bằng CTE chain đúng thứ tự |
| 2 | Block 7 / 9 | SP L200 update `SEGMENT` của **BUDGET**_2026_UNIONALL (không phải Forecast). Draft cũ áp cho Forecast | Chuyển sang CONSOL step `s01` cho TYPE='BUDGET' |
| 3 | Block 6 | SP L86 viết `LIKE '% BANCA%'` (có space) → INFO_2 có thể luôn = `''` trong SP | Giữ `PAYMENT_MODE` theo draft — đổi `"''"` nếu cần khớp 100% |
| 4 | Block 6 | SP L83 `'%SME_MSME%'` lấy `INFO_1` — bảng Databricks tên `..._sme` không match | Kiểm tra lại nếu bảng SME cần INFO_1 |
| 5 | Block 6 | RHE: SP lấy `INFO_2` thật và `SOURCE_TBL = 'RHE'` | Đã xử lý riêng qua `RHE_LABEL` |
| 6 | Block 14 | `365 / C.NO_DAY`: nếu `MAPPING_DAYS.NO_DAY` trên SQL Server là INT thì SQL Server chia nguyên (365/31 = 11), Spark luôn ra DOUBLE (11.77) | **Kiểm tra kiểu cột NO_DAY** trên SQL Server khi đối chiếu %NIM/%CI |
| 7 | Block 9+ | Dimension trong CALC / CONSOL là chữ HOA (do chuẩn hóa CI) | Downstream đọc CALC cần so sánh case-insensitive |
| 8 | Block 9 | `DATE_BUDGET = None` → nếu bảng budget có `DS_PARTITION_DATE` sẽ lấy MAX | Kiểm tra log snapshot ở Block 9 |
| 9 | Block 10 / 16 | CALC chạy theo stage; stage parallel ghi qua bảng `CALC_STAGE` rồi merge 1 lần. Không có trong SP | Lần đầu nên chạy 1 lượt `MAX_PARALLEL = 1` và 1 lượt `= 8`, so sánh CALC bằng `EXCEPT ALL` để xác nhận kết quả giống nhau |
| 10 | Block 2 | `inserted_rows()` đọc `num_inserted_rows` từ kết quả INSERT. Nếu runtime không trả cột này → log hiện `n/a` (không lỗi) | Kiểm tra log lần chạy đầu |
