# Phương pháp migrate stored procedure SQL Server sang Databricks notebook (Tổng quát)

## Tổng quan

Phương pháp migrate stored procedure SQL Server sang Databricks notebook. Áp dụng cho **Budget, Forecast**, và các SP cùng pattern. Gồm **5 code methods**, mỗi method giải quyết 1 bài toán cụ thể trong quá trình migration.

### Nguyên tắc migration chung

- **Giữ nguyên nội dung SQL gốc** — chỉ thay syntax differences, không refactor logic

- **Mỗi block trong SP = 1 named tuple trong Python** — trace 1:1 khi debug

- **Python chỉ làm orchestrator** — toàn bộ business logic nằm trong SQL string

- **Tách thành functions theo nhóm** — dễ review, dễ tái sử dụng giữa Budget/Forecast

``` text
N source tables (schema khác nhau)
│
▼
METHOD 1: Dynamic Union ──────── Chuẩn hóa schema → 1 bảng thống nhất
│
▼
METHOD 2: Business Rule Normalize ─ Chuỗi CASE-WHEN reclassify dimensions
│
▼
METHOD 3: Consolidate + Stage ──── UNION BY NAME nhiều nguồn → temp views
│
▼
METHOD 4: SQL-Preserving Migration ─ Named SQL blocks + Python executor
│
▼
METHOD 5: Pivot Output ─────────── Mapping tables + dynamic PIVOT → matrix
│
▼
VALIDATION: Cross-check ────────── So sánh source vs output aggregates
```

------------------------------------------------------------------------

## Method 1: Dynamic Union (Schema-Heterogeneous Sources)

### Bài toán

N source tables cùng domain nhưng **schema khác nhau** (có/không có một số cột optional). Cần gộp thành 1 bảng chuẩn hóa.

### Pattern

``` python
# A. Normalize column names → uppercase
for table in SOURCE_TABLES:
raw = normalize_columns(spark.table(table)) # upper + strip + replace space
raw.createOrReplaceTempView(f"_clean_{table_name}")
# Lưu lại danh sách columns thực tế
CLEAN_VIEW_COLS[table] = [c.upper() for c in raw.columns]

# B. Dynamic column substitution — detect column tồn tại
col_x = "REAL_COL" if "REAL_COL" in col_names else "'default_value'"

# C. Generate SELECT cho mỗi table
select_sql = f"""
SELECT CAST(col_a AS STRING) AS COL_A,
CAST({col_x} AS STRING) AS COL_X, -- dynamic
TRY_CAST(VALUE AS DOUBLE) AS VALUE,
'{table_name}' AS SOURCE_TBL
FROM {clean_view}
WHERE TRY_CAST(VALUE AS DOUBLE) <> 0
AND MONTH BETWEEN '{MONTH_FROM}' AND '{MONTH_TO}'
"""

# D. INSERT từng table (không UNION ALL 1 lần) → dễ debug
for sel in select_clauses:
spark.sql(f"INSERT INTO {target_table}\n{sel}")
```

### Nguyên tắc

- **Column substitution table**: Định nghĩa mapping {column → fallback} và {column → alias_by_table_name} (ví dụ: table chứa keyword "SME" thì dùng cột khác cho CUSTOMER_TIER)

- **TRY_CAST** cho VALUE — tránh lỗi khi source có string

- **INSERT từng table** thay vì 1 UNION ALL lớn — khi 1 table lỗi schema, các table khác vẫn chạy

- **SOURCE_TBL column** — ghi lại nguồn gốc để trace ngược

## Method 2: Business Rule Normalization (Chained CASE-WHEN)

### Bài toán

Dữ liệu raw có naming không nhất quán giữa các source. Cần reclassify/rename các dimension values theo business rules trước khi tính toán.

### Pattern

**PySpark version** — chuỗi withColumn tuần tự:

``` python
def normalize(df):
out = df
# Rule 1: Reclassify dựa trên tổ hợp nhiều columns
condition = ci_equals("COL_A", "val_1") & ci_in("COL_B", ["x", "y"])
out = out.withColumn("TARGET_COL",
F.when(condition, F.lit("new_value")).otherwise(F.col("TARGET_COL")))

# Rule 2, 3, ... (tuần tự, rule sau có thể đọc kết quả rule trước)
...

# Filter cuối: loại bỏ measures đã tính sẵn (derived measures)
out = out.filter(~ci_in("MEASURE", EXCLUDED_MEASURES_LIST))
return out.cache()
```

**SQL version** — chuỗi CTE s0 → s1 → ... → sN:

``` sql
WITH s0 AS (SELECT * FROM source),
s1 AS (
SELECT ...,
CASE WHEN condition THEN 'new_value' ELSE original END AS TARGET_COL
FROM s0
),
s2 AS (
SELECT ...,
CASE WHEN condition_using_s1_result THEN ... END AS TARGET_COL
FROM s1
),
...
SELECT * FROM sN WHERE MEASURE NOT IN (excluded_list)
```

### Nguyên tắc

- **Thứ tự rules quan trọng** — rule sau có thể phụ thuộc kết quả rule trước (ví dụ: rename MEASURE trước, rồi mới filter theo MEASURE mới)

- **Case-insensitive matching bắt buộc** — dùng UPPER(COALESCE(col, '')) ở mọi comparison (legacy SQL Server CI collation)

- **Mỗi step chỉ sửa 1-2 columns** — giữ các column khác nguyên, dễ debug

- **Filter derived measures cuối cùng** — loại các measures sẽ được tính lại ở CALC (tỷ lệ %, avg/cus, etc.)

- **SQL version nhanh hơn PySpark** khi logic chỉ là CASE-WHEN — tránh overhead Python ↔ JVM

------------------------------------------------------------------------

## Method 3: Multi-Source Consolidation + Staging

### Bài toán

Gộp nhiều nguồn dữ liệu (bottom-up budget, top-down targets, ...) thành 1 dataset thống nhất, tạo temp views cho downstream.

### Pattern

``` python
def consolidate(source_a_df, source_b_df):
# 1. Align schema: rename columns, add TYPE discriminator
a = source_a_df.select(..., F.lit("TYPE_A").alias("TYPE"))
b = source_b_df.select(..., F.lit("TYPE_B").alias("TYPE"))

# 2. UNION BY NAME (cho phép missing columns)
consol = a.unionByName(b, allowMissingColumns=True)

# 3. Post-union normalization (rules chỉ áp dụng sau khi gộp)
consol = consol.withColumn("COL",
F.when(condition_across_types, new_value).otherwise(F.col("COL")))

return consol.cache()

def persist_stage_tables():
# Build từng source
source_a = build_and_normalize_source_a()
source_b = build_source_b()

# Consolidate
consol = consolidate(source_a, source_b)

# Register temp views cho SQL downstream
source_a.createOrReplaceTempView("view_a")
source_b.createOrReplaceTempView("view_b")
consol.createOrReplaceTempView("consol_v")

return source_a, source_b, consol
```

### Nguyên tắc

- **unionByName + allowMissingColumns=True** — an toàn khi 2 sources có schema hơi khác

- **TYPE discriminator** — phân biệt nguồn gốc dữ liệu trong cùng 1 table

- **Post-union rules** — một số normalization chỉ có ý nghĩa sau khi gộp (ví dụ: chuẩn hóa tên measure giữa 2 nguồn)

- **cache()** trên consolidated view — sẽ được đọc nhiều lần bởi CALC blocks

- **Partition filter trên source** — nếu source có partition date, filter trước khi union

------------------------------------------------------------------------

## Method 4: SQL-Preserving Migration (SQL Server → Databricks)

### Bài toán

Stored procedure SQL Server chứa hàng chục câu INSERT/UPDATE/DELETE tuần tự. Cần migrate sang Databricks notebook sao cho:

1.  **Giữ nguyên nội dung SQL gốc** — dễ đối chiếu 1:1 với stored procedure khi debug

2.  **Track được từng block** — biết block nào lỗi, block nào cho kết quả sai

3.  **Tái sử dụng cho Forecast** và các stored procedure khác cùng pattern

### Coding Style: "Named SQL Blocks as Python List"

**Ý tưởng cốt lõi**: Mỗi câu SQL gốc trong stored procedure được giữ nguyên nội dung, chỉ wrap thành 1 tuple (name, sql_text) trong Python. Toàn bộ logic nằm trong SQL string — Python chỉ làm orchestrator.

``` python
# ---- ĐỊNH NGHĨA: Mỗi function trả về list of (name, sql) ----

def _calc_blocks_group_a():
"""Nhóm A: tương ứng lines XX-YY trong stored procedure gốc."""
return [
(
"block_name_matching_sp_comment", # Tên = comment/label trong SP gốc
f"""
-- Giữ nguyên SQL gốc, chỉ thay:
-- ANALYSIS..TABLE_NAME → {fq(catalog, schema, table)}
-- @VARIABLE → '{python_variable}'
-- temp view name → Spark temp view name
INSERT INTO {calc_table}
SELECT
COL_A,
COL_B,
CASE WHEN condition THEN 'value' ELSE COL_C END AS COL_C,
'L1' AS LEVEL,
SUM(VALUE) AS VALUE
FROM {source_view}
WHERE filter_conditions
GROUP BY COL_A, COL_B,
CASE WHEN condition THEN 'value' ELSE COL_C END
""",
),
(
"next_block_name",
f"""...""",
),
]

def _calc_blocks_group_b():
"""Nhóm B: tương ứng lines YY-ZZ."""
return [...]

# ---- COMBINER: Gộp tất cả nhóm theo đúng thứ tự SP gốc ----

def all_calc_blocks():
return (
_calc_blocks_group_a()
+ _calc_blocks_group_b()
+ _calc_blocks_group_c()
+ _calc_blocks_group_d()
)
```

### Executor: Chạy tuần tự + logging

``` python
def execute_blocks(blocks, calc_table):
for idx, (name, sql_text) in enumerate(blocks, 1):
start = time.time()
spark.sql(sql_text)
elapsed = time.time() - start

# Log row count sau INSERT để track
if "insert" in sql_text.lower():
cnt = spark.sql(f"SELECT count(*) FROM {calc_table}").first()[0]
print(f" [{idx:2d}/{len(blocks)}] {name:40s} ({elapsed:5.1f}s) → {cnt:,} rows")
else:
print(f" [{idx:2d}/{len(blocks)}] {name:40s} ({elapsed:5.1f}s)")

# Chạy pipeline
blocks = all_calc_blocks()
execute_blocks(blocks, calc_table)
```

### Quy tắc translate SQL Server → Databricks SQL

| **SQL Server** | **Databricks SQL** | **Ghi chú** |
|----|----|----|
| ANALYSIS..TABLE_NAME | {fq(catalog, schema, table)} | Dùng f-string + helper function |
| @VARIABLE | '{python_var}' | Inject qua f-string |
| \#temp_table | Temp view hoặc Delta table | createOrReplaceTempView |
| UPDATE ... SET | Rewrite thành withColumn hoặc CTE + INSERT | Databricks SQL không hỗ trợ UPDATE trên view |
| DELETE FROM ... WHERE | Rewrite thành filter ngược | INSERT INTO ... SELECT ... WHERE NOT (delete_condition) |
| EXEC(@dynamic_sql) | Build SQL string trong Python + spark.sql() | Ví dụ: dynamic PIVOT column list |
| LEFT(col, N) | substr(col, 1, N) |  |
| RIGHT(col, N) | right(col, N) | Databricks hỗ trợ right() |
| ISNULL(col, default) | COALESCE(col, default) |  |
| CONVERT(type, col) | CAST(col AS type) hoặc TRY_CAST | TRY_CAST an toàn hơn |
| CI collation (default) | upper(trim(col)) ở mọi comparison | SQL Server mặc định case-insensitive |
| SET @LIST = ... + EXEC | Python collect → f-string inject | Ví dụ: PIVOT column list |

### Tại sao style này phù hợp cho migration

| **Lợi ích** | **Giải thích** |
|----|----|
| **1**:1 **traceability** | Mỗi (name, sql) tương ứng 1 block trong SP gốc. Khi kết quả sai, so sánh SQL string với SP line-by-line |
| **Minimal rewrite** | SQL gốc giữ nguyên logic, chỉ thay table references và syntax differences. Không cần hiểu toàn bộ business logic để migrate |
| **Debug từng block** | Executor log tên + thời gian + row count. Block lỗi → biết ngay tên, chạy lại riêng block đó |
| **Tái sử dụng cho Forecast** | Cùng pattern: đọc SP Forecast → tách thành named blocks → thay table references → chạy |
| **Review dễ** | Business team có thể đọc SQL string trực tiếp, không cần biết Python/PySpark |

### 4 nhóm blocks điển hình (áp dụng cho cả Budget và Forecast)

### Nhóm A: Leaf Insert — từ consolidated view vào CALC table

- Mỗi block = 1 INSERT INTO ... SELECT ... GROUP BY

- CASE-WHEN trong SELECT map source dimensions → target hierarchy

- Nhiều blocks cho các nhóm measures khác nhau

### Nhóm B: Hierarchical Rollup — self-read từ CALC table

- Pattern: collapse 1 dimension level thành 'ALL', SUM từ level thấp hơn

- Chạy từ level thấp lên cao (L1 → L2 → ... → L5)

- Composite measures (TOI, PBT, AUM) tại level cao nhất

### Nhóm C: Supplementary + FY Rollup

- \_SUM measures: duplicate cho ratio calculations

- FY Rollup: aggregate monthly → annual, phân biệt 3 loại:

- - **Flow** (income, cost): SUM 12 tháng

  <!-- -->

  - **Stock** (balance, customer count): chỉ lấy tháng 12

  <!-- -->

  - **Average**: SUM / 12

### Nhóm D: Ratios + Variance

- **Annualized rate**: (A / B) × (365 / days_in_period) — self-join CALC table

- **Simple ratio**: A / B, có thể cap tại 1 (100%)

- **YoY Variance**: CTE pivot 2 năm → tính absolute diff và % change

------------------------------------------------------------------------

## Method 5: Pivot Output (Mapping Tables + Dynamic PIVOT)

### Bài toán

Chuyển CALC table (long format) thành matrix (wide format) cho Excel template, dùng 2 mapping tables định nghĩa row/column IDs.

### Pattern

``` python
def build_pivot_output():
# 1. Đọc column mapping → build dynamic PIVOT list
col_ids = [row.ID for row in spark.sql(f"SELECT ID FROM {y_mapping}").collect()]
pivot_cols = ", ".join(str(c) for c in sorted(col_ids))

# 2. PIVOT query
spark.sql(f"""
CREATE TABLE {output_table} AS
SELECT * FROM (
SELECT
X.ID AS ID_ROW,
CAST(Y.ID AS BIGINT) AS ID_COLUMN,
A.VALUE
FROM {calc_table} A
RIGHT JOIN {x_mapping} X -- row mapping (product/measure → row ID)
ON upper(trim(A.dim_1)) = upper(trim(X.dim_1))
AND upper(trim(A.dim_2)) = upper(trim(X.dim_2))
...
AND (upper(trim(X.wildcard_dim)) = 'ALL'
OR upper(trim(A.wildcard_dim)) = upper(trim(X.wildcard_dim)))
LEFT JOIN {y_mapping} Y -- column mapping (time/type → col ID)
ON upper(trim(A.time_dim)) = upper(trim(Y.time_dim))
AND (upper(trim(Y.filter_dim)) = 'ALL'
OR upper(trim(A.filter_dim)) = upper(trim(Y.filter_dim)))
)
PIVOT (SUM(VALUE) FOR ID_COLUMN IN ({pivot_cols}))
ORDER BY ID_ROW
""")
```

### Nguyên tắc

- **RIGHT JOIN X**: Giữ tất cả rows từ mapping (kể cả không có data) → Excel template cần đủ rows

- **LEFT JOIN Y**: Chỉ lấy columns có trong mapping

- **Wildcard dimension** ('ALL'): Mapping row/column có thể match tất cả giá trị của 1 dimension → dùng OR condition

- **upper(trim(...))** trên mọi join key — case-insensitive

- **replace(char(160), ' ')** — handle non-breaking spaces trong text data

- **Dynamic PIVOT list** — build tại runtime từ mapping table, không hardcode

------------------------------------------------------------------------

## Validation Pattern

``` python
def cross_check():
# So sánh aggregate từ source vs output
source_total = spark.sql("""
SELECT substr(time_col, 1, 4) AS YEAR, SUM(VALUE)
FROM source_table
WHERE dimension IN (component_list)
GROUP BY 1
""")
output_total = spark.sql("""
SELECT substr(time_col, 1, 4) AS YEAR, SUM(VALUE)
FROM calc_table
WHERE LEVEL = 'top_level' AND MEASURE IN (component_list)
AND time_col NOT LIKE '%FY%' -- loại FY rows tránh double-count
GROUP BY 1
""")
# Display cả 2 để so sánh bằng mắt
```

------------------------------------------------------------------------

## Helper Functions (Tái sử dụng)

``` python
# Case-insensitive comparison (bắt buộc khi migrate từ SQL Server CI collation)
def ci_equals(col_name, value):
return F.upper(F.coalesce(F.col(col_name).cast("string"), F.lit(""))) == F.lit(value.upper())

def ci_in(col_name, values):
return F.upper(F.coalesce(F.col(col_name).cast("string"), F.lit(""))).isin([v.upper() for v in values])

# Normalize column names
def normalize_columns(df):
return df.toDF(*[c.upper().strip().replace(" ", "_") for c in df.columns])

# Safe column read
def txt(df, col_name, default=""):
if col_name.upper() in df.columns:
return F.coalesce(F.trim(F.col(col_name).cast("string")), F.lit(default))
return F.lit(default)

def dbl(df, col_name):
if col_name.upper() in df.columns:
return F.regexp_replace(F.trim(F.col(col_name).cast("string")), ",", "").cast("double")
return F.lit(0.0).cast("double")

# Fully qualified table name
def fq(catalog, schema, table):
return f"`{catalog}`.`{schema}`.`{table}`"
```

------------------------------------------------------------------------

## Constraints khi tối ưu

| **\#** | **Constraint** | **Lý do** |
|----|----|----|
| 1 | **Thứ tự CALC blocks phải giữ nguyên** | Block sau đọc kết quả block trước (self-referencing INSERT) |
| 2 | **Case-insensitive matching ở mọi join/filter** | Legacy SQL Server CI collation |
| 3 | **Product hierarchy mapping phải consistent** | Cùng CASE-WHEN logic giữa leaf insert và rollup |
| 4 | **FY rollup phân biệt 3 loại measure** | Flow (SUM), Stock (tháng 12), Average (SUM/12) |
| 5 | **PFS VIEW: RIGHT JOIN X + LEFT JOIN Y** | Không đổi thứ tự — X quyết định rows, Y quyết định columns |
| 6 | **Variance blocks chạy sau FY rollup** | Cần FY rows tồn tại trước |
| 7 | **\_SUM measures trước ratio blocks** | Ratio dùng \_SUM làm numerator/denominator |
| 8 | **Filter VALUE ≠ 0 ở source, không ở CALC** | CALC có thể có VALUE = 0 hợp lệ (ratio = 0) |
| 9 | **Handle non-breaking space char(160)** | Trong MEASURE join của PFS VIEW |
| 10 | **Partition filter trên source tables** | Hardcoded → nên parameterize |

## Cơ hội tối ưu

| **Cơ hội** | **Mô tả** | **Impact** |
|----|----|----|
| **Batch INSERT** | Gộp các L1 blocks cùng source thành 1 query UNION ALL | Giảm số lần scan source |
| **Bỏ sequential COUNT** | SELECT count(\*) sau mỗi INSERT chỉ dùng khi debug | Giảm ~40 queries |
| **SQL-only normalize** | Dùng CTE chain thay PySpark withColumn chain | Giảm Python↔JVM overhead |
| **Cache CALC intermediate** | CALC table đọc lại nhiều lần → cache sau mỗi nhóm blocks | Giảm re-scan Delta |
| **Parallelize ratio blocks** | Các ratio blocks độc lập có thể chạy song song | Giảm wall time |
| **Parameterize year/dates** | Thay hardcoded '2027', '2026' bằng variables | Tái sử dụng cho năm sau |

