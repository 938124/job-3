CREATE OR REPLACE VIEW ${silver_catalog}.atlas_stage.BPC_Plug_Financials AS
SELECT
  `BPC Plug Beginning Month Date`          AS bpc_plug_beginning_month_date,
  `BPC Plug Gross Revenue (less Freight)`  AS bpc_plug_gross_revenue_less_freight,
  `BPC Plug Discount`                      AS bpc_plug_discount,
  `BPC Plug Net Revenue`                   AS bpc_plug_net_revenue,
  `BPC Plug Cost`                          AS bpc_plug_cost,
  `BPC Plug Profit`                        AS bpc_plug_profit,
  `PC Region`                              AS pc_region
FROM ${bronze_catalog}.atlas.BPC_Plug_Financials
WHERE IS_DELETED = false;


CREATE OR REPLACE VIEW ${silver_catalog}.atlas_stage.BPC_Plug_Financials_MachineCosts AS
SELECT
  `Unnamed: 1`   AS unnamed_1,
  `Jan`          AS jan,
  `Feb`          AS feb,
  `Mar`          AS mar,
  `Apr`          AS apr,
  `May`          AS may,
  `Jun`          AS jun,
  `Jul`          AS jul,
  `Aug`          AS aug,
  `Sep`          AS sep,
  `Oct`          AS oct,
  `Nov`          AS nov,
  `Dec`          AS dec
FROM ${bronze_catalog}.atlas.BPC_Plug_Financials_MachineCosts
WHERE IS_DELETED = false;


CREATE OR REPLACE VIEW ${silver_catalog}.atlas_stage.BW_COPA_DailyFeePoker_Units AS
SELECT
  `Sales Org`             AS sales_org,
  `Juris_Num`             AS juris_num,
  `Profit Center Numeric` AS profit_ctr,
  `Sold To Cust`          AS sold_to_cust,
  `Install At`            AS install_at,
  `Doc Type`              AS doc_type,
  `Doc Number`            AS doc_number,
  `Line Item`             AS line_item,
  `Marterial Num`         AS material_num,
  `Serial Num`            AS serial_num,
  `Theme ID`              AS theme_id,
  `NO of Billing Days`    AS no_of_billing_days,
  `EOM Installed Base`    AS eom_installed_base,
  `Atlas Month`           AS atlas_month
FROM ${bronze_catalog}.atlas.BW_COPA_DailyFeePoker_Units
WHERE IS_DELETED = false;


CREATE OR REPLACE VIEW ${silver_catalog}.atlas_stage.BW_ZRBINQ_BillingPlans AS
SELECT
  `DOC TYPE`           AS doc_type,
  `SERIAL NUM`         AS serial_num,
  `DOC NUMBER`         AS doc_number,
  `LINE ITEM`          AS line_item,
  `SOLD TO CUST`       AS sold_to_cust,
  `SOLD TO NAME`       AS sold_to_name,
  `INSTALL AT`         AS install_at,
  `INSTALL AT NAME`    AS install_at_name,
  `PROFIT CTR`         AS profit_ctr,
  `PROFIT CTR DESC`    AS profit_ctr_desc,
  `MATERIAL NUM`       AS material_num,
  `THEME`              AS theme,
  `THEME ID`           AS theme_id,
  `NO OF BILLING DAYS` AS no_of_billing_days,
  `SHORT TEXT`         AS short_text,
  `TARGET QTY`         AS target_qty,
  `BILL STATUS`        AS bill_status,
  `FROM DATE`          AS from_date,
  `TO DATE`            AS to_date,
  `BILLING DATE`       AS billing_date,
  `NET VALUE`          AS net_value,
  `NET PRICE`          AS net_price,
  `GO-LIVE`            AS go_live,
  `SHUT DOWN`          AS shut_down,
  `JURIS`              AS juris,
  `ACCT EXEC NAME`     AS acct_exec_name,
  `REF MATERIAL NUM`   AS ref_material_num,
  `CUSTOMER GROUP`     AS customer_group,
  `JURIS_NUM`          AS juris_num,
  `SALES GROUP`        AS sales_group,
  `SALES OFFICE`       AS sales_office,
  `SALES ORG`          AS sales_org,
  `ACCT EXEC`          AS acct_exec,
  `ADD VALUE DAYS`     AS add_value_days,
  `BILL PLAN`          AS bill_plan,
  `BRAND`              AS brand,
  `CALENDAR`           AS calendar,
  `DENOM`              AS denom,
  `FISCAL_DATE`        AS fiscal_date,
  `INSTALL`            AS install,
  `PERIOD LEN`         AS period_len,
  `REJECTION`          AS rejection,
  `REMOVE`             AS remove,
  `SYSTEM`             AS system,
  `EQUIPMENT NUM`      AS equipment_num,
  `CURRENCY`           AS currency,
  `ATLAS TYPE`         AS atlas_type,
  `UPLOAD MONTH DATE`  AS upload_month_date
FROM ${bronze_catalog}.atlas.BW_ZRBINQ_BillingPlans
WHERE IS_DELETED = false;


CREATE OR REPLACE VIEW ${silver_catalog}.atlas_stage.GameOpsInstallBaseActivity AS
SELECT
  `Fiscal Week`     AS date_to,
  `Product Type`    AS bingo_non_premium,
  `Serial Number`   AS serial_num,
  `Customer Number` AS customer_number,
  `System Code`     AS system_code,
  `Brand Code`      AS brand_code,
  `Theme Code`      AS theme_code_updated,
  `Pricing`         AS unit_type,
  `Activity`        AS activity,
  `STL`             AS stl_flag,
  `Material`        AS material_updated,
  `Profit Center`   AS profit_center,
  `Install Date`    AS install_date,
  `Go Live Date`    AS go_live_date,
  `Shut Down Date`  AS shutdown_date,
  `Removal Date`    AS removal_date
FROM ${bronze_catalog}.atlas.GameOpsInstallBaseActivity
WHERE IS_DELETED = false;


CREATE OR REPLACE VIEW ${silver_catalog}.atlas_stage.GameOpsInstallBasePSD AS
SELECT
  `Fiscal Week`     AS date_to,
  `Product Type`    AS product_type,
  `Serial Number`   AS serial_num,
  `Customer Number` AS customer_number,
  `System Code`     AS system_code,
  `Brand Code`      AS brand_code,
  `Theme Code`      AS theme_code_fix,
  `Pricing`         AS unit_type,
  `Material`        AS material_updated,
  `STL`             AS stl_flag_cosmo,
  `Profit Center`   AS profit_center,
  `Install Date`    AS install_date,
  `Go Live Date`    AS go_live_date
FROM ${bronze_catalog}.atlas.GameOpsInstallBasePSD
WHERE IS_DELETED = false;


CREATE OR REPLACE VIEW ${silver_catalog}.atlas_stage.RPM_Exp_Depreciation AS
SELECT
  `Co. Code`            AS co_code,
  `Asset`               AS asset,
  `Sub`                 AS sub,
  `Class`               AS class,
  `Life`                AS life,
  `SerialNum`           AS serialnum,
  `Material`            AS material,
  `Asset Description`   AS asset_description,
  `Group`               AS group,
  `Group 2`             AS group_2,
  `NVGLAcct`            AS nvglacct,
  `NVADGLAcct`          AS nvadglacct,
  `NVDeprExpGL`         AS nvdeprexpgl,
  `CFINGLAcct`          AS cfinglacct,
  `CFINADGLAcct`        AS cfinadglacct,
  `CFIN FS ROLLUP`      AS cfin_fs_rollup,
  `Inst At`             AS inst_at,
  `Status`              AS status,
  `Cost_Ctr`            AS cost_ctr,
  `Func Area`           AS func_area,
  `SYS Category`        AS sys_category,
  `Brand`               AS brand,
  `Game Theme`          AS game_theme,
  `CapDate`             AS capdate,
  `CapVal-USD`          AS capval_usd,
  `NetBook-USD`         AS netbook_usd,
  `Salvage Value`       AS salvage_value,
  `Depreciation`        AS depreciation,
  `Beginning Month Date` AS beginning_month_date
FROM ${bronze_catalog}.atlas.RPM_Exp_Depreciation
WHERE IS_DELETED = false;


CREATE OR REPLACE VIEW ${silver_catalog}.atlas_stage.RPM_Intl_Performance_EMEA_Africa AS
SELECT
  `SERIAL NUM`         AS serial_num,
  `MATERIAL NUM`       AS material_num,
  `NO OF BILLING DAYS` AS no_of_billing_days,
  `PROFIT CTR`         AS profit_ctr,
  `THEME ID`           AS theme_id,
  `INSTALL AT`         AS install_at,
  `INSTALL AT NAME`    AS install_at_name,
  `LOCAL NET VALUE`    AS local_net_value,
  `LOCAL NET PRICE`    AS local_net_price,
  `SYSTEM ID`          AS system_id,
  `BRAND ID`           AS brand_id,
  `JURIS`              AS juris,
  `Posting Period`     AS posting_period
FROM ${bronze_catalog}.atlas.RPM_Intl_Performance_EMEA_Africa
WHERE IS_DELETED = false;


CREATE OR REPLACE VIEW ${silver_catalog}.atlas_stage.RPM_Intl_Performance_EMEA_FixedFee AS
SELECT
  `Recurring Type`    AS recurring_type,
  `Serial Number`     AS serial_number,
  `Install-At Number` AS install_at_number,
  `Jurisdiction`      AS jurisdiction,
  `Region`            AS region,
  `Profit center`     AS profit_center,
  `SAP Material`      AS sap_material,
  `Cabinet`           AS cabinet,
  `Brand Description` AS brand_description,
  `Theme Description` AS theme_description,
  `Unit Status`       AS unit_status,
  `Total Active days` AS total_active_days,
  `Month`             AS month
FROM ${bronze_catalog}.atlas.RPM_Intl_Performance_EMEA_FixedFee
WHERE IS_DELETED = false;


CREATE OR REPLACE VIEW ${silver_catalog}.atlas_stage.RPM_Intl_Performance_EMEA_GreeceWLA AS
SELECT
  `Month`         AS month_of_period_start,
  `Serial Number` AS machine_id_serial,
  `Coin In`       AS sum_of_coin_in,
  `Net Win`       AS sum_of_net_win,
  `Net Win_2`     AS net_win_2_players_reward_deducted,
  `EGM Day Count` AS egm_day_count,
  `Deal Type`     AS deal_type
FROM ${bronze_catalog}.atlas.RPM_Intl_Performance_EMEA_GreeceWLA
WHERE IS_DELETED = false;


CREATE OR REPLACE VIEW ${silver_catalog}.atlas_stage.RPM_Intl_Performance_LAC AS
SELECT
  `Serial Number`    AS serial_number,
  `Posting Period`   AS posting_period,
  `Sold To Cust Num` AS sold_to_cust_num,
  `Source Currency`  AS source_currency,
  `Gross Win`        AS gross_win,
  `Net Win`          AS net_win,
  `IGT Split`        AS igt_split,
  `Discount`         AS discount,
  `Net IGT Rev`      AS net_igt_rev,
  `Days on-line`     AS days_on_line,
  `Country`          AS country,
  `Brand`            AS brand,
  `Theme`            AS theme
FROM ${bronze_catalog}.atlas.RPM_Intl_Performance_LAC
WHERE IS_DELETED = false;


CREATE OR REPLACE VIEW ${silver_catalog}.atlas_stage.RPM_Peformance_PremLotto_DE AS
SELECT
  `Fiscal End of Month Date` AS fiscal_end_of_month_date,
  `License`                  AS license,
  `Amount_Played`            AS amount_played,
  `Net_Revenue`              AS net_revenue,
  `IGT FEE`                  AS igt_fee,
  `Profit Center`            AS profit_center,
  `StartDate`                AS startdate,
  `EndDate`                  AS enddate
FROM ${bronze_catalog}.atlas.RPM_Peformance_PremLotto_DE
WHERE IS_DELETED = false;


CREATE OR REPLACE VIEW ${silver_catalog}.atlas_stage.RPM_Peformance_PremLotto_NY AS
SELECT
  `Fiscal End of Month Date` AS fiscal_end_of_month_date,
  `IGT SN`                   AS igt_sn,
  `Credits Played`           AS credits_played,
  `Coupons Played`           AS coupons_played,
  `Net Win (Credits)`        AS net_win_credits,
  `IGT FEE`                  AS igt_fee,
  `Profit Center`            AS profit_ctr
FROM ${bronze_catalog}.atlas.RPM_Peformance_PremLotto_NY
WHERE IS_DELETED = false;


CREATE OR REPLACE VIEW ${silver_catalog}.atlas_stage.RPM_Peformance_PremLotto_RI AS
SELECT
  `Fiscal End of Month Date` AS fiscal_end_of_month_date,
  `IGT SN`                   AS igt_sn,
  `Cash In`                  AS cash_in,
  `Net Income`               AS net_income,
  `IGT FEE`                  AS igt_fee,
  `Profit Center`            AS profit_ctr
FROM ${bronze_catalog}.atlas.RPM_Peformance_PremLotto_RI
WHERE IS_DELETED = false;


CREATE OR REPLACE VIEW ${silver_catalog}.atlas_stage.RPM_Performance_CDS_Class_II AS
SELECT
  `Install At`                AS install_at,
  `Document Num`              AS document_num,
  `Invoice Date From`         AS invoice_date_from,
  `Invoice Date To`           AS invoice_date_to,
  `MJSegment`                 AS mjsegment,
  `MJPJur`                    AS mjpjur,
  `Serial Num`                AS serial_num,
  `Brand Code`                AS brand_code,
  `Theme Code`                AS theme_code,
  `Aggregated Amount Wagered` AS aggregated_amount_wagered,
  `Aggregated Amount Won`     AS aggregated_amount_won,
  `Gross Win`                 AS gross_win,
  `Aggregated Machine Days`   AS aggregated_machine_days,
  `Lease Rate`                AS lease_rate,
  `Flat Rate`                 AS flat_rate,
  `Min Per Day`               AS min_per_day,
  `Casino Split`              AS casino_split,
  `IGT Split`                 AS igt_split,
  `Total Rev`                 AS total_rev,
  `Total Discount`            AS total_discount,
  `Net Amount`                AS net_amount,
  `Concession Amount`         AS concession_amount
FROM ${bronze_catalog}.atlas.RPM_Performance_CDS_Class_II
WHERE IS_DELETED = false;


CREATE OR REPLACE VIEW ${silver_catalog}.atlas_stage.RPM_Performance_CDS_Class_II_ManualBilling AS
SELECT
  `End of Month Date` AS end_of_month_date,
  `Install At`        AS install_at,
  `Serial Num`        AS serial_num,
  `Coin In`           AS coin_in,
  `Gross Win`         AS gross_win,
  `Cust. Split`       AS cust_split,
  `IGT Split`         AS igt_split,
  `Gross IGT Rev`     AS gross_igt_rev,
  `Net IGT Rev`       AS net_igt_rev,
  `Machine Days`      AS machine_days,
  `Par Split`         AS par_split
FROM ${bronze_catalog}.atlas.RPM_Performance_CDS_Class_II_ManualBilling
WHERE IS_DELETED = false;


CREATE OR REPLACE VIEW ${silver_catalog}.atlas_stage.RPM_Performance_WAP_NewCanada AS
SELECT
  `Brand Code`               AS brand_code,
  `Theme Code`               AS theme_code,
  `System`                   AS system,
  `Customer Num Install At`  AS customer_num_install_at,
  `Serial Num`               AS serial_num,
  `EOD Date`                 AS eod_date,
  `Machine Hours`            AS machine_hours,
  `Meter Wagered Amount`     AS meter_wagered_amount,
  `CEJE Amount`              AS ceje_amount,
  `Contribution Rate`        AS contribution_rate,
  `WAP Operator Fee`         AS wap_operator_fee,
  `Daily CAP Setting Amount` AS daily_cap_setting_amount,
  `Daily CAP Discount Amount` AS daily_cap_disc_amount,
  `Daily MIN Setting Amount` AS daily_min_setting_amount,
  `Daily MIN Disc Amount`    AS daily_min_disc_amount,
  `OOS Discount`             AS oos_discount,
  `Daily Flat Fee`           AS daily_flat_fee,
  `WAP Operator Fee Discount` AS wap_operator_fee_discount,
  `Total Operator Fee`       AS total_operator_fee,
  `Final Contribution Amount` AS final_contribution_amount
FROM ${bronze_catalog}.atlas.RPM_Performance_WAP_NewCanada
WHERE IS_DELETED = false;
