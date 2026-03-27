CREATE OR REPLACE VIEW atlas_stage.install_base_activity_with_override AS
WITH base_join AS (
  SELECT
    a.*,
    bo.customer_number AS o_customer_number,
    bo.product_type    AS o_product_type,
    bo.profit_center   AS o_profit_center,
    bo.system_code     AS o_system_code,
    bo.brand_code      AS o_brand_code,
    bo.theme_code      AS o_theme_code,
    bo.pricing         AS o_pricing,
    bo.material        AS o_material,
    bo.stl             AS o_stl,
    row_number() OVER (
      PARTITION BY a.install_base_activity_sk
      ORDER BY bo.effective_from DESC, bo.audit_create_date DESC
    ) AS rn_bo
  FROM atlas_stage.install_base_activity a
  LEFT JOIN atlas_map.install_base_override bo
    ON a.serial_number = bo.serial_number
   AND a.fiscal_week >= bo.effective_from
   AND (bo.effective_to IS NULL OR a.fiscal_week <= bo.effective_to)
   AND bo.audit_is_deleted = false
  WHERE a.audit_is_deleted = false
),
activity_join AS (
  SELECT
    b.*,
    ao.activity_new AS o_activity,
    row_number() OVER (
      PARTITION BY b.install_base_activity_sk
      ORDER BY ao.audit_create_date DESC
    ) AS rn_ao
  FROM base_join b
  LEFT JOIN atlas_map.install_base_activity_override ao
    ON b.serial_number = ao.serial_number
   AND b.fiscal_week   = ao.fiscal_week
   AND b.activity      = ao.activity_old
   AND ao.audit_is_deleted = false
  WHERE b.rn_bo = 1
)
SELECT
  install_base_activity_sk,
  calendar_month,
  fiscal_week,
  calendar_quarter,
  COALESCE(o_product_type, product_type)         AS product_type,
  serial_number,
  COALESCE(o_customer_number, customer_number)   AS customer_number,
  COALESCE(o_system_code, system_code)           AS system_code,
  COALESCE(o_brand_code, brand_code)             AS brand_code,
  COALESCE(o_theme_code, theme_code)             AS theme_code,
  COALESCE(o_pricing, pricing)                   AS pricing,
  COALESCE(o_activity, activity)                 AS activity,
  COALESCE(o_stl, stl)                           AS stl,
  starting_ib,
  COALESCE(o_material, material)                 AS material,
  COALESCE(o_profit_center, profit_center)       AS profit_center,
  install_date,
  go_live_date,
  shut_down_date,
  removal_date,
  audit_etl_id,
  audit_create_date,
  audit_update_date,
  audit_is_deleted
FROM activity_join
WHERE rn_ao = 1;