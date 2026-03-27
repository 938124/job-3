CREATE OR REPLACE VIEW atlas_stage.install_base_with_override AS
WITH j AS (
  SELECT
    ib.*,
    ibo.product_type      AS o_product_type,
    ibo.customer_number   AS o_customer_number,
    ibo.system_code       AS o_system_code,
    ibo.brand_code        AS o_brand_code,
    ibo.theme_code        AS o_theme_code,
    ibo.pricing           AS o_pricing,
    ibo.material          AS o_material,
    ibo.stl               AS o_stl,
    ibo.profit_center     AS o_profit_center,
    ibo.install_base_override_sk,
    row_number() OVER (
      PARTITION BY ib.serial_number, ib.fiscal_week
      ORDER BY ibo.effective_from DESC, ibo.audit_create_date DESC
    ) AS rn
  FROM atlas_stage.install_base ib
  LEFT JOIN atlas_map.install_base_override ibo
    ON ib.serial_number = ibo.serial_number
   AND ib.fiscal_week >= ibo.effective_from
   AND (ibo.effective_to IS NULL OR ib.fiscal_week <= ibo.effective_to)
   AND ibo.audit_is_deleted = false
  WHERE ib.audit_is_deleted = false
)
SELECT
  install_base_sk,
  install_base_override_sk,
  fiscal_week,
  serial_number,
  COALESCE(o_product_type, product_type)      AS product_type,
  COALESCE(o_customer_number, customer_number) AS customer_number,
  COALESCE(o_system_code, system_code)        AS system_code,
  COALESCE(o_brand_code, brand_code)          AS brand_code,
  COALESCE(o_theme_code, theme_code)          AS theme_code,
  COALESCE(o_pricing, pricing)                AS pricing,
  COALESCE(o_material, material)              AS material,
  COALESCE(o_stl, stl)                        AS stl,
  COALESCE(o_profit_center, profit_center)    AS profit_center,
  install_date,
  go_live_date,
  audit_etl_id,
  audit_create_date,
  audit_update_date,
  audit_is_deleted
FROM j
WHERE rn = 1;