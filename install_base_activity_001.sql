CREATE TABLE IF NOT EXISTS atlas_stage.install_base_activity(
  install_base_activity_sk BIGINT     NOT NULL,
  fiscal_week         DATE            NOT NULL,
  product_type        STRING          ,
  serial_number       STRING          NOT NULL,
  customer_number     STRING          NOT NULL,
  system_code         STRING          NOT NULL,
  brand_code          STRING          NOT NULL,
  theme_code          STRING          NOT NULL,
  pricing             STRING          NOT NULL,
  activity            STRING          NOT NULL,
  stl                 STRING          NOT NULL,
  starting_ib         INT             ,    --should be 0 if null
  material            STRING          NOT NULL,
  profit_center       STRING          ,
  install_date        DATE            ,
  go_live_date        DATE            ,
  shutdown_date      DATE            ,
  removal_date        DATE            ,
  audit_etl_id        BIGINT          NOT NULL,
  audit_create_date   TIMESTAMP       NOT NULL,
  audit_update_date   TIMESTAMP       NOT NULL,
  audit_is_deleted    BOOLEAN         NOT NULL,
  CONSTRAINT pk_atlas_stage_install_base_activity PRIMARY KEY (install_base_activity_sk)
)
USING DELTA;