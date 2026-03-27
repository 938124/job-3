CREATE TABLE IF NOT EXISTS atlas_stage.install_base (
  install_base_sk     BIGINT          NOT NULL,
  fiscal_week         DATE            NOT NULL,
  product_type        STRING          NOT NULL,
  serial_number       STRING          NOT NULL,
  customer_number     STRING          NOT NULL,
  system_code         STRING          NOT NULL,
  brand_code          STRING          NOT NULL,
  theme_code          STRING          NOT NULL,
  pricing             STRING          NOT NULL,
  material            STRING          NOT NULL,
  stl                 STRING          ,
  profit_center       STRING          ,
  install_date        DATE            ,
  go_live_date        DATE            ,
  audit_etl_id        BIGINT          NOT NULL,
  audit_create_date   TIMESTAMP       not null,
  audit_update_date   TIMESTAMP       not null,
  audit_is_deleted    boolean         not null,
  CONSTRAINT pk_atlas_stage_install_base PRIMARY KEY (install_base_sk)
)
USING DELTA;