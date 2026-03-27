CREATE OR REPLACE VIEW ${silver_catalog}.atlas_stage.install_base_core AS
WITH base AS (
  SELECT
    t.CustomerFull,
    t.CorporateCust,
    t.AccountManager,
    t.SalesGroup,
    t.SalesOffice,
    t.SalesDistrict,
    t.SalesDirector,
    t.JurisdictionCode,
    t.JurisdictionFull,
    t.MJPJurisdictionCode,
    t.MJPJurisdiction,
    t.MJPJurisdictionFull,
    t.StateCode,
    t.SystemFull,
    t.ProductCategory,
    t.ProductCategoryName,
    t.ProductGroupName,
    t.ProductLine,
    t.ProductLineName,
    t.ProductSegment,
    t.ProductSegmentName,
    t.ProfitCenterName,
    t.UpdateUser,
    t.UpdateDate,
    t.Notes,
    t.IsTempShutdown,
    t.PremiumBrand,
    t.City,
    t.COVIDRecoveryDate,
    t.ConversionDate,
    t.CustomerName,
    t.DateFrom,
    t.STLFlag,
    t.DateTo,
    t.CustomerNum,
    t.CorporateCustName,
    t.AccountManagerName,
    t.EquipmentNum,
    t.CompanyCode,
    t.SerialNum,
    t.SalesRegion,
    t.SalesGroupName,
    t.SalesOfficeName,
    t.SalesDistrictName,
    t.SalesDirectorName,
    t.Jurisdiction,
    t.GameType,
    t.ReelStrip,
    t.State,
    t.SystemCode,
    t.System,
    t.MJDenom,
    t.Material,
    t.UnitType,
    t.BillTypeCode,
    t.ProductType,
    t.ProductChannel,
    t.ProductGroup,
    t.BeginCount,
    t.Installs,
    t.Removals,
    t.Conversions,
    t.EndCount,
    t.FunctionalArea,
    t.IsDomestic,
    t.LegacyUnitType,
    t.UCStatus,
    t.ProfitCenter,
    t.RemovalDate,
    t.ShutdownDate,
    t.GoLiveDate,
    t.InstallDate,

    CASE
      WHEN t.Removals = 1 AND t.OldBrandCode <> t.BrandCode
        THEN concat(t.OldBrandCode, ' - ', t.OldBrand)
      ELSE t.BrandFull
    END AS brand_full_adj,

    CASE
      WHEN t.Conversions = 1 AND t.OldBrandCode <> t.BrandCode
        THEN t.OldBrandCode
    END AS conv_old_brand_code,

    CASE
      WHEN t.Conversions = 1 AND t.OldBrandCode <> t.BrandCode
        THEN t.OldBrand
    END AS conv_old_brand,

    CASE
      WHEN t.Conversions = 1 AND t.OldThemeCode <> t.ThemeCode
        THEN t.OldThemeCode
    END AS conv_old_theme_code,

    CASE
      WHEN t.Conversions = 1 AND t.OldThemeCode <> t.ThemeCode
        THEN t.OldTheme
    END AS conv_old_theme,

    CASE
      WHEN t.Removals = 1 AND t.OldBrandCode <> t.BrandCode
        THEN t.OldBrandCode
      ELSE t.BrandCode
    END AS brand_code_adj,

    CASE
      WHEN t.Removals = 1 AND t.OldBrandCode <> t.BrandCode
        THEN t.OldBrand
      ELSE t.Brand
    END AS brand_adj,

    CASE
      WHEN t.Removals = 1 AND t.OldThemeCode <> t.ThemeCode
        THEN t.OldThemeCode
      ELSE t.ThemeCode
    END AS theme_code_adj,

    CASE
      WHEN t.Removals = 1 AND t.OldThemeCode <> t.ThemeCode
        THEN t.OldTheme
      ELSE t.Theme
    END AS theme_adj,

    CASE
      WHEN t.Removals = 1 AND t.OldThemeCode <> t.ThemeCode
        THEN concat(t.OldThemeCode, ' - ', t.OldTheme)
      ELSE t.ThemeFull
    END AS theme_full_adj,

    CASE
      WHEN length(coalesce(t.SoldToCustomerNum, '')) = 0
        THEN t.CustomerNum
      ELSE t.SoldToCustomerNum
    END AS sold_to_customer_num_adj,

    CASE
      WHEN length(coalesce(t.SoldToCustomerFull, '')) = 0
        THEN t.CustomerFull
      ELSE t.SoldToCustomerFull
    END AS sold_to_customer_full_adj,

    t.etl_id AS etl_id
  FROM ${bronze_catalog}.atlas.zuc_detail_ca t
)

SELECT
  CustomerFull          AS customer_full,
  CorporateCust         AS corporate_cust,
  AccountManager        AS account_manager,
  SalesGroup            AS sales_group,
  SalesOffice           AS sales_office,
  SalesDistrict         AS sales_district,
  SalesDirector         AS sales_director,
  JurisdictionCode      AS jurisdiction_code,
  JurisdictionFull      AS jurisdiction_full,
  MJPJurisdictionCode   AS mjp_jurisdiction_code,
  MJPJurisdiction       AS mjp_jurisdiction,
  MJPJurisdictionCode   AS mjp_jurisdiction_code_dup,
  MJPJurisdictionFull   AS mjp_jurisdiction_full,
  StateCode             AS state_code,
  SystemFull            AS system_full,
  brand_full_adj        AS brand_full_adj,
  ProductCategory       AS product_category,
  ProductCategoryName   AS product_category_name,
  ProductGroupName      AS product_group_name,
  ProductLine           AS product_line,
  ProductLineName       AS product_line_name,
  ProductSegment        AS product_segment,
  ProductSegmentName    AS product_segment_name,
  ProfitCenterName      AS profit_center_name,
  UpdateUser            AS update_user,
  UpdateDate            AS update_date,
  Notes                 AS notes,
  IsTempShutdown        AS is_temp_shutdown,
  PremiumBrand          AS premium_brand,
  City                  AS city,
  COVIDRecoveryDate     AS covid_recovery_date,
  ConversionDate        AS conversion_date,
  CustomerName          AS customer_name,
  DateFrom              AS date_from,
  STLFlag               AS stl_flag,
  DateTo                AS date_to,
  CustomerNum           AS customer_num,
  CustomerName          AS customer_name_dup,
  CorporateCustName     AS corporate_cust_name,
  AccountManagerName    AS account_manager_name,
  EquipmentNum          AS equipment_num,
  CompanyCode           AS company_code,
  SerialNum             AS serial_number,
  SalesRegion           AS sales_region,
  SalesGroupName        AS sales_group_name,
  SalesOfficeName       AS sales_office_name,
  SalesDistrictName     AS sales_district_name,
  SalesDirectorName     AS sales_director_name,
  Jurisdiction          AS jurisdiction,
  MJPJurisdiction       AS mjp_jurisdiction_dup,
  GameType              AS game_type,
  ReelStrip             AS reel_strip,
  State                 AS state,
  SystemCode            AS system_code,
  System                AS system,
  conv_old_brand_code   AS conv_old_brand_code,
  conv_old_brand        AS conv_old_brand,
  conv_old_theme_code   AS conv_old_theme_code,
  conv_old_theme        AS conv_old_theme,
  brand_code_adj        AS brand_code_adj,
  brand_adj             AS brand_adj,
  theme_code_adj        AS theme_code_adj,
  theme_adj             AS theme_adj,
  theme_full_adj        AS theme_full_adj,
  MJDenom               AS mj_denom,
  Material              AS material,
  UnitType              AS unit_type,
  BillTypeCode          AS bill_type_code,
  ProductType           AS product_type,
  ProductChannel        AS product_channel,
  CorporateCustName     AS corporate_cust_name_dup,
  ProductGroup          AS product_group,
  SUM(BeginCount)       AS begin_count,
  SUM(Installs)         AS installs,
  SUM(Removals)         AS removals,
  SUM(Conversions)      AS conversions,
  SUM(EndCount)         AS end_count,
  FunctionalArea        AS functional_area,
  IsDomestic            AS is_domestic,
  LegacyUnitType        AS legacy_unit_type,
  UCStatus              AS uc_status,
  ProfitCenter          AS profit_center,
  RemovalDate           AS removal_date,
  ShutdownDate          AS shutdown_date,
  GoLiveDate            AS go_live_date,
  InstallDate           AS install_date,
  sold_to_customer_num_adj  AS sold_to_customer_num_adj,
  sold_to_customer_full_adj AS sold_to_customer_full_adj,
  etl_id                AS etl_id
FROM base
WHERE
  (
    (
      Material NOT IN ('QUICK_BET', 'QUICK_BET_U', 'QUICK_BET_V3', 'QUICK_BET_V3_U', 'QUICKBETV4')
      AND ProductType IN ('NON-PREMIUM', 'PREMIUM')
      AND UnitType NOT IN ('LOAN_TEST')
      AND ProfitCenter NOT IN (
        '0000441010', '0000101650', '0000104608', '0000100313',
        '0000143605', '0000260104', '0000360001', '0000250135'
      )
      AND Jurisdiction NOT IN (
        'Macau - MO',
        'Delaware VLT - US',
        'New York VLT - US',
        'Rhode Island VLT - US',
        'New South Wales - (NSWAL) DNU'
      )
      AND STLFlag NOT IN ('0002')
      AND brand_adj NOT IN ('IGT CORE PRODUCT')
    )
    OR
    (
      (
        ProductType IN ('PREMIUM')
        AND Jurisdiction IN ('Delaware VLT - US', 'New York VLT - US', 'Rhode Island VLT - US')
      )
      OR
      (
        Jurisdiction IN ('Delaware VLT - US', 'New York VLT - US', 'Rhode Island VLT - US')
        AND brand_adj IN ('LT GAME PRODUCT', 'Multi Player')
      )
    )
  )
GROUP BY
  CustomerFull,
  CorporateCust,
  AccountManager,
  SalesGroup,
  SalesOffice,
  SalesDistrict,
  SalesDirector,
  JurisdictionCode,
  JurisdictionFull,
  MJPJurisdictionCode,
  MJPJurisdiction,
  MJPJurisdictionFull,
  StateCode,
  SystemFull,
  brand_full_adj,
  ProductCategory,
  ProductCategoryName,
  ProductGroupName,
  ProductLine,
  ProductLineName,
  ProductSegment,
  ProductSegmentName,
  ProfitCenterName,
  UpdateUser,
  UpdateDate,
  Notes,
  IsTempShutdown,
  PremiumBrand,
  City,
  COVIDRecoveryDate,
  ConversionDate,
  CustomerName,
  DateFrom,
  STLFlag,
  DateTo,
  CustomerNum,
  CorporateCustName,
  AccountManagerName,
  EquipmentNum,
  CompanyCode,
  SerialNum,
  SalesRegion,
  SalesGroupName,
  SalesOfficeName,
  SalesDistrictName,
  SalesDirectorName,
  Jurisdiction,
  GameType,
  ReelStrip,
  State,
  SystemCode,
  System,
  conv_old_brand_code,
  conv_old_brand,
  conv_old_theme_code,
  conv_old_theme,
  brand_code_adj,
  brand_adj,
  theme_code_adj,
  theme_adj,
  theme_full_adj,
  MJDenom,
  Material,
  UnitType,
  BillTypeCode,
  ProductType,
  ProductChannel,
  ProductGroup,
  FunctionalArea,
  IsDomestic,
  LegacyUnitType,
  UCStatus,
  ProfitCenter,
  RemovalDate,
  ShutdownDate,
  GoLiveDate,
  InstallDate,
  sold_to_customer_num_adj,
  sold_to_customer_full_adj,
  etl_id;