Select 
 pid, 
 user_name, 
 starttime, 
 query, 
 query, 
 status 
from stv_recents 
where status='Running';
SELECT pg_terminate_backend(18709);
DROP TABLE jostens_stage.item_update;
CREATE TABLE jostens_stage.item_update(
  cover_size varchar(50), 
  dimensions varchar(50), 
  item_size varchar(50), 
  style varchar(50), 
  type varchar(50), 
  material varchar(50), 
  color varchar(50), 
  degree varchar(50), 
  lot_size varchar(50), 
  metal_quality varchar(50), 
  apparel_color varchar(50), 
  apparel_size varchar(50), 
  quality varchar(50), 
  length varchar(50), 
  design varchar(240), 
  trim_size varchar(240), 
  system_update_date_time date,
  inventory_item_id bigint, 
  organization_id bigint, 
  cogs_code_combination_id bigint, 
  sales_code_combination_id bigint, 
  expense_code_combination_id bigint, 
  organization_code varchar(3), 
  organization_name varchar(60), 
  item_number varchar(50), 
  std_lot_size bigint, 
  description varchar(240), 
  item_type varchar(80), 
  item_status varchar(10), 
  unit_weight bigint, 
  weight_uom_code varchar(10), 
  inventory_category varchar(50), 
  purchasing_category varchar(50), 
  gl_product_category varchar(50), 
  prod_class_category varchar(50), 
  prod_sub_category varchar(50), 
  purchasing_sub_class varchar(40), 
  primary_unit_of_measure varchar(25), 
  generic_flag varchar(1), 
  costing_enabled_flag varchar(1), 
  inventory_asset_flag varchar(1), 
  dropship_flag varchar(1), 
  planning_make_buy varchar(5), 
  material_cost bigint, 
  resource_cost bigint, 
  outside_processing_cost bigint, 
  material_overhead_cost bigint, 
  overhead_cost bigint, 
  item_cost bigint, 
  descriptor_invoice_usage varchar(240), 
  item_type_id bigint, 
  shippable_item_update_flag varchar(1), 
  min_minmax_quantity bigint, 
  max_minmax_quantity bigint, 
  lot_control_code bigint, 
  full_lead_time bigint
);
TRUNCATE TABLE  jostens_stage.item_update ;
​
COPY jostens_stage.item_update
FROM 's3://jostens-data-dev-analysis/item_update/'
FORMAT AS PARQUET
IAM_ROLE 'arn:aws:iam::147913565791:role/redshift-iam-role-s3-glue-athena' ;
​
SELECT COUNT(*) FROM jostens_stage.item_update;
SELECT * FROM jostens_stage.item_update limit 5;
DROP TABLE IF EXISTS jostens.item_update CASCADE; 
​
CREATE TABLE jostens.item_update
SORTKEY (inventory_item_id) 
AS 
SELECT  *
FROM  jostens_stage.item_update;
​
select * from jostens.item_update limit 5;
select count(*) from jostens.item_update;

--data type
select pg_get_cols ('jostens.item_update');