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
DROP TABLE jostens_stage.item_mf01;
CREATE TABLE jostens_stage.item_mf01(
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
  STRUCTURE_NAME varchar(30), 
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
  shippable_item_flag varchar(1), 
  min_minmax_quantity bigint, 
  max_minmax_quantity bigint, 
  lot_control_code bigint, 
  full_lead_time bigint
  );
TRUNCATE TABLE  jostens_stage.item_mf01 ;
​
COPY jostens_stage.item_mf01
FROM 's3://jostens-data-dev-analysis/item_mf01/'
FORMAT AS PARQUET
IAM_ROLE 'arn:aws:iam::147913565791:role/redshift-iam-role-s3-glue-athena' ;
​
SELECT COUNT(*) FROM jostens_stage.item_mf01;
SELECT * FROM jostens_stage.item_mf01;
DROP TABLE IF EXISTS jostens.item_mf01 CASCADE; 
​
CREATE TABLE jostens.item_mf01
SORTKEY (inventory_item_id) 
AS 
SELECT  *
FROM  jostens_stage.item_mf01;
​
select * from jostens.item_mf01 limit 5;
select count(*) from jostens.item_mf01;

--data type
select pg_get_cols ('jostens.item_mf01');