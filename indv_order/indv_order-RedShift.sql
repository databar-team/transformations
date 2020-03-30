DROP TABLE jostens_stage.indv_order;
CREATE TABLE jostens_stage.indv_order
(
  indv_order_number bigint, 
  bridge_order_number bigint, 
  service_order_number bigint, 
  service_order_type varchar(30),
  order_group varchar(30), 
  c_consumer_id bigint, 
  distribution_number bigint, 
  early_delivery_list_status varchar(8), 
  external_bill_to_system_ref varchar(30), 
  external_ship_to_system_ref varchar(30), 
  order_form_id bigint, 
  item_validation_source varchar(30), 
  ordered_date timestamp, 
  organization_id bigint, 
  profile_id bigint, 
  salesrep_tx_type varchar(10), 
  salesrep_tx_type_desc varchar(30), 
  ship_method_code varchar(30), 
  ship_method_desc varchar(80), 
  ship_to_io_address_flag varchar(1), 
  source_balance_due varchar(9), 
  source_batch_name varchar(30), 
  source_form_name varchar(30), 
  source_indv_order_reference varchar(250), 
  source_input_operator_id varchar(30), 
  source_order_reference varchar(250), 
  source_paid_amount DOUBLE PRECISION, 
  source_system_name varchar(30), 
  source_total_sell_price DOUBLE PRECISION, 
  source_total_tax_amount DOUBLE PRECISION, 
  source_transact_reference varchar(250), 
  status varchar(30), 
  status_reason varchar(30), 
  use_profile_flag varchar(1), 
  salesrep_notes varchar(2000), 
  io_creation_date timestamp, 
  comments varchar(2000), 
  grad_pres_date varchar(150), 
  using_contemporary_dateline varchar(80), 
  namelist_file varchar(240), 
  original_item_ordered varchar(50), 
  combined_indv_order_number bigint, 
  jop_io_number varchar(5), 
  special_offers_opt_in_flag varchar(1), 
  assoc_sales_rep_number varchar(20), 
  prod_code_1 varchar(30), 
  warranty_bin_number varchar(240), 
  tracker_number varchar(240), 
  inside_engraving_ind varchar(240), 
  inside_engraving_cnt varchar(240), 
  original_order_number varchar(240), 
  scheduling_type varchar(240), 
  pricing_code varchar(240), 
  ioc99_prod_code1 varchar(240), 
  ioc99_prod_code2 varchar(240), 
  ioc99_prod_code3 varchar(240), 
  ioc99_prod_code4 varchar(240), 
  last_updated_by bigint, 
  distribution1 varchar(150), 
  distribution2 varchar(150), 
  context varchar(150), 
  attribute78 varchar(240), 
  attribute82 varchar(240), 
  attribute83 varchar(240), 
  attribute126 varchar(240), 
  attribute138 varchar(240), 
  attribute146 varchar(240), 
  attribute147 varchar(240), 
  stone_color varchar(80), 
  allow_pricing_err varchar(1), 
  allow_pricing_err_reason varchar(150), 
  top_core_font varchar(2), 
  height varchar(20), 
  weight varchar(20), 
  gender varchar(10), 
  cap_size varchar(12), 
  special_gwn_sizes varchar(1), 
  engrave_option_code varchar(4), 
  jlry_default_group varchar(30), 
  dropship_rep_override_flag varchar(1), 
  design_id varchar(20), 
  ordered_by_name varchar(1000), 
  original_web_quote varchar(20),
  PRIMARY KEY (indv_order_number)
);

--TRUNCATE TABLE  jostens_stage.indv_order ;
​
COPY jostens_stage.indv_order
FROM 's3://jostens-data-dev-analysis/indv_order/'
FORMAT AS PARQUET
IAM_ROLE 'arn:aws:iam::147913565791:role/redshift-iam-role-s3-glue-athena' ;
​
SELECT COUNT(*) FROM jostens_stage.indv_order;
DROP TABLE IF EXISTS jostens.indv_order; 
​
CREATE TABLE jostens.indv_order
SORTKEY (indv_order) 
AS 
SELECT  *
FROM  jostens_stage.indv_order  ;
​
