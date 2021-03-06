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
DROP TABLE jostens_stage.indv_orders;
CREATE TABLE jostens_stage.indv_orders(
 bridge_order_number bigint, 
  sales_rep_number bigint, 
  oracle_customer_number varchar(30), 
  service_order_number bigint, 
  oracle_order_number bigint, 
  order_entered_date timestamp, 
  order_requested_delivery_date timestamp, 
  order_scheduled_ship_date timestamp, 
  order_created_date timestamp, 
  order_imported_date timestamp, 
  order_cycle_duration DOUBLE PRECISION, 
  order_group varchar(30), 
  order_type varchar(80), 
  order_class varchar(80), 
  order_source varchar(80), 
  status varchar(30), 
  status_reason varchar(30), 
  prepaid_ind varchar(1), 
  sales_channel varchar(80), 
  customer_service_team_name varchar(80), 
  return_number bigint, 
  creation_code varchar(30), 
  creation_reason varchar(30), 
  collection_doc_replace_flag varchar(1), 
  collection_doc_exclude_flag varchar(1), 
  shipment_priority_code varchar(30), 
  greg_who varchar(150), 
  greg_order_number varchar(150), 
  greg_what_was_ordered varchar(150), 
  greg_what_was_received varchar(150), 
  yb_job_number varchar(150), 
  yb_file_year varchar(150), 
  yb_customer_number varchar(150), 
  yb_advisor_name varchar(150), 
  yb_customer_phone_number varchar(150), 
  yb_plant_code varchar(150), 
  yb_jds_vendor varchar(150), 
  order_receipt_method varchar(150), 
  sales_rep_transaction_type varchar(10), 
  annc_add_order varchar(150), 
  annc_closed_date varchar(150), 
  annc_what_was_ordered varchar(150), 
  annc_what_was_received varchar(150), 
  dipl_who varchar(150), 
  dipl_order_number bigint, 
  dipl_what_was_ordered varchar(150), 
  dipl_what_was_received varchar(150), 
  ordered_by_name varchar(150), 
  ordered_by_phone varchar(150), 
  ordered_by_email varchar(150), 
  combined_bridge_order bigint, 
  combined_order_flag varchar(1), 
  created_by varchar(100), 
  creation_date timestamp, 
  comments varchar(2000), 
  lfxp_who varchar(150), 
  lfxp_order_number varchar(150), 
  lfxp_what_was_ordered varchar(150), 
  lfxp_what_was_received varchar(150), 
  copied_from_bridge_order bigint, 
  bridge_order_grad_date varchar(150), 
  grad_year DOUBLE PRECISION, 
  photographer_number bigint, 
  photographer_name varchar(30), 
  source_order_reference varchar(250), 
  ybms_job_number varchar(150), 
  ybms_yearbook_year varchar(150), 
  ybms_plant varchar(150), 
  ybms_program varchar(150), 
  ybms_trim_size varchar(150), 
  ybms_pages bigint, 
  ybms_copies bigint, 
  ybms_large_label_qty bigint, 
  ybms_small_label_qty bigint, 
  ybms_yeartech_online varchar(150), 
  ybms_service_order_id bigint, 
  ybms_mf_kit_reques_date timestamp, 
  ybms_commission_pct bigint, 
  ybms_yto_indicator varchar(2), 
  op_received_date timestamp, 
  mfg_instructions varchar(240), 
  greg_grad_type_date varchar(150), 
  submitter_type varchar(150), 
  shop_handling_code varchar(150), 
  last_update_date timestamp, 
  schedule_type varchar(150), 
  purchase_order_number varchar(50), 
  zdp_indicator varchar(1), 
  zdp_status varchar(150), 
  sample_order_type varchar(2), 
  owms_team varchar(30), 
  owms_order_category varchar(30), 
  owms_order_type varchar(30), 
  owms_job_num varchar(30), 
  owms_stuff_date varchar(30), 
  owms_cd_code varchar(30), 
  owms_cust_num varchar(30), 
  owms_order_info varchar(50), 
  merge_eligible_flag varchar(1), 
  combined_ndpd_ind varchar(1),
  PRIMARY KEY (bridge_order_number)
);
TRUNCATE TABLE  jostens_stage.indv_orders ;
​
COPY jostens_stage.indv_orders
FROM 's3://jostens-data-dev-analysis/indv_orders/'
FORMAT AS PARQUET
IAM_ROLE 'arn:aws:iam::147913565791:role/redshift-iam-role-s3-glue-athena' ;
​
SELECT COUNT(*) FROM jostens_stage.indv_orders;
SELECT * FROM jostens_stage.indv_orders limit 5;
DROP TABLE IF EXISTS jostens.indv_orders CASCADE; 
​
CREATE TABLE jostens.indv_orders
SORTKEY (inventory_item_id) 
AS 
SELECT  *
FROM  jostens_stage.indv_orders;
​
select * from jostens.indv_orders limit 5;
select count(*) from jostens.indv_orders;

--data type
select pg_get_cols ('jostens.indv_orders');