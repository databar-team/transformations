CREATE TABLE IF NOT EXISTS   jostens_stage.indv_order
(
  indv_order_number bigint, 
  bridge_order_number bigint, 
  service_order_number bigint, 
  service_order_type string, 
  order_group string, 
  c_consumer_id bigint, 
  distribution_number bigint, 
  early_delivery_list_status string, 
  external_bill_to_system_ref string, 
  external_ship_to_system_ref string, 
  order_form_id bigint, 
  item_validation_source string, 
  ordered_date timestamp, 
  organization_id bigint, 
  profile_id bigint, 
  salesrep_tx_type string, 
  salesrep_tx_type_desc string, 
  ship_method_code string, 
  ship_method_desc string, 
  ship_to_io_address_flag string, 
  source_balance_due string, 
  source_batch_name string, 
  source_form_name string, 
  source_indv_order_reference string, 
  source_input_operator_id string, 
  source_order_reference string, 
  source_paid_amount double, 
  source_system_name string, 
  source_total_sell_price double, 
  source_total_tax_amount double, 
  source_transact_reference string, 
  status string, 
  status_reason string, 
  use_profile_flag string, 
  salesrep_notes string, 
  io_creation_date timestamp, 
  comments string, 
  grad_pres_date string, 
  using_contemporary_dateline string, 
  namelist_file string, 
  original_item_ordered string, 
  combined_indv_order_number bigint, 
  jop_io_number string, 
  special_offers_opt_in_flag string, 
  assoc_sales_rep_number string, 
  prod_code_1 string, 
  warranty_bin_number string, 
  tracker_number string, 
  inside_engraving_ind string, 
  inside_engraving_cnt string, 
  original_order_number string, 
  scheduling_type string, 
  pricing_code string, 
  ioc99_prod_code1 string, 
  ioc99_prod_code2 string, 
  ioc99_prod_code3 string, 
  ioc99_prod_code4 string, 
  last_updated_by bigint, 
  distribution1 string, 
  distribution2 string, 
  context string, 
  attribute78 string, 
  attribute82 string, 
  attribute83 string, 
  attribute126 string, 
  attribute138 string, 
  attribute146 string, 
  attribute147 string, 
  stone_color string, 
  allow_pricing_err string, 
  allow_pricing_err_reason string, 
  top_core_font string, 
  height string, 
  weight string, 
  gender string, 
  cap_size string, 
  special_gwn_sizes string, 
  engrave_option_code string, 
  jlry_default_group string, 
  dropship_rep_override_flag string, 
  design_id string, 
  ordered_by_name string, 
  original_web_quote string
  PRIMARY KEY (abc)
);



