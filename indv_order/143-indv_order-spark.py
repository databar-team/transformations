sql1 = """SELECT jioa.indv_order_id as INDV_ORDER_NUMBER,
jioa.order_header_id as BRIDGE_ORDER_NUMBER,
joha.service_order_id as SERVICE_ORDER_NUMBER,
jso.service_order_type as SERVICE_ORDER_TYPE,
joha.order_group as ORDER_GROUP,
jioa.consumer_id as C_CONSUMER_ID,
jioa.distribution_number as DISTRIBUTION_NUMBER,
jioa.early_delivery_list_status as EARLY_DELIVERY_LIST_STATUS,
jioa.external_bill_to_system_ref as EXTERNAL_BILL_TO_SYSTEM_REF,
jioa.external_ship_to_system_ref as EXTERNAL_SHIP_TO_SYSTEM_REF,
jioa.order_form_id as ORDER_FORM_ID,
item_validation_source as ITEM_VALIDATION_SOURCE,
jioa.ordered_date as ORDERED_DATE,
jioa.org_id as ORGANIZATION_ID,
jioa.profile_id as PROFILE_ID,
jioa.salesrep_tx_type as SALESREP_TX_TYPE,
SUBSTR (jcl_sale.meaning, 1, 30) as SALESREP_TX_TYPE_DESC,
jioa.ship_method_code as SHIP_METHOD_CODE,
jcl_ship.meaning as SHIP_METHOD_DESC,
jioa.ship_to_io_address_flag as SHIP_TO_IO_ADDRESS_FLAG,
jioa.source_balance_due as SOURCE_BALANCE_DUE,
jioa.source_batch_name as SOURCE_BATCH_NAME,
jioa.source_form_name as SOURCE_FORM_NAME,
jioa.source_indv_order_reference as SOURCE_INDV_ORDER_REFERENCE,
jioa.source_input_operator_id as SOURCE_INPUT_OPERATOR_ID,
jioa.source_order_reference as SOURCE_ORDER_REFERENCE,
jioa.source_paid_amount as SOURCE_PAID_AMOUNT,
jioa.source_system_name as SOURCE_SYSTEM_NAME,
jioa.source_total_sell_price as SOURCE_TOTAL_SELL_PRICE,
jioa.source_total_tax_amount as SOURCE_TOTAL_TAX_AMOUNT,
jioa.source_transact_reference as SOURCE_TRANSACT_REFERENCE,
jioa.status as STATUS,
jioa.status_reason as STATUS_REASON,
jioa.use_profile_flag as USE_PROFILE_FLAG,
 '"' || REPLACE (jioa.salesrep_notes, '"', '""') || '"' as SALESREP_NOTES,
jioa.creation_date as IO_CREATION_DATE,
'"' || REPLACE (jioa.salesrep_notes, '"', '""') || '"' as COMMENTS,
SUBSTR (CASE jiodfv.context_value WHEN 'DIPL' THEN jiodfv.grad_pres_date ELSE NULL END, 1, 150) as GRAD_PRES_DATE,
SUBSTR(CASE jiodfv.context_value WHEN 'DIPL' THEN jiodfv.using_contemporary_dateline ELSE NULL END,1,80) as USING_CONTEMPORARY_DATELINE,
jioa.attribute39 as NAMELIST_FILE,
 CASE jioa.context WHEN 'LFXP_REPAIR' THEN jioa.attribute29 ELSE NULL END AS ORIGINAL_ITEM_ORDERED,
jioa.combined_indv_order_id as COMBINED_INDV_ORDER_NUMBER,
 SUBSTR(CASE jioa.context WHEN 'JEWL' THEN jioa.attribute9 ELSE NULL END,1,5) as JOP_IO_NUMBER,
 CASE WHEN jioa.context = 'ANNC_SY04' THEN CASE WHEN jioa.attribute65 = '1' THEN 'Y' ELSE 'N' END ELSE NULL END as SPECIAL_OFFERS_OPT_IN_FLAG,
CASE WHEN jioa.context = 'PGA' THEN SUBSTR(jioa.attribute67,1,20) ELSE NULL END as ASSOC_SALES_REP_NUMBER,
CASE WHEN jioa.context = 'IOC99' THEN SUBSTR(jioa.attribute55,1,30) ELSE NULL END as PROD_CODE_1,
CASE jioa.context WHEN 'JLRY_STD' THEN jioa.attribute94 ELSE NULL END as WARRANTY_BIN_NUMBER,
CASE jioa.context WHEN 'JLRY_STD' THEN jioa.attribute90 ELSE NULL END as TRACKER_NUMBER,
CASE jioa.context WHEN 'JLRY_STD' THEN jioa.attribute113 ELSE NULL END as INSIDE_ENGRAVING_IND,
CASE jioa.context WHEN 'JLRY_STD' THEN jioa.attribute72 ELSE NULL END as INSIDE_ENGRAVING_CNT,
CASE jioa.context WHEN 'JLRY_STD' THEN jioa.attribute93 ELSE NULL END as ORIGINAL_ORDER_NUMBER,
CASE jioa.context WHEN 'JLRY_STD' THEN jioa.attribute150 ELSE NULL END as SCHEDULING_TYPE,
CASE jioa.context WHEN 'JLRY_STD' THEN jioa.attribute114 ELSE NULL END as PRICING_CODE,
CASE jioa.context WHEN 'JLRY_STD' THEN jioa.attribute85 ELSE NULL END as IOC99_PROD_CODE1,
CASE jioa.context WHEN 'JLRY_STD' THEN jioa.attribute86 ELSE NULL END as IOC99_PROD_CODE2,
CASE jioa.context WHEN 'JLRY_STD' THEN jioa.attribute87 ELSE NULL END as IOC99_PROD_CODE3,
CASE jioa.context WHEN 'JLRY_STD' THEN jioa.attribute88 ELSE NULL END as IOC99_PROD_CODE4,
jioa.last_updated_by as LAST_UPDATED_BY,
jioa.distribution1 as DISTRIBUTION1,
jioa.distribution2 as DISTRIBUTION2,
jioa.CONTEXT as CONTEXT,
jioa.ATTRIBUTE78 as ATTRIBUTE78,
jioa.ATTRIBUTE82 as ATTRIBUTE82,
jioa.ATTRIBUTE83 as ATTRIBUTE83,
 '"' || REPLACE (jioa.ATTRIBUTE126, '"', '""') || '"' as ATTRIBUTE126,
 jioa.ATTRIBUTE138 as ATTRIBUTE138,
jioa.ATTRIBUTE146 as ATTRIBUTE146,
jioa.ATTRIBUTE147 as ATTRIBUTE147,
SUBSTR (CASE jioa.context WHEN 'JLRY_STD' THEN jioa.ATTRIBUTE1 WHEN 'CHAMP_RING' THEN jioa.ATTRIBUTE1 WHEN 'JEWL' THEN jioa.ATTRIBUTE38 WHEN 'JEWL_CAN_SCAN' THEN jioa.ATTRIBUTE38 WHEN 'IOC99' THEN jioa.ATTRIBUTE38 ELSE NULL END, 1, 80) AS STONE_COLOR,
CASE CASE jioa.context WHEN 'JLRY_STD' THEN jioa.ATTRIBUTE45 WHEN 'CHAMP_RING' THEN jioa.ATTRIBUTE45 ELSE NULL END WHEN 'Y' THEN 'Y'ELSE NULL END AS ALLOW_PRICING_ERR,
SUBSTR (CASE CASE jioa.context WHEN 'JLRY_STD' THEN jioa.ATTRIBUTE45 WHEN 'CHAMP_RING' THEN jioa.ATTRIBUTE45 ELSE NULL END WHEN 'Y' THEN CASE jioa.context WHEN 'JLRY_STD' THEN jioa.ATTRIBUTE46 WHEN 'CHAMP_RING' THEN jioa.attribute46 ELSE NULL END ELSE NULL END, 1, 150) AS ALLOW_PRICING_ERR_REASON,
CASE jioa.context WHEN 'JLRY_STD' THEN SUBSTR (jioa.ATTRIBUTE106, 1, 2) ELSE NULL END AS TOP_CORE_FONT,
 CASE jioa.context WHEN 'AN_SPR08' THEN SUBSTR (jioa.attribute28, 1, 3) WHEN
          'AN_SPR18' THEN SUBSTR (jioa.attribute28, 1, 3) WHEN
          'ANNC_SCAN_WEB' THEN SUBSTR (jioa.attribute28, 1, 3) WHEN
          'BULK' THEN SUBSTR (jioa.attribute4, 1, 3) WHEN
          'CCG003'THEN SUBSTR (jioa.attribute1, 1, 3) WHEN
          'CCG008' THEN SUBSTR (jioa.attribute5, 1, 3) WHEN
          'CG100'THEN SUBSTR (jioa.attribute5, 1, 3) WHEN
          'CG120'THEN SUBSTR (jioa.attribute3, 1, 3) WHEN
          'CG95'THEN SUBSTR (jioa.attribute6, 1, 3) WHEN
          'CGANNC'THEN SUBSTR (jioa.attribute4, 1, 3) WHEN
          'IOC99' THEN NULL WHEN
          'STUDENT_GREG' THEN SUBSTR (jioa.attribute3, 1, 3) ELSE
          CASE SUBSTR (jioa.context, 1, 7) WHEN
                   'ANNC_SY' THEN SUBSTR (jioa.attribute28, 1, 3) ELSE
                   NULL END END AS HEIGHT,
                   CASE
          jioa.context WHEN
          'AN_SPR08' THEN SUBSTR (jioa.attribute27, 1, 3) WHEN
          'AN_SPR18' THEN SUBSTR (jioa.attribute27, 1, 3) WHEN
          'ANNC_SCAN_WEB' THEN SUBSTR (jioa.attribute27, 1, 3) WHEN
          'BULK' THEN NULL WHEN
          'CCG003' THEN SUBSTR (jioa.attribute2, 1, 3) WHEN
          'CCG008' THEN SUBSTR (jioa.attribute4, 1, 3) WHEN
          'CG100' THEN SUBSTR (jioa.attribute4, 1, 3) WHEN
          'CG120' THEN SUBSTR (jioa.attribute4, 1, 3) WHEN
          'CG95' THEN SUBSTR (jioa.attribute5, 1, 3) WHEN
          'CGANNC' THEN SUBSTR (jioa.attribute3, 1, 3) WHEN
          'IOC99' THEN NULL WHEN
          'STUDENT_GREG' THEN SUBSTR (jioa.attribute4, 1, 3) ELSE
          CASE SUBSTR (jioa.context, 1, 7) WHEN
                   'ANNC_SY' THEN SUBSTR (jioa.attribute27, 1, 3) ELSE
                   NULL END END AS WEIGHT,
CASE
          jioa.context WHEN
          'AN_SPR08' THEN SUBSTR (jioa.attribute71, 1, 1) WHEN
          'AN_SPR18' THEN SUBSTR (jioa.attribute71, 1, 1) WHEN
          'ANNC_SCAN_WEB' THEN SUBSTR (jioa.attribute71, 1, 1) WHEN
          'BULK' THEN SUBSTR (jioa.attribute71, 1, 1) WHEN
          'CCG003' THEN NULL WHEN
          'CCG008' THEN SUBSTR (jioa.attribute71, 1, 1) WHEN
          'CG100' THEN SUBSTR (jioa.attribute71, 1, 1) WHEN
          'CG120' THEN SUBSTR (jioa.attribute71, 1, 1) WHEN
          'CG95' THEN SUBSTR (jioa.attribute71, 1, 1) WHEN
          'CGANNC' THEN SUBSTR (jioa.attribute71, 1, 1) WHEN
          'IOC99' THEN SUBSTR (jioa.attribute1, 1, 1) WHEN
          'STUDENT_GREG' THEN SUBSTR (jioa.attribute71, 1, 1)ELSE
          CASE SUBSTR (jioa.context, 1, 7) WHEN
                   'ANNC_SY' THEN SUBSTR (jioa.attribute71, 1, 1) ELSE
                   NULL END END AS GENDER,
CASE
          jioa.context WHEN
          'AN_SPR08' THEN SUBSTR (jioa.attribute113, 1, 12) WHEN
          'AN_SPR18' THEN SUBSTR (jioa.attribute113, 1, 12) WHEN
          'ANNC_SCAN_WEB' THEN SUBSTR (jioa.attribute30, 1, 12) WHEN
          'BULK' THEN SUBSTR (jioa.attribute6, 1, 12) WHEN
          'CCG003' THEN NULL WHEN
          'CCG008' THEN SUBSTR (jioa.attribute7, 1, 12) WHEN
          'CG100' THEN SUBSTR (jioa.attribute7, 1, 12) WHEN
          'CG120' THEN SUBSTR (jioa.attribute7, 1, 12) WHEN
          'CG95' THEN SUBSTR (jioa.attribute8, 1, 12) WHEN
          'CGANNC' THEN SUBSTR (jioa.attribute5, 1, 12) WHEN
          'IOC99' THEN NULL WHEN
          'STUDENT_GREG' THEN SUBSTR (jioa.attribute7, 1, 12) ELSE
          CASE SUBSTR (jioa.context, 1, 7) WHEN
                   'ANNC_SY' THEN SUBSTR (jioa.attribute113, 1, 12) ELSE
                   NULL END END AS CAP_SIZE,
CASE
          jioa.context WHEN
          'AN_SPR08' THEN SUBSTR (jioa.attribute69, 1, 1) WHEN
          'AN_SPR18' THEN SUBSTR (jioa.attribute69, 1, 1) WHEN
          'ANNC_SCAN_WEB' THEN SUBSTR (jioa.attribute29, 1, 1) WHEN
          'BULK' THEN NULL WHEN
          'CCG003' THEN NULL WHEN
          'CCG008' THEN SUBSTR (jioa.attribute3, 1, 1) WHEN
          'CG100' THEN NULL WHEN
          'CG120' THEN SUBSTR (jioa.attribute5, 1, 1) WHEN
          'CG95' THEN NULL WHEN
          'CGANNC' THEN NULL WHEN
          'IOC99' THEN NULL WHEN
          'STUDENT_GREG' THEN SUBSTR (jioa.attribute5, 1, 1) ELSE
          CASE SUBSTR (jioa.context, 1, 7) WHEN
                   'ANNC_SY' THEN SUBSTR (jioa.attribute29, 1, 1) ELSE
                   NULL END END  AS SPECIAL_GWN_SIZES,
 CASE jioa.context WHEN 'JLRY_STD' THEN SUBSTR (jioa.attribute73, 1, 4) ELSE NULL END AS ENGRAVE_OPTION_CODE,
 CASE jioa.context WHEN 'JLRY_STD' THEN SUBSTR (jioa.attribute96, 1, 30) ELSE NULL END AS JLRY_DEFAULT_GROUP,
CASE
          jioa.context WHEN
          'ANNC_URBAN' THEN SUBSTR (jioa.attribute117, 1, 1) ELSE
          CASE SUBSTR (jioa.context, 1, 7) WHEN
                   'ANNC_SY' THEN SUBSTR (jioa.attribute117, 1, 1) ELSE
                   NULL END END AS DROPSHIP_REP_OVERRIDE_FLAG,
jioa.attribute120 AS DESIGN_ID,                                      /* design_id */
jioa.attribute249 AS ORDERED_BY_NAME,                                  /*ordered_by_name*/
jioa.attribute203 AS ORIGINAL_WEB_QUOTE                                           /* WQD */               

                                  
FROM rawdb.joe_indv_orders_all jioa
INNER JOIN rawdb.joe_order_headers_all joha
ON jioa.order_header_id = joha.order_header_id
LEFT JOIN rawdb.joe_service_orders jso
ON joha.service_order_id = jso.service_order_id
LEFT JOIN rawdb.jfn_common_lookups jcl_sale
ON     jcl_sale.lookup_code = jioa.salesrep_tx_type
AND jcl_sale.lookup_type = 'SALESREP_TX_TYPES'
LEFT JOIN rawdb.jfn_common_lookups jcl_ship
ON     jcl_ship.lookup_code = jioa.ship_method_code
AND jcl_ship.lookup_type = 'FREIGHT_VENDORS'
LEFT JOIN
(SELECT indv_order_id,
context    AS context_value,
attribute2 AS grad_pres_date,
attribute3 AS using_contemporary_dateline
FROM rawdb.joe_indv_orders_all
WHERE coalesce (
                  ORG_ID,
                  coalesce (
                     cast (
                   CASE '103' WHEN ' ' THEN NULL ELSE '103' END as bigint),
                     -99)) =
                  coalesce (
                     cast (
                    CASE '103' WHEN ' ' THEN NULL ELSE '103' END as bigint),
                     -99)) jiodfv
ON jiodfv.indv_order_id = jioa.indv_order_id
                     LIMIT 5""" 
       
print (sql1) 
df = sqlContext.sql(sql1) 
df.count() 
df.show(10) 

### Save DF to S3: 
df.write.mode("overwrite").parquet("s3://jostens-data-dev-analysis/indv_order/") 
### Read From S3: 
df_check = spark.read.parquet("s3://jostens-data-dev-analysis/indev_order/") 
df_check.count() 
df_check.show(10) 