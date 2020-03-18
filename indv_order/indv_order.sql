SELECT jioa.indv_order_id,
       jioa.order_header_id,
       joha.service_order_id,
       jso.service_order_type,
       joha.order_group,
       jioa.consumer_id,
       jioa.distribution_number,
       jioa.early_delivery_list_status,
       jioa.external_bill_to_system_ref,
       jioa.external_ship_to_system_ref,
       jioa.order_form_id,
--       jof.form_name,
--       jof.form_type,
       item_validation_source,
       TO_CHAR (TRUNC (jioa.ordered_date), 'DD-MON-YYYY'),
       jioa.org_id,
       jioa.profile_id,
       jioa.salesrep_tx_type,
       SUBSTR (jcl_sale.meaning, 1, 30),
       jioa.ship_method_code,
       jcl_ship.meaning,
       jioa.ship_to_io_address_flag,
       jioa.source_balance_due,
       jioa.source_batch_name,
       jioa.source_form_name,
       jioa.source_indv_order_reference,
       jioa.source_input_operator_id,
       jioa.source_order_reference,
       jioa.source_paid_amount,
       jioa.source_system_name,
       jioa.source_total_sell_price,
       jioa.source_total_tax_amount,
       jioa.source_transact_reference,
       jioa.status,
       jioa.status_reason,
       jioa.use_profile_flag,
       '"' || REPLACE (jioa.salesrep_notes, '"', '""') || '"',
       jioa.creation_date,
       '"' || REPLACE (jioa.comments, '"', '""') || '"',
       SUBSTR (
          DECODE (jiodfv.context_value, 'DIPL', jiodfv.grad_pres_date, NULL),
          1,
          150),
       SUBSTR (
          DECODE (jiodfv.context_value,
                  'DIPL', jiodfv.using_contemporary_dateline,
                  NULL),
          1,
          80),
       jioa.attribute39,
       SUBSTR (DECODE (jioa.context, 'LFXP_REPAIR', jioa.attribute29, NULL),
               1,
               50),
       jioa.combined_indv_order_id,
       SUBSTR (DECODE (jioa.context, 'JEWL', jioa.attribute9, NULL), 1, 5),
       CASE
          WHEN jioa.context = 'ANNC_SY04'
          THEN
             CASE WHEN jioa.attribute65 = '1' THEN 'Y' ELSE 'N' END
          ELSE
             NULL
       END,
       CASE
          WHEN jioa.context = 'PGA' THEN SUBSTR (jioa.attribute67, 1, 20)
          ELSE NULL
       END,
       CASE
          WHEN jioa.context = 'IOC99' THEN SUBSTR (jioa.attribute55, 1, 30)
          ELSE NULL
       END,
       DECODE (jioa.context, 'JLRY_STD', jioa.attribute94, NULL),
       DECODE (jioa.context, 'JLRY_STD', jioa.attribute90, NULL),
       DECODE (jioa.context, 'JLRY_STD', jioa.attribute113, NULL),
       DECODE (jioa.context, 'JLRY_STD', jioa.attribute72, NULL),
       DECODE (jioa.context, 'JLRY_STD', jioa.attribute93, NULL),
       DECODE (jioa.context, 'JLRY_STD', jioa.attribute150, NULL),
       DECODE (jioa.context, 'JLRY_STD', jioa.attribute114, NULL),
       DECODE (jioa.context, 'JLRY_STD', jioa.attribute85, NULL),
       DECODE (jioa.context, 'JLRY_STD', jioa.attribute86, NULL),
       DECODE (jioa.context, 'JLRY_STD', jioa.attribute87, NULL),
       DECODE (jioa.context, 'JLRY_STD', jioa.attribute88, NULL),
--       DECODE (jioa.context, 'JLRY_STD', jipp.payment_plan_name, NULL),
       jioa.last_updated_by,
       jioa.distribution1,
       jioa.distribution2,
       jioa.CONTEXT,
       jioa.ATTRIBUTE78,
       jioa.ATTRIBUTE82,
       jioa.ATTRIBUTE83,
       '"' || REPLACE (jioa.ATTRIBUTE126, '"', '""') || '"',
       jioa.ATTRIBUTE138,
       jioa.ATTRIBUTE146,
       jioa.ATTRIBUTE147,
       SUBSTR (
          DECODE (jioa.context,
                  'JLRY_STD', jioa.ATTRIBUTE1,
                  'CHAMP_RING', jioa.ATTRIBUTE1,
                  'JEWL', jioa.ATTRIBUTE38,
                  'JEWL_CAN_SCAN', jioa.ATTRIBUTE38,
                  'IOC99', jioa.ATTRIBUTE38,
                  NULL),
          1,
          80)                                                /* Stone Color */
             ,
       DECODE (
          DECODE (jioa.context,
                  'JLRY_STD', jioa.ATTRIBUTE45,
                  'CHAMP_RING', jioa.ATTRIBUTE45,
                  NULL),
          'Y', 'Y',
          NULL)                                        /* Allow pricing err */
               ,
       SUBSTR (
          DECODE (
             DECODE (jioa.context,
                     'JLRY_STD', jioa.ATTRIBUTE45,
                     'CHAMP_RING', jioa.ATTRIBUTE45,
                     NULL),
             'Y', DECODE (jioa.context,
                          'JLRY_STD', jioa.ATTRIBUTE46,
                          'CHAMP_RING', jioa.attribute46,
                          NULL),
             NULL),
          1,
          150)                                  /* Allow pricing err reason */
              ,
       DECODE (jioa.context,
               'JLRY_STD', SUBSTR (jioa.ATTRIBUTE106, 1, 2),
               NULL),
       DECODE (
          jioa.context,
          'AN_SPR08', SUBSTR (jioa.attribute28, 1, 3),
          'AN_SPR18', SUBSTR (jioa.attribute28, 1, 3),
          'ANNC_SCAN_WEB', SUBSTR (jioa.attribute28, 1, 3),
          'BULK', SUBSTR (jioa.attribute4, 1, 3),
          'CCG003', SUBSTR (jioa.attribute1, 1, 3),
          'CCG008', SUBSTR (jioa.attribute5, 1, 3),
          'CG100', SUBSTR (jioa.attribute5, 1, 3),
          'CG120', SUBSTR (jioa.attribute3, 1, 3),
          'CG95', SUBSTR (jioa.attribute6, 1, 3),
          'CGANNC', SUBSTR (jioa.attribute4, 1, 3),
          'IOC99', NULL,
          'STUDENT_GREG', SUBSTR (jioa.attribute3, 1, 3),
          (DECODE (SUBSTR (jioa.context, 1, 7),
                   'ANNC_SY', SUBSTR (jioa.attribute28, 1, 3),
                   NULL)))                                          /*Height*/
                          ,
       DECODE (
          jioa.context,
          'AN_SPR08', SUBSTR (jioa.attribute27, 1, 3),
          'AN_SPR18', SUBSTR (jioa.attribute27, 1, 3),
          'ANNC_SCAN_WEB', SUBSTR (jioa.attribute27, 1, 3),
          'BULK', NULL,
          'CCG003', SUBSTR (jioa.attribute2, 1, 3),
          'CCG008', SUBSTR (jioa.attribute4, 1, 3),
          'CG100', SUBSTR (jioa.attribute4, 1, 3),
          'CG120', SUBSTR (jioa.attribute4, 1, 3),
          'CG95', SUBSTR (jioa.attribute5, 1, 3),
          'CGANNC', SUBSTR (jioa.attribute3, 1, 3),
          'IOC99', NULL,
          'STUDENT_GREG', SUBSTR (jioa.attribute4, 1, 3),
          (DECODE (SUBSTR (jioa.context, 1, 7),
                   'ANNC_SY', SUBSTR (jioa.attribute27, 1, 3),
                   NULL)))                                          /*Weight*/
                          ,
       DECODE (
          jioa.context,
          'AN_SPR08', SUBSTR (jioa.attribute71, 1, 1),
          'AN_SPR18', SUBSTR (jioa.attribute71, 1, 1),
          'ANNC_SCAN_WEB', SUBSTR (jioa.attribute71, 1, 1),
          'BULK', SUBSTR (jioa.attribute71, 1, 1),
          'CCG003', NULL,
          'CCG008', SUBSTR (jioa.attribute71, 1, 1),
          'CG100', SUBSTR (jioa.attribute71, 1, 1),
          'CG120', SUBSTR (jioa.attribute71, 1, 1),
          'CG95', SUBSTR (jioa.attribute71, 1, 1),
          'CGANNC', SUBSTR (jioa.attribute71, 1, 1),
          'IOC99', SUBSTR (jioa.attribute1, 1, 1),
          'STUDENT_GREG', SUBSTR (jioa.attribute71, 1, 1),
          (DECODE (SUBSTR (jioa.context, 1, 7),
                   'ANNC_SY', SUBSTR (jioa.attribute71, 1, 1),
                   NULL)))                                          /*Gender*/
                          ,
       DECODE (
          jioa.context,
          'AN_SPR08', SUBSTR (jioa.attribute113, 1, 12),
          'AN_SPR18', SUBSTR (jioa.attribute113, 1, 12),
          'ANNC_SCAN_WEB', SUBSTR (jioa.attribute30, 1, 12),
          'BULK', SUBSTR (jioa.attribute6, 1, 12),
          'CCG003', NULL,
          'CCG008', SUBSTR (jioa.attribute7, 1, 12),
          'CG100', SUBSTR (jioa.attribute7, 1, 12),
          'CG120', SUBSTR (jioa.attribute7, 1, 12),
          'CG95', SUBSTR (jioa.attribute8, 1, 12),
          'CGANNC', SUBSTR (jioa.attribute5, 1, 12),
          'IOC99', NULL,
          'STUDENT_GREG', SUBSTR (jioa.attribute7, 1, 12),
          (DECODE (SUBSTR (jioa.context, 1, 7),
                   'ANNC_SY', SUBSTR (jioa.attribute113, 1, 12),
                   NULL)))                                        /*Cap Size*/
                          ,
       DECODE (
          jioa.context,
          'AN_SPR08', SUBSTR (jioa.attribute69, 1, 1),
          'AN_SPR18', SUBSTR (jioa.attribute69, 1, 1),
          'ANNC_SCAN_WEB', SUBSTR (jioa.attribute29, 1, 1),
          'BULK', NULL,
          'CCG003', NULL,
          'CCG008', SUBSTR (jioa.attribute3, 1, 1),
          'CG100', NULL,
          'CG120', SUBSTR (jioa.attribute5, 1, 1),
          'CG95', NULL,
          'CGANNC', NULL,
          'IOC99', NULL,
          'STUDENT_GREG', SUBSTR (jioa.attribute5, 1, 1),
          (DECODE (SUBSTR (jioa.context, 1, 7),
                   'ANNC_SY', SUBSTR (jioa.attribute29, 1, 1),
                   NULL)))                              /*Special Gown Sizes*/
                          ,
       DECODE (jioa.context,
               'JLRY_STD', SUBSTR (jioa.attribute73, 1, 4),
               NULL)                                 /* Engrave Option Code */
                    ,
       DECODE (jioa.context,
               'JLRY_STD', SUBSTR (jioa.attribute96, 1, 30),
               NULL)                                       /* Default Group */
                    ,
       DECODE (
          jioa.context,
          'ANNC_URBAN', SUBSTR (jioa.attribute117, 1, 1),
          (DECODE (SUBSTR (jioa.context, 1, 7),
                   'ANNC_SY', SUBSTR (jioa.attribute117, 1, 1),
                   NULL)))                             /* DropShip SalesRep */
                          ,
       jioa.attribute120,                                      /* design_id */
       jioa.attribute249,                                  /*ordered_by_name*/
       jioa.attribute203                                             /* WQD */
  FROM joe_indv_orders_all jioa
       INNER JOIN joe_order_headers_all joha
          ON jioa.order_header_id = joha.order_header_id
       LEFT JOIN joe_service_orders jso
          ON joha.service_order_id = jso.service_order_id
       LEFT JOIN jfn_common_lookups jcl_sale
          ON     jcl_sale.lookup_code = jioa.salesrep_tx_type
             AND jcl_sale.lookup_type = 'SALESREP_TX_TYPES'
       LEFT JOIN jfn_common_lookups jcl_ship
          ON     jcl_ship.lookup_code = jioa.ship_method_code
             AND jcl_ship.lookup_type = 'FREIGHT_VENDORS'
       --  left join joe_order_forms jof on jioa.order_form_id = jof.order_form_id(+), deferred
       --  left join      joe_indv_payment_plans jipp on jipp.indv_order_id(+) = jioa.indv_order_id , deferred
       --* Outer join is needed due to the DFV is a multi-org view.
       LEFT JOIN
       (SELECT indv_order_id,
               context    AS context_value,
               attribute2 AS grad_pres_date,
               attribute3 AS using_contemporary_dateline
          FROM joe_indv_orders_all
         WHERE NVL (
                  ORG_ID,
                  NVL (
                     TO_NUMBER (
                        DECODE (
                           SUBSTR (USERENV ('CLIENT_INFO'), 1, 1),
                           ' ', NULL,
                           SUBSTR (USERENV ('CLIENT_INFO'), 1, 10))),
                     -99)) =
                  NVL (
                     TO_NUMBER (
                        DECODE (SUBSTR (USERENV ('CLIENT_INFO'), 1, 1),
                                ' ', NULL,
                                SUBSTR (USERENV ('CLIENT_INFO'), 1, 10))),
                     -99)) jiodfv
          ON jiodfv.indv_order_id = jioa.indv_order_id
;
