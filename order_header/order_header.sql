SELECT joha.order_header_id,
       jrs.salesrep_number,
       hca.account_number,
       joha.service_order_id,
       joha.oracle_order_number,
       TO_CHAR (joha.received_date, 'DD-MON-YYYY'),
       TO_CHAR (joha.requested_delivery_date, 'DD-MON-YYYY'),
       TO_CHAR (joha.requested_ship_date, 'DD-MON-YYYY'),
       TO_CHAR (joha.creation_date, 'DD-MON-YYYY HH24:MI:SS'),
       NULL                                          /* order_imported_date */
           ,
       NULL                                          /* duration            */
           ,
       joha.order_group,
       ott.name,
       joha.order_class,
       joha.order_source,
       joha.status,
       joha.status_reason,
       joha.prepaid_flag,
       joha.sales_channel_code,
       jcl_cst.meaning,
       joha.return_number,
       joha.creation_code,
       joha.creation_reason,
       joha.collection_doc_replace_flag,
       -- cr#23709
       joha.collection_doc_exclude_flag,
       -- new addition 9/12/08 cr#14587
       joha.shipment_priority_code,
       -- WHO
       DECODE (joha.context, 'GREG', joha.attribute1, NULL),
       -- ORDER NUMBER
       DECODE (joha.context, 'GREG', joha.attribute2, NULL),
       -- WHAT_WAS_ORDERED
       DECODE (joha.context, 'GREG', joha.attribute3, NULL),
       -- WHAT_WAS_RECEIVED
       DECODE (joha.context, 'GREG', joha.attribute4, NULL),
       -- YB Job Number
       DECODE (joha.context, 'GREG', joha.attribute5, NULL),
       -- YB Job Year
       DECODE (joha.context, 'GREG', joha.attribute6, NULL),
       -- YB Juns Number
       DECODE (joha.context, 'GREG', joha.attribute7, NULL),
       -- YB Adviser Name
       DECODE (joha.context, 'GREG', joha.attribute8, NULL),
       -- YB Customer Telephone
       DECODE (joha.context, 'GREG', joha.attribute9, NULL),
       -- YB Plant Code
       DECODE (joha.context, 'GREG', joha.attribute10, NULL),
       -- YB JDS Vendor
       DECODE (joha.context, 'GREG', joha.attribute11, NULL),
       -- Order Receipt Method
       DECODE (joha.context,
               'GREG', joha.attribute13,
               'DIPL', joha.attribute13,
               NULL),
       joha.salesrep_tx_type,
       -- Add Order
       DECODE (joha.context, 'ANNC', joha.attribute1, NULL),
       -- Closed Date
       DECODE (joha.context, 'ANNC', joha.attribute2, NULL),
       -- WHAT_WAS_ORDERED
       DECODE (joha.context, 'ANNC', joha.attribute7, NULL),
       -- WHAT_WAS_RECEIVED
       DECODE (joha.context, 'ANNC', joha.attribute8, NULL),
       -- WHO
       DECODE (joha.context, 'DIPL', joha.attribute1, NULL),
       -- ORDER NUMBER
       DECODE (joha.context, 'DIPL', joha.attribute2, NULL),
       -- WHAT_WAS_ORDERED
       DECODE (joha.context, 'DIPL', joha.attribute3, NULL),
       -- WHAT_WAS_RECEIVED
       DECODE (joha.context, 'DIPL', joha.attribute4, NULL),
       -- ORDERED BY NAME
       DECODE (joha.context,
               'DIPL', joha.attribute14,
               'GREG', joha.attribute17,
               NULL),
       -- ORDERED BY PHONE
       DECODE (joha.context,
               'DIPL', joha.attribute15,
               'GREG', joha.attribute18,
               NULL),
       -- ORDERED BY EMAIL
       DECODE (joha.context,
               'DIPL', joha.attribute16,
               'GREG', joha.attribute19,
               NULL),
       joha.combined_order_header_id,
       joha.combined_order_flag,
       fndu.user_name,
       TO_CHAR (joha.creation_date, 'DD-MON-YYYY HH24:MI:SS'),
       REPLACE (joha.comments, CHR (31), NULL),
       -- WHO
       DECODE (joha.context, 'LFXP', joha.attribute1, NULL),
       -- ORDER NUMBER
       DECODE (joha.context, 'LFXP', joha.attribute2, NULL),
       -- WHAT_WAS_ORDERED
       DECODE (joha.context, 'LFXP', joha.attribute3, NULL),
       -- WHAT_WAS_RECEIVED
       DECODE (joha.context, 'LFXP', joha.attribute4, NULL),
       joha.copied_from_order_header_id,
       -- BO GRAD DATE
       DECODE (joha.context, 'DIPL', joha.attribute5, NULL),
       -- GRAD YEAR **Removed.  No longer being used.
       NULL,
       DECODE (
          joha.context,
          'SAPC', SUBSTR (joha.attribute2,
                          1,
                          (INSTR (joha.attribute2, '-') - 1)),
          NULL),
       DECODE (
          joha.context,
          'SAPC', SUBSTR (joha.attribute2,
                          (INSTR (joha.attribute2, '-') + 1),
                          LENGTH (joha.attribute2)),
          NULL),
       REPLACE (joha.source_order_reference, CHR (31), ''),
       DECODE (joha.context,                                -- YBMS Job Number
                            'YBMS', joha.attribute1, NULL),
       DECODE (joha.context,                             -- YBMS Yearbook Year
                            'YBMS', joha.attribute2, NULL),
       DECODE (joha.context,                                     -- YBMS Plant
                            'YBMS', joha.attribute3, NULL),
       DECODE (joha.context,                                   -- YBMS Program
                            'YBMS', joha.attribute4, NULL),
       DECODE (joha.context,                                 -- YBMS Trim Size
                            'YBMS', joha.attribute5, NULL),
       DECODE (joha.context,                                     -- YBMS Pages
                            'YBMS', joha.attribute6, NULL),
       DECODE (joha.context,                                    -- YBMS Copies
                            'YBMS', joha.attribute7, NULL),
       DECODE (joha.context,                      -- YBMS Large Label Quantity
                            'YBMS', joha.attribute8, NULL),
       DECODE (joha.context,                      -- YBMS Small Label Quantity
                            'YBMS', joha.attribute9, NULL),
       DECODE (joha.context,                             -- YBMS YTO Indicator
                            'YBMS', joha.attribute10, NULL),
       DECODE (joha.context,                          -- YBMS Service Order Id
                            'YBMS', joha.attribute11, NULL),
       DECODE (joha.context,                        -- YBMS MF Kit Reqest Date
                            'YBMS', joha.attribute12, NULL),
       DECODE (joha.context,                            -- YBMS Commission Pct
                            'YBMS', joha.attribute13, NULL),
       DECODE (joha.context,                                  -- Yeartech Code
                            'YBMS', joha.attribute14, NULL),
       DECODE (joha.context,                              -- OPS Received Date
                            'DIPL', joha.attribute6, NULL),
       REPLACE (
          REPLACE (REPLACE (UPPER (joha.mfg_instructions), CHR (10), NULL),
                   CHR (13),
                   NULL),
          CHR (3),
          NULL),
       DECODE (joha.context, 'GREG', joha.attribute20, NULL),
       -- GREG_GRAD_TYPE_DATE
       DECODE (joha.context, 'DIPL', joha.attribute11       /*submitter_type*/
                                                     , NULL), --CR:23614 11Jul2011
       NVL (jsr.attribute3, ' '),
       TO_CHAR (joha.LAST_UPDATE_DATE, 'DD-MON-YYYY'),
       DECODE (joha.context, 'JLRY', joha.attribute18, NULL),
       joha.purchase_order_number,
       DECODE (joha.context,
               'JLRY', joha.attribute17,
               'ANNC', joha.attribute17,
               NULL),
       SUBSTR (
          DECODE (
             joha.attribute17,
             'Y', DECODE (joha.context,
                          'JLRY', joha.attribute22,
                          'ANNC', joha.attribute22,
                          NULL),
             NULL),
          1,
          49),
       DECODE (joha.context,
               'JLRY', joha.attribute3,
               'JEWL', joha.attribute3,
               NULL),
       DECODE (joha.context, 'OWMS', joha.attribute12, NULL),      --OWMS_TEAM
       DECODE (joha.context, 'OWMS', joha.attribute13, NULL), --OWMS_ORDER_CATEGORY
       DECODE (joha.context, 'OWMS', joha.attribute14, NULL), --OWMS_ORDER_TYPE
       DECODE (joha.context, 'OWMS', joha.attribute15, NULL),   --OWMS_JOB_NUM
       DECODE (joha.context, 'OWMS', joha.attribute16, NULL), --OWMS_STUFF_DATE
       DECODE (joha.context, 'OWMS', joha.attribute17, NULL),   --OWMS_CD_CODE
       DECODE (
          joha.context,
          'OWMS', (SELECT HCA.ACCOUNT_NUMBER
                     FROM AR.HZ_CUST_ACCOUNTS HCA
                    WHERE HCA.CUST_ACCOUNT_ID(+) =
                             TO_NUMBER (joha.ATTRIBUTE18)),
          NULL),                                               --OWMS_CUST_NUM
       DECODE (joha.context, 'OWMS', joha.attribute20, NULL), --OWMS_ORDER_INFO
       joha.merge_eligible_flag,
       DECODE (joha.order_group, 'ANNC', joha.attribute18, NULL) --COMBINED_NDPD_IND
  FROM joe_order_headers_all joha
       LEFT JOIN fnd_user fndu ON fndu.user_id = joha.created_by
       LEFT JOIN jfn_common_lookups jcl_cst
          ON     joha.customer_service_team = jcl_cst.lookup_code
             AND 'CUSTOMER_SERVICE_TEAMS_' || joha.order_group =
                    jcl_cst.lookup_type
       LEFT JOIN jfn_common_lookups jcl_sc
          ON     joha.sales_channel_code = jcl_sc.lookup_code
             AND 'SALES_CHANNEL' = jcl_sc.lookup_type
       LEFT JOIN oe_transaction_types_all ota
          ON joha.order_type_id = ota.transaction_type_id
       LEFT JOIN oe_transaction_types_tl ott
          ON     ota.transaction_type_id = ott.transaction_type_id
             AND 'US' = ott.language
       LEFT JOIN jtf_rs_salesreps jrs
          ON joha.salesrep_id = jrs.salesrep_id AND joha.org_id = jrs.org_id
       LEFT JOIN hz_cust_accounts hca
          ON joha.customer_id = hca.cust_account_id
       LEFT JOIN joe_so_reports jsr
          ON     joha.service_order_id = jsr.service_order_id -- Added new by madishs
             AND jsr.report_name = 'MFG_PROC'
 
