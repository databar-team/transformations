--code below illustrates alternative for three fields: ship_to_contact, sold_to_contact and bill_to_contact
SELECT jso.service_order_id,
       jso.description,
       jso.service_order_type,
       jso.assignment_type,
       NVL (jrs.salesrep_number, -1)                              SALES_REP_NUMBER,
       hca.account_number,
       jso.order_group,
       jso.event,
       jcl_event.meaning                                          EVENT_DESC,
       jso.so_program,
       s_program.description,
       jso.status,
       jso.reason,
       REPLACE (SUBSTR (jso.comments, 1, 750), CHR (9), CHR (32)) AS COMMENTS,
       jso.sales_year,
       su.ship_to_site_use_id,
       su.sold_to_site_use_id,
       su.bill_to_site_use_id,
       --         (SELECT    last_name
       --                 || DECODE (first_name, NULL, NULL, ', ' || first_name)
       --            FROM ra_contacts
       --           WHERE contact_id(+) = su.ship_to_contact_id)
       --            SHIP_TO_CONTACT,
       (SELECT    last_name
               || DECODE (first_name, NULL, NULL, ', ' || first_name)
          FROM (SELECT ACCT_ROLE.CUST_ACCOUNT_ROLE_ID           AS CONTACT_ID,
                       SUBSTRB (PARTY.PERSON_LAST_NAME, 1, 50)  AS last_name,
                       SUBSTRB (PARTY.PERSON_FIRST_NAME, 1, 40) AS first_name
                  FROM ar.HZ_CUST_ACCOUNT_ROLES ACCT_ROLE,
                       ar.HZ_PARTIES            PARTY,
                       ar.HZ_RELATIONSHIPS      REL,
                       ar.HZ_ORG_CONTACTS       ORG_CONT,
                       ar.HZ_PARTIES            REL_PARTY,
                       ar.HZ_CUST_ACCOUNTS      ROLE_ACCT
                 WHERE     ACCT_ROLE.PARTY_ID = REL.PARTY_ID
                       AND ACCT_ROLE.ROLE_TYPE = 'CONTACT'
                       AND ORG_CONT.PARTY_RELATIONSHIP_ID =
                              REL.RELATIONSHIP_ID
                       AND REL.SUBJECT_ID = PARTY.PARTY_ID
                       AND REL.PARTY_ID = REL_PARTY.PARTY_ID
                       AND REL.SUBJECT_TABLE_NAME = 'HZ_PARTIES'
                       AND REL.OBJECT_TABLE_NAME = 'HZ_PARTIES'
                       AND ACCT_ROLE.CUST_ACCOUNT_ID =
                              ROLE_ACCT.CUST_ACCOUNT_ID
                       AND ROLE_ACCT.PARTY_ID = REL.OBJECT_ID)
         WHERE contact_id(+) = su.ship_to_contact_id)
          SHIP_TO_CONTACT,
          sold_to_contact.last_name
       || DECODE (sold_to_contact.first_name,
                  NULL, NULL,
                  ', ' || sold_to_contact.first_name)
          sold_to_contact,
          bill_to_contact.last_name
       || DECODE (bill_to_contact.first_name,
                  NULL, NULL,
                  ', ' || bill_to_contact.first_name)
          bill_to_contact,
       jso.purchase_order_num,
       jso.copied_from_service_order_id,
       jso.orig_system_source,
       jso.orig_system_reference,
       jso.assigned_customer_service_team,
       jcl_team.meaning
          CUSTOMER_SERVICE_TEAM_NAME,
       jso.assigned_customer_service_rep,
       fu.user_name,
       TO_CHAR (TRUNC (jso.start_date_active), 'YYYY-MM-DD')
          start_date_active,
       TO_CHAR (TRUNC (jso.end_date_active), 'YYYY-MM-DD')
          end_date_active,
       oa.name                            /* this is really the agreement # */
              ,
       oa.agreement_num                            /* really the agree name */
                       ,
       oa.agreement_type_code,
       ol.meaning
          AGREEMENT_TYPE_DESC,
       opl.name
          AGREEMENT_PRICE_LIST,
       rt.name
          AGREEMENT_TERMS,
       TO_CHAR (TRUNC (oa.start_date_active), 'YYYY-MM-DD')
          AGREEMENT_START_DATE_ACTIVE,
       TO_CHAR (TRUNC (oa.end_date_active), 'YYYY-MM-DD')
          AGREEMENT_END_DATE_ACTIVE,
       jso.product_offering_usage,
       jso.pricing_method,
       SUBSTR (jso.special_instructions, 1, 2000)
          SPECIAL_INSTRUCTIONS,
       jcl_pack.meaning
          PACKING_METHOD,
       --* replace tabs with spaces
       REPLACE (jso.packing_instructions, CHR (9), CHR (32))
          packing_instructions,
       jso.ship_method_code,
       jcl_ship.meaning
          SHIP_METHOD_DESC,
       REPLACE (jso.shipping_instructions, CHR (9), CHR (32))
          shipping_instructions,
       jso.freight_terms_code,
       jso.invoice_frequency,
       REPLACE (jso.invoice_comments, CHR (9), CHR (32))
          invoice_comments,
       jso.commission_program_code,
       jso.primary_service_order_flag,
       jso.annual_roll_flag,
       jso.order_line_summarization_flag,
       jso.warehouse_id,
       jso.consolidated_ship_warehouse_id,
       CASE WHEN jso.context = 'RECG' THEN jsojd.attribute1 /*tape_frequencies*/
                                                           ELSE NULL END
          AS RECG_TAPE_FREQUENCIES,
       CASE WHEN jso.context = 'RECG' THEN jsojd.attribute2 /*solicit_frequencies*/
                                                           ELSE NULL END
          AS RECG_SOLICIT_FREQUENCIES,
       CASE WHEN jso.context = 'RECG' THEN jsojd.attribute3 /*ship_frequencies*/
                                                           ELSE NULL END
          AS RECG_SHIP_FREQUENCIES,
       CASE WHEN jso.context = 'RECG' THEN jsojd.attribute4    /*down_select*/
                                                           ELSE NULL END
          AS RECG_DOWN_SELECT,
       CASE
          WHEN jso.context = 'RECG'
          THEN
             CASE
                WHEN jsojd.attribute5                        /*award_wo_logo*/
                                     = 'NO' THEN 'N'
                WHEN jsojd.attribute5 = 'YES' THEN 'Y'
                ELSE jsojd.attribute5
             END
          ELSE
             NULL
       END
          AS RECG_AWARD_WO_LOGO,
       CASE WHEN jso.context = 'RECG' THEN jsojd.attribute6 /*dedicated_800_number*/
                                                           ELSE NULL END
          AS RECG_DEDICATED_800_NUMBER,
       CASE WHEN jso.context = 'RECG' THEN jsojd.attribute7     /*fax_number*/
                                                           ELSE NULL END
          AS RECG_FAX_NUMBER,
       CASE WHEN jso.context = 'RECG' THEN jsojd.attribute8     /*ivr_number*/
                                                           ELSE NULL END
          AS RECG_IVR_NUMBER,
       CASE WHEN jso.context = 'RECG' THEN jsojd.attribute9       /*web_site*/
                                                           ELSE NULL END
          AS RECG_WEB_SITE,
       CASE WHEN jso.context = 'RECG' THEN jsojd.attribute10           /*crm*/
                                                            ELSE NULL END
          AS RECG_CRM,
       CASE
          WHEN jso.order_group IN ('COMM', 'DIPL')
          THEN
             CASE
                WHEN INSTR (jso.service_order_type, '-') > 0
                THEN
                   SUBSTR (jso.service_order_type,
                           1,
                           (INSTR (jso.service_order_type, '-') - 1))
                ELSE
                   jso.service_order_type
             END
          ELSE
             NULL
       END
          SERVICE_ORDER_TYPE1,
       CASE
          WHEN jso.order_group IN ('COMM', 'DIPL')
          THEN
             CASE
                WHEN INSTR (jso.service_order_type, '-') > 0
                THEN
                   SUBSTR (jso.service_order_type,
                           (INSTR (jso.service_order_type, '-') + 1))
                ELSE
                   NULL
             END
          ELSE
             NULL
       END
          SERVICE_ORDER_TYPE2,
       jso.salesrep_tx_type,
       fndu.user_name,
       TO_CHAR (jso.creation_date, 'YYYY-MM-DD')
          AS CREATION_DATE,
       so_creation.user_name,
       so_creation.creation_date,
       jso.price_builder_code,
       CASE WHEN jso.context = 'DIPL' THEN jsojd.attribute10 /*rolled_from_service_order*/
                                                            ELSE NULL END
          AS DIPL_ROLLED_FROM_SERVICE_ORDER,
       CASE WHEN jso.context = 'DIPL' THEN jsojd.attribute11      /*GROUPING*/
                                                            ELSE NULL END
          AS DIPL_GROUPING,
       CASE WHEN jso.context = 'YBMS' THEN jsojd.attribute1     /*job_number*/
                                                           ELSE NULL END
          YBMS_JOB_NUMBER,
       CASE WHEN jso.context = 'YBMS' THEN jsojd.attribute2  /*yearbook_year*/
                                                           ELSE NULL END
          YBMS_YEARBOOK_YEAR,
       CASE WHEN jso.context = 'YBMS' THEN jsojd.attribute3          /*plant*/
                                                           ELSE NULL END
          YBMS_PLANT,
       CASE WHEN jso.context = 'YBMS' THEN jsojd.attribute4        /*program*/
                                                           ELSE NULL END
          YBMS_PROGRAM,
       CASE WHEN jso.context = 'YBMS' THEN jsojd.attribute5      /*trim_size*/
                                                           ELSE NULL END
          YBMS_TRIM_SIZE,
       CASE WHEN jso.context = 'YBMS' THEN jsojd.attribute6          /*pages*/
                                                           ELSE NULL END
          YBMS_PAGES,
       CASE WHEN jso.context = 'YBMS' THEN jsojd.attribute7         /*copies*/
                                                           ELSE NULL END
          YBMS_COPIES,
       CASE WHEN jso.context = 'YBMS' THEN jsojd.attribute10 /*yeartech_online*/
                                                            ELSE NULL END
          YBMS_YEARTECH_ONLINE,
       CASE WHEN jso.context = 'YBMS' THEN jsojd.attribute11 /*contract_ship_date*/
                                                            ELSE NULL END
          YBMS_CONTRACT_SHIP_DATE,
       DECODE (jso.context, 'ANNC', SUBSTR (jsojd.ATTRIBUTE2 /*web_catalog_flag*/
                                                            , 1, 1), NULL)
          AS ANNC_WEB_CATALOG_FLAG,
       UPPER (
          REPLACE (
             CASE
                WHEN jso.context = 'DIPL' THEN jso.attribute9
                WHEN jso.context = 'GREG' THEN jso.attribute10
                ELSE NULL
             END,
             CHR (10),
             CHR (32)))
          GT_PO_NUMBER_REQUIRED,
       REPLACE (jso.attribute15, CHR (10), '')
          REGALIA_EMAIL_ADDRESS,
       UPPER (
          REPLACE (
             CASE
                WHEN jso.context = 'DIPL' THEN jso.attribute14
                WHEN jso.context = 'GREG' THEN jso.attribute11
                ELSE NULL
             END,
             CHR (10),
             CHR (32)))
          CONTACT_NAME,
       CASE
          WHEN jso.context = 'DIPL'
          THEN
             REPLACE (jso.attribute15, CHR (10), '')
          WHEN jso.context = 'GREG'
          THEN
             REPLACE (jso.attribute15, CHR (10), '')
          ELSE
             NULL
       END
          CONTACT_EMAIL_ADDRESS,
       UPPER (
          REPLACE (
             CASE
                WHEN jso.context = 'DIPL' THEN jso.attribute16
                WHEN jso.context = 'GREG' THEN jso.attribute14
                ELSE NULL
             END,
             CHR (10),
             CHR (32)))
          AS CONTACT_PHONE,
       UPPER (
          REPLACE (
             CASE
                WHEN jso.context = 'DIPL' THEN jso.attribute17
                ELSE NULL
             END,
             CHR (10),
             CHR (32)))
          AS CONTACT_FAX,
       DECODE (jso.context, 'ANNC', jso.attribute5)
          AS SCHEDULING_OFFSET_DAYS,
       CASE
          WHEN jso.context = 'DIPL' THEN jso.attribute12
          WHEN jso.context = 'GREG' THEN jso.attribute12
          ELSE NULL
       END
          HOMESHIP_FLAG,
       jso.context,
       jso.attribute1,
       jso.attribute2,
       jso.attribute3,
       jso.attribute4,
       jso.attribute5,
       jso.attribute6,
       jso.attribute7,
       jso.attribute8,
       jso.attribute9,
       jso.attribute10,
       jso.attribute11,
       jso.attribute12,
       jso.attribute13,
       jso.attribute14,
       REPLACE (jso.attribute15, CHR (10), '')                    ATTRIBUTE15,
       jso.attribute16,
       jso.attribute17,
       jso.attribute18,
       jso.attribute19,
       jso.attribute20,
       jso.attribute21,
       jso.attribute22,
       jso.attribute23,
       jso.attribute24,
       jso.attribute25,
       jso.attribute26,
       jso.attribute27,
       jso.attribute28,
       jso.attribute29,
       jso.attribute30
  FROM joe_service_orders jso
       INNER JOIN joe_service_orders jsojd
          ON jso.service_order_id = jsojd.service_order_id
       LEFT JOIN
       (  SELECT service_order_id,
                 MAX (DECODE (site_use_code, 'BILL_TO', site_use_id, NULL))
                    bill_to_site_use_id,
                 MAX (DECODE (site_use_code, 'SHIP_TO', site_use_id, NULL))
                    ship_to_site_use_id,
                 MAX (DECODE (site_use_code, 'SOLD_TO', site_use_id, NULL))
                    sold_to_site_use_id,
                 MAX (DECODE (site_use_code, 'BILL_TO', contact_id, NULL))
                    bill_to_contact_id,
                 MAX (DECODE (site_use_code, 'SHIP_TO', contact_id, NULL))
                    ship_to_contact_id,
                 MAX (DECODE (site_use_code, 'SOLD_TO', contact_id, NULL))
                    sold_to_contact_id,
                 MAX (last_update_date) max_last_update_date
            FROM joe_so_addresses
           WHERE site_use_code IN ('BILL_TO', 'SOLD_TO', 'SHIP_TO')
        GROUP BY service_order_id) su
          ON jso.service_order_id = su.service_order_id
       LEFT JOIN
       (  SELECT jsa.service_order_id,
                 MAX (com1_salesrep_id) max_salesrep_id,
                 MAX (GREATEST (jtaa.last_update_date, jtaa.start_date))
                    max_last_update_date
            FROM jfi_territory_assignments_all jtaa,
                 joe_service_orders          jso,
                 joe_so_addresses            jsa
           WHERE     jso.service_order_id = jsa.service_order_id
                 AND jsa.site_use_code = 'SOLD_TO'
                 AND jso.customer_id = jtaa.customer_id
                 AND jso.assignment_type = jtaa.assignment_type
                 AND TRUNC (SYSDATE) BETWEEN jtaa.start_date AND jtaa.end_date
                 AND jsa.site_use_id = jtaa.site_use_id
        GROUP BY jsa.service_order_id) rep
          ON jso.service_order_id = rep.service_order_id
       LEFT JOIN
       (SELECT B.AGREEMENT_ID,
               B.AGREEMENT_NUM,
               B.AGREEMENT_TYPE_CODE,
               B.PRICE_LIST_ID,
               B.TERM_ID,
               B.START_DATE_ACTIVE,
               B.END_DATE_ACTIVE,
               T.NAME,
               B.LAST_UPDATE_DATE
          FROM apps.OE_AGREEMENTS_TL T, apps.OE_AGREEMENTS_B B
         WHERE     B.AGREEMENT_ID = T.AGREEMENT_ID
               AND T.LANGUAGE = USERENV ('LANG')) oa
          ON jso.agreement_id = oa.agreement_id
       LEFT JOIN hz_cust_accounts hca
          ON jso.customer_id = hca.cust_account_id
       LEFT JOIN fnd_user fu
          ON jso.assigned_customer_service_rep = fu.user_id
       LEFT JOIN jfn_common_lookups jcl_event
          ON     jcl_event.lookup_code = jso.event
             AND jcl_event.lookup_type =
                    'SERVICE_ORDER_EVENTS_' || jso.order_group
       LEFT JOIN jfn_common_lookups jcl_team
          ON     jcl_team.lookup_code = jso.assigned_customer_service_team
             AND jcl_team.lookup_type =
                    'CUSTOMER_SERVICE_TEAMS_' || jso.order_group
       LEFT JOIN oe_lookups ol
          ON     ol.lookup_code = oa.agreement_type_code
             AND ol.lookup_type = 'AGREEMENT_TYPE'
       LEFT JOIN oe_price_lists opl ON opl.price_list_id = oa.price_list_id
       LEFT JOIN jfn_common_lookups jcl_ship
          ON     jcl_ship.lookup_code = jso.ship_method_code
             AND jcl_ship.lookup_type = 'FREIGHT_VENDORS'
       LEFT JOIN jfn_common_lookups jcl_pack
          ON     jcl_pack.lookup_code = jso.packing_method
             AND jcl_pack.lookup_type = 'PACK_METHOD'
       LEFT JOIN jtf_rs_salesreps jrs
          ON jrs.salesrep_id = rep.max_salesrep_id
       LEFT JOIN
       (SELECT B.TERM_ID, T.NAME
          FROM RA_TERMS_TL T, RA_TERMS_B B
         WHERE B.TERM_ID = T.TERM_ID AND T.LANGUAGE = USERENV ('LANG')) rt
          ON rt.term_id = oa.term_id
       LEFT JOIN fnd_flex_values_vl s_program
          ON     s_program.flex_value = jso.so_program
             AND s_program.flex_value_set_id = 1002715     --flex_value_set_id
       LEFT JOIN fnd_user fndu ON fndu.user_id = jso.created_by
       LEFT JOIN
       (SELECT DISTINCT
               jsoh.service_order_id, /*use distinct because service_order_id can have exact same create date in joe_so_status_history*/
               fndu1.user_name,
               TO_CHAR (jsoh.creation_date, 'YYYY-MM-DD') creation_date
          FROM fnd_user fndu1, joe_so_status_history jsoh
         WHERE     jsoh.status = 'ACTIVE'
               AND jsoh.creation_date =
                      (SELECT MIN (jsoh1.creation_date)
                         FROM joe_so_status_history jsoh1
                        WHERE     jsoh1.service_order_id =
                                     jsoh.service_order_id
                              AND status = 'ACTIVE')
               AND fndu1.user_id(+) = jsoh.created_by) so_creation
          ON so_creation.service_order_id = jso.service_order_id
       LEFT JOIN
       (SELECT ACCT_ROLE.CUST_ACCOUNT_ROLE_ID           AS CONTACT_ID,
               SUBSTRB (PARTY.PERSON_LAST_NAME, 1, 50)  AS last_name,
               SUBSTRB (PARTY.PERSON_FIRST_NAME, 1, 40) AS first_name
          FROM ar.HZ_CUST_ACCOUNT_ROLES ACCT_ROLE,
               ar.HZ_PARTIES            PARTY,
               ar.HZ_RELATIONSHIPS      REL,
               ar.HZ_ORG_CONTACTS       ORG_CONT,
               ar.HZ_PARTIES            REL_PARTY,
               ar.HZ_CUST_ACCOUNTS      ROLE_ACCT
         WHERE     ACCT_ROLE.PARTY_ID = REL.PARTY_ID
               AND ACCT_ROLE.ROLE_TYPE = 'CONTACT'
               AND ORG_CONT.PARTY_RELATIONSHIP_ID = REL.RELATIONSHIP_ID
               AND REL.SUBJECT_ID = PARTY.PARTY_ID
               AND REL.PARTY_ID = REL_PARTY.PARTY_ID
               AND REL.SUBJECT_TABLE_NAME = 'HZ_PARTIES'
               AND REL.OBJECT_TABLE_NAME = 'HZ_PARTIES'
               AND ACCT_ROLE.CUST_ACCOUNT_ID = ROLE_ACCT.CUST_ACCOUNT_ID
               AND ROLE_ACCT.PARTY_ID = REL.OBJECT_ID) sold_to_contact
          ON sold_to_contact.contact_id = su.sold_to_contact_id
       LEFT JOIN
       (SELECT ACCT_ROLE.CUST_ACCOUNT_ROLE_ID           AS CONTACT_ID,
               SUBSTRB (PARTY.PERSON_LAST_NAME, 1, 50)  AS last_name,
               SUBSTRB (PARTY.PERSON_FIRST_NAME, 1, 40) AS first_name
          FROM ar.HZ_CUST_ACCOUNT_ROLES ACCT_ROLE,
               ar.HZ_PARTIES            PARTY,
               ar.HZ_RELATIONSHIPS      REL,
               ar.HZ_ORG_CONTACTS       ORG_CONT,
               ar.HZ_PARTIES            REL_PARTY,
               ar.HZ_CUST_ACCOUNTS      ROLE_ACCT
         WHERE     ACCT_ROLE.PARTY_ID = REL.PARTY_ID
               AND ACCT_ROLE.ROLE_TYPE = 'CONTACT'
               AND ORG_CONT.PARTY_RELATIONSHIP_ID = REL.RELATIONSHIP_ID
               AND REL.SUBJECT_ID = PARTY.PARTY_ID
               AND REL.PARTY_ID = REL_PARTY.PARTY_ID
               AND REL.SUBJECT_TABLE_NAME = 'HZ_PARTIES'
               AND REL.OBJECT_TABLE_NAME = 'HZ_PARTIES'
               AND ACCT_ROLE.CUST_ACCOUNT_ID = ROLE_ACCT.CUST_ACCOUNT_ID
               AND ROLE_ACCT.PARTY_ID = REL.OBJECT_ID) bill_to_contact
          ON bill_to_contact.contact_id = su.bill_to_contact_id
 WHERE jso.service_order_id = 3037918
