WITH cte_ra_contacts as (
   
   SELECT 
         ACCT_ROLE.CUST_ACCOUNT_ROLE_ID      AS CONTACT_ID,
         SUBSTR (PARTY.PERSON_LAST_NAME, 1, 50)  AS last_name,
         SUBSTR (PARTY.PERSON_FIRST_NAME, 1, 40) AS first_name
   FROM 
         rawdb.HZ_CUST_ACCOUNT_ROLES ACCT_ROLE
   INNER JOIN
         rawdb.HZ_RELATIONSHIPS      REL
         ON
         ACCT_ROLE.PARTY_ID = REL.PARTY_ID
   INNER JOIN
         rawdb.HZ_ORG_CONTACTS       ORG_CONT
         on 
         ORG_CONT.PARTY_RELATIONSHIP_ID = REL.RELATIONSHIP_ID
   INNER JOIN 
         rawdb.HZ_PARTIES            PARTY
         on 
         REL.SUBJECT_ID = PARTY.PARTY_ID
   INNER JOIN 
         rawdb.HZ_PARTIES            REL_PARTY
         on 
         REL.PARTY_ID = REL_PARTY.PARTY_ID
   INNER JOIN 
         rawdb.HZ_CUST_ACCOUNTS      ROLE_ACCT
         on 
         ROLE_ACCT.PARTY_ID = REL.OBJECT_ID
         and ACCT_ROLE.CUST_ACCOUNT_ID = ROLE_ACCT.CUST_ACCOUNT_ID
   WHERE     
         ACCT_ROLE.ROLE_TYPE = 'CONTACT'
         AND REL.SUBJECT_TABLE_NAME = 'HZ_PARTIES'
         AND REL.OBJECT_TABLE_NAME = 'HZ_PARTIES'
)

SELECT
   jso.service_order_id as SERVICE_ORDER_NUMBER,
   jso.description as SERVICE_ORDER_DESCRIPTION,
   jso.service_order_type as SERVICE_ORDER_TYPE,
   jso.assignment_type as ASSIGNMENT_TYPE,
   cast(coalesce(jrs.salesrep_number) as bigint) as SALES_REP_NUMBER,
   hca.account_number as ORACLE_CUSTOMER_NUMBER,
   jso.order_group as ORDER_GROUP,
   jso.event as EVENT_CODE,
   jcl_event.meaning as EVENT_DESC,
   jso.so_program as PROGRAM_CODE,
   s_program.description as PROGRAM_DESC,
   jso.status as STATUS,
   jso.reason as REASON,
   REPLACE (SUBSTR (jso.comments, 1, 750), CHR (9), CHR (32)) AS COMMENTS,
   jso.sales_year as SALES_YEAR,
   su.ship_to_site_use_id as SHIP_TO_ADDRESS_ID,
   su.sold_to_site_use_id as SOLD_TO_ADDRESS_ID,
   su.bill_to_site_use_id as BILL_TO_ADDRESS_ID,
    cship_to_contacts.last_name ||
      CASE
         WHEN
            cship_to_contacts.first_name = NULL
         THEN
            NULL 
         ELSE
            (', ' || cship_to_contacts.first_name )
      END AS SHIP_TO_CONTACT,
    
      csold_to_contacts.last_name ||
      CASE
         WHEN
            csold_to_contacts.first_name = NULL
         THEN
            NULL 
         ELSE
            (', ' || csold_to_contacts.first_name )
      END AS SOLD_TO_CONTACT,
 
      cbill_to_contacts.last_name ||
      CASE
         WHEN
            cbill_to_contacts.first_name = NULL
         THEN
            NULL 
         ELSE
            (', ' || cbill_to_contacts.first_name )
      END AS BILL_TO_CONTACT,
      jso.purchase_order_num as PURCHASE_ORDER_NUMBER,
   jso.copied_from_service_order_id as COPY_FROM_SERVICE_ORDER_NUMBER,
   jso.orig_system_source as ORIG_SYSTEM_SOURCE,
   jso.orig_system_reference as ORIG_SYSTEM_REFERENCE,
   jso.assigned_customer_service_team as ASSIGNED_CUSTOMER_SERVICE_TEAM,
   jcl_team.meaning as CUSTOMER_SERVICE_TEAM_NAME,
   jso.assigned_customer_service_rep as ASSIGNED_CUSTOMER_SERVICE_REP,
   fu.user_name as CUSTOMER_SERVICE_REP_NAME,
   jso.start_date_active as start_date_active,
   jso.end_date_active as END_DATE_ACTIVE,
   oa.name as AGREEMENT_NUMBER,
   oa.agreement_num as AGREEMENT_NAME,
   oa.agreement_type_code as AGREEMENT_TYPE_CODE,
   ol.meaning as AGREEMENT_TYPE_DESC,
   opl.name as AGREEMENT_PRICE_LIST,
   rt.name as AGREEMENT_TERMS,
   oa.start_date_active as AGREEMENT_START_DATE_ACTIVE,
   oa.end_date_active as AGREEMENT_END_DATE_ACTIVE,
   jso.product_offering_usage as PRODUCT_OFFERING_USAGE,
   jso.pricing_method as PRICING_METHOD,
   SUBSTR (jso.special_instructions, 1, 2000) as SPECIAL_INSTRUCTIONS,
   jcl_pack.meaning as PACKING_METHOD,
   REPLACE(jso.packing_instructions, CHR(9), CHR(32)) as PACKING_INSTRUCTIONS,
   jso.ship_method_code as SHIP_METHOD_CODE,
   jcl_ship.meaning as SHIP_METHOD_DESC,
   REPLACE (jso.shipping_instructions, CHR (9), CHR (32)) as SHIPPING_INSTRUCTIONS,
   jso.freight_terms_code as FREIGHT_TERMS,
   jso.invoice_frequency as INVOICE_FREQUENCY,
   REPLACE (jso.invoice_comments, CHR (9), CHR (32)) as INVOICE_COMMENTS,
   jso.commission_program_code as COMMISSION_PROGRAM,
   jso.primary_service_order_flag as PRIMARY_SERVICE_ORDER_FLAG,
   jso.annual_roll_flag as ANNUAL_ROLL_FLAG,
   jso.order_line_summarization_flag as ORDER_LINE_SUMMARIZATION_FLAG,
   jso.warehouse_id as WAREHOUSE_ID,
   jso.consolidated_ship_warehouse_id as CONSOLIDATED_SHIP_WAREHOUSE_ID,
   CASE
      WHEN
         jso.context = 'RECG' 
      THEN
         jsojd.attribute1 
      ELSE
         NULL 
   END
   AS RECG_TAPE_FREQUENCIES, 
   CASE
      WHEN
         jso.context = 'RECG' 
      THEN
         jsojd.attribute2 
      ELSE
         NULL 
   END
   AS RECG_SOLICIT_FREQUENCIES, 
   CASE
      WHEN
         jso.context = 'RECG' 
      THEN
         jsojd.attribute3 
      ELSE
         NULL 
   END
   AS RECG_SHIP_FREQUENCIES, 
   CASE
      WHEN
         jso.context = 'RECG' 
      THEN
         jsojd.attribute4 
      ELSE
         NULL 
   END
   AS RECG_DOWN_SELECT, 
   CASE
      WHEN
         jso.context = 'RECG' 
      THEN
         CASE
            WHEN
               jsojd.attribute5 = 'NO' 
            THEN
               'N' 
            WHEN
               jsojd.attribute5 = 'YES' 
            THEN
               'Y' 
            ELSE
               jsojd.attribute5 
         END
         ELSE
            NULL 
   END
   AS RECG_AWARD_WO_LOGO, 
   CASE
      WHEN
         jso.context = 'RECG' 
      THEN
         jsojd.attribute6 
      ELSE
         NULL 
   END
   AS RECG_DEDICATED_800_NUMBER, 	/*dedicated_800_number*/
   CASE
      WHEN
         jso.context = 'RECG' 
      THEN
         jsojd.attribute7 
      ELSE
         NULL 
   END
   AS RECG_FAX_NUMBER, 	/*fax_number*/
   CASE
      WHEN
         jso.context = 'RECG' 
      THEN
         jsojd.attribute8 
      ELSE
         NULL 
   END
   AS RECG_IVR_NUMBER, 	/*ivr_number*/
   CASE
      WHEN
         jso.context = 'RECG' 
      THEN
         jsojd.attribute9 
      ELSE
         NULL 
   END
   AS RECG_WEB_SITE, 	/*web_site*/
   CASE
      WHEN
         jso.context = 'RECG' 
      THEN
         jsojd.attribute10 
      ELSE
         NULL 
   END
   AS RECG_CRM, 	/*crm*/
   CASE WHEN jso.order_group IN ('COMM', 'DIPL') THEN CASE WHEN INSTR (jso.service_order_type, '-') > 0 THEN SUBSTR (jso.service_order_type, 1, (INSTR (jso.service_order_type, '-') - 1)) ELSE jso.service_order_type END ELSE NULL END as SERVICE_ORDER_TYPE1,
CASE WHEN jso.order_group IN ('COMM', 'DIPL') THEN CASE WHEN INSTR (jso.service_order_type, '-') > 0 THEN SUBSTR (jso.service_order_type, (INSTR (jso.service_order_type, '-') + 1)) ELSE NULL END ELSE NULL END AS SERVICE_ORDER_TYPE2,
jso.salesrep_tx_type as SALES_REP_TRANSACTION_TYPE, 
   fndu.user_name as CREATED_BY, 
   jso.creation_date as CREATION_DATE, 
   so_creation.user_name as FIRST_ACTIVATED_BY, 
   so_creation.creation_date as FIRST_ACTIVATION_DATE, 
   jso.price_builder_code as PRICE_BUILDER,
   CASE
      WHEN
         jso.context = 'DIPL' 
      THEN
         jsojd.attribute10 
      ELSE
         NULL 
   END
   AS DIPL_ROLLED_FROM_SERVICE_ORDER, 	/*rolled_from_service_order*/
   CASE
      WHEN
         jso.context = 'DIPL' 
      THEN
         jsojd.attribute11 
      ELSE
         NULL 
   END
   AS DIPL_GROUPING, 	/*GROUPING*/
   CASE
      WHEN
         jso.context = 'YBMS' 
      THEN
         jsojd.attribute1 
      ELSE
         NULL 
   END
   as YBMS_JOB_NUMBER, 	/*job_number*/
   CASE
      WHEN
         jso.context = 'YBMS' 
      THEN
         jsojd.attribute2 
      ELSE
         NULL 
   END
   as YBMS_YEARBOOK_YEAR, 	/*yearbook_year*/
   CASE
      WHEN
         jso.context = 'YBMS' 
      THEN
         jsojd.attribute3 
      ELSE
         NULL 
   END
   as YBMS_PLANT, 	/*plant*/
   CASE
      WHEN
         jso.context = 'YBMS' 
      THEN
         jsojd.attribute4 
      ELSE
         NULL 
   END
   as YBMS_PROGRAM, 	/*program*/
   CASE
      WHEN
         jso.context = 'YBMS' 
      THEN
         jsojd.attribute5 
      ELSE
         NULL 
   END
   AS YBMS_TRIM_SIZE, 	/*trim_size*/
   cast(
   CASE
      WHEN
         jso.context = 'YBMS' 
      THEN
         jsojd.attribute6 
      ELSE
         NULL 
   END
   as bigint) as YBMS_PAGES, 	/*pages*/
   /*pages*/
   CASE
      WHEN
         jso.context = 'YBMS' 
      THEN
         jsojd.attribute7 
      ELSE
         NULL 
   END
   as YBMS_COPIES, 	/*copies*/
   CASE
      WHEN
         jso.context = 'YBMS' 
      THEN
         jsojd.attribute10 
      ELSE
         NULL 
   END
   AS YBMS_YEARTECH_ONLINE, 	/*yeartech_online*/
   CASE
      WHEN
         jso.context = 'YBMS' 
      THEN
         jsojd.attribute11 
      ELSE
         NULL 
   END
   as YBMS_CONTRACT_SHIP_DATE, 	/*contract_ship_date*/
   CASE
      jso.context 
      WHEN
         'ANNC' 
      THEN
         SUBSTR (jsojd.ATTRIBUTE2 , 1, 1) 
      ELSE
         NULL 
   END
   AS ANNC_WEB_CATALOG_FLAG, UPPER (REPLACE (
   CASE
      WHEN
         jso.context = 'DIPL' 
      THEN
         jso.attribute9 
      WHEN
         jso.context = 'GREG' 
      THEN
         jso.attribute10 
      ELSE
         NULL 
   END
, CHR (10), CHR (32))) as GT_PO_NUMBER_REQUIRED,
REPLACE (jso.attribute15, CHR (10), '') as REGALIA_EMAIL_ADDRESS, 
UPPER (REPLACE (
   CASE
      WHEN
         jso.context = 'DIPL' 
      THEN
         jso.attribute14 
      WHEN
         jso.context = 'GREG' 
      THEN
         jso.attribute11 
      ELSE
         NULL 
   END
, CHR (10), CHR (32))) as CONTACT_NAME, 
   CASE
      WHEN
         jso.context = 'DIPL' 
      THEN
         REPLACE (jso.attribute15, CHR (10), '') 
      WHEN
         jso.context = 'GREG' 
      THEN
         REPLACE (jso.attribute15, CHR (10), '') 
      ELSE
         NULL 
   END
   AS CONTACT_EMAIL_ADDRESS, 
   UPPER (REPLACE (
   CASE
      WHEN
         jso.context = 'DIPL' 
      THEN
         jso.attribute16 
      WHEN
         jso.context = 'GREG' 
      THEN
         jso.attribute14 
      ELSE
         NULL 
   END
, CHR (10), CHR (32))) AS CONTACT_PHONE, 
UPPER (REPLACE (
   CASE
      WHEN
         jso.context = 'DIPL' 
      THEN
         jso.attribute17 
      ELSE
         NULL 
   END
, CHR (10), CHR (32))) AS CONTACT_FAX, 
   CASE
      jso.context 
      WHEN
         'ANNC' 
      THEN
         jso.attribute5 
   END
   AS SCHEDULING_OFFSET_DAYS,
   CASE
      WHEN
         jso.context = 'DIPL' 
      THEN
         jso.attribute12 
      WHEN
         jso.context = 'GREG' 
      THEN
         jso.attribute12 
      ELSE
         NULL 
   END
   as HOMESHIP_FLAG, 
   jso.context as CONTEXT,
   jso.attribute1 as ATTRIBUTE1, 
jso.attribute2 as ATTRIBUTE2, 
jso.attribute3 as ATTRIBUTE3, 
jso.attribute4 as ATTRIBUTE4, 
jso.attribute5 as ATTRIBUTE5, 
jso.attribute6 as ATTRIBUTE6, 
jso.attribute7 as ATTRIBUTE7, 
jso.attribute8 as ATTRIBUTE8, 
jso.attribute9 as ATTRIBUTE9, 
jso.attribute10 as ATTRIBUTE10, 
jso.attribute11 as ATTRIBUTE11, 
jso.attribute12 as ATTRIBUTE12, 
jso.attribute13 as ATTRIBUTE13, 
jso.attribute14 as ATTRIBUTE14, 
REPLACE (jso.attribute15, CHR (10), '') as ATTRIBUTE15, 
jso.attribute16 as ATTRIBUTE16, 
jso.attribute17 as ATTRIBUTE17, 
jso.attribute18 as ATTRIBUTE18, 
jso.attribute19 as ATTRIBUTE19, 
jso.attribute20 as ATTRIBUTE20, 
jso.attribute21 as ATTRIBUTE21, 
jso.attribute22 as ATTRIBUTE22, 
jso.attribute23 as ATTRIBUTE23, 
jso.attribute24 as ATTRIBUTE24, 
jso.attribute25 as ATTRIBUTE25, 
jso.attribute26 as ATTRIBUTE26, 
jso.attribute27 as ATTRIBUTE27, 
jso.attribute28 as ATTRIBUTE28, 
jso.attribute29 as ATTRIBUTE29, 
jso.attribute30 as ATTRIBUTE30 

   
   
FROM
   rawdb.joe_service_orders jso 
   INNER JOIN
      rawdb.joe_service_orders jsojd 
      ON jso.service_order_id = jsojd.service_order_id 
   LEFT JOIN
      (
         SELECT
            service_order_id,
            MAX(
            CASE
               site_use_code 
               WHEN
                  'BILL_TO' 
               THEN
                  site_use_id 
               ELSE
                  NULL 
            END
) bill_to_site_use_id, MAX(
            CASE
               site_use_code 
               WHEN
                  'SHIP_TO' 
               THEN
                  site_use_id 
               ELSE
                  NULL 
            END
) ship_to_site_use_id, MAX(
            CASE
               site_use_code 
               WHEN
                  'SOLD_TO' 
               THEN
                  site_use_id 
               ELSE
                  NULL 
            END
) sold_to_site_use_id, MAX(
            CASE
               site_use_code 
               WHEN
                  'BILL_TO' 
               THEN
                  contact_id 
               ELSE
                  NULL 
            END
) bill_to_contact_id, MAX(
            CASE
               site_use_code 
               WHEN
                  'SHIP_TO' 
               THEN
                  contact_id 
               ELSE
                  NULL 
            END
) ship_to_contact_id, MAX(
            CASE
               site_use_code 
               WHEN
                  'SOLD_TO' 
               THEN
                  contact_id 
               ELSE
                  NULL 
            END
) sold_to_contact_id, MAX (last_update_date) max_last_update_date 
         FROM
            rawdb.joe_so_addresses 
         WHERE
            site_use_code IN 
            (
               'BILL_TO', 'SOLD_TO', 'SHIP_TO'
            )
         GROUP BY
            service_order_id
      )
      su 
      ON jso.service_order_id = su.service_order_id 
   LEFT JOIN
      (
         SELECT
            jsa.service_order_id,
            MAX (com1_salesrep_id) max_salesrep_id,
            MAX (GREATEST (jtaa.last_update_date, jtaa.start_date)) max_last_update_date 
         FROM
            rawdb.jfi_territory_assignments_all jtaa 
            inner join
               rawdb.joe_service_orders jso 
               ON jso.assignment_type = jtaa.assignment_type 
               AND jso.customer_id = jtaa.customer_id 
            inner join
               rawdb.joe_so_addresses jsa 
               on jso.service_order_id = jsa.service_order_id 
               AND jsa.site_use_id = jtaa.site_use_id 
         WHERE
            jsa.site_use_code = 'SOLD_TO' 
         GROUP BY
            jsa.service_order_id
      )
      rep 
      ON jso.service_order_id = rep.service_order_id 
   LEFT JOIN
      (
         SELECT
            B.AGREEMENT_ID,
            B.AGREEMENT_NUM,
            B.AGREEMENT_TYPE_CODE,
            B.PRICE_LIST_ID,
            B.TERM_ID,
            B.START_DATE_ACTIVE,
            B.END_DATE_ACTIVE,
            T.NAME,
            B.LAST_UPDATE_DATE 
         FROM
            rawdb.OE_AGREEMENTS_TL T 
            INNER JOIN
               rawdb.OE_AGREEMENTS_B B 
               ON B.AGREEMENT_ID = T.AGREEMENT_ID 
         WHERE
            T.LANGUAGE = 'US'
      )
      oa 
      ON jso.agreement_id = oa.agreement_id 
   LEFT JOIN
      rawdb.hz_cust_accounts hca 
      ON jso.customer_id = hca.cust_account_id 
   LEFT JOIN
      rawdb.fnd_user fu 
      ON jso.assigned_customer_service_rep = fu.user_id 
   LEFT JOIN
      rawdb.jfn_common_lookups jcl_event 
      ON jcl_event.lookup_code = jso.event 
      AND jcl_event.lookup_type = 'SERVICE_ORDER_EVENTS_' || jso.order_group 
   LEFT JOIN
      rawdb.jfn_common_lookups jcl_team 
      ON jcl_team.lookup_code = jso.assigned_customer_service_team 
      AND jcl_team.lookup_type = 'CUSTOMER_SERVICE_TEAMS_' || jso.order_group 
   LEFT JOIN
      rawdb.oe_lookups ol 
      ON ol.lookup_code = oa.agreement_type_code 
      AND ol.lookup_type = 'AGREEMENT_TYPE' 
   LEFT JOIN
      rawdb.oe_price_lists_vl opl 
      ON opl.price_list_id = oa.price_list_id 
   LEFT JOIN
      rawdb.jfn_common_lookups jcl_ship 
      ON jcl_ship.lookup_code = jso.ship_method_code 
      AND jcl_ship.lookup_type = 'FREIGHT_VENDORS' 
   LEFT JOIN
      rawdb.jfn_common_lookups jcl_pack 
      ON jcl_pack.lookup_code = jso.packing_method 
      AND jcl_pack.lookup_type = 'PACK_METHOD' 
   LEFT JOIN
      rawdb.jtf_rs_salesreps jrs 
      ON jrs.salesrep_id = rep.max_salesrep_id 
   LEFT JOIN
      (
         SELECT
            B.TERM_ID,
            T.NAME 
         FROM
            rawdb.RA_TERMS_TL T
         INNER JOIN
            rawdb.RA_TERMS_B B 
         on
            B.TERM_ID = T.TERM_ID 
         
         where T.LANGUAGE = 'US'
      )
      rt 
      ON rt.term_id = oa.term_id 
   LEFT JOIN
      rawdb.fnd_flex_values_vl s_program 
      ON s_program.flex_value = jso.so_program 
      AND s_program.flex_value_set_id = 1002715 		--flex_value_set_id
   LEFT JOIN
      rawdb.fnd_user fndu 
      ON fndu.user_id = jso.created_by 
   

  LEFT JOIN
      (
         SELECT DISTINCT
	jsoh.service_order_id, 
	fndu1.user_name,
     jsoh.creation_date
FROM 
	rawdb.fnd_user fndu1
LEFT join
	rawdb.joe_so_status_history jsoh
ON
	fndu1.user_id = jsoh.created_by
WHERE     
	jsoh.status = 'ACTIVE'
	AND jsoh.creation_date =
	(
			SELECT 
				MIN (jsoh1.creation_date)  
			FROM 
				rawdb.joe_so_status_history jsoh1
			WHERE     
				jsoh1.service_order_id = jsoh.service_order_id
				AND status = 'ACTIVE'
	)
      )
      so_creation 
      ON so_creation.service_order_id = jso.service_order_id 
 
        LEFT JOIN cte_ra_contacts cship_to_contacts
          ON cship_to_contacts.contact_id = su.ship_to_contact_id
       LEFT JOIN cte_ra_contacts csold_to_contacts
          ON csold_to_contacts.contact_id = su.sold_to_contact_id
       LEFT JOIN cte_ra_contacts cbill_to_contacts
          ON cbill_to_contacts.contact_id = su.bill_to_contact_id