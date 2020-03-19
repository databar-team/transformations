import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SQLContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import DateType
from datetime import datetime
import os

# Create a Glue context
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
sqlContext = SQLContext(spark)

# mode = "zeppelin"
mode = "glue"

args = getResolvedOptions(sys.argv,['JOB_NAME','env_prefix', 'table'])
jobName = args['JOB_NAME']
envPrefix = args['env_prefix']
table = args['table']

print ("Job: {}".format(jobName))
print ("Environment: {}".format(envPrefix))
print ("Table: {}".format(table))

try:
    
        
        sql = """
          WITH cte_xxxx
AS
(SELECT joha.context
      ,HCA.ACCOUNT_NUMBER
      ,joha.order_header_id
      -- Dodaj kolo da bi se joinovale nazad u queriju:
FROM rawdb.joe_order_headers_all joha
LEFT OUTER JOIN rawdb.HZ_CUST_ACCOUNTS HCA
     ON HCA.CUST_ACCOUNT_ID =
     CASE
           --  WHEN joha.context = 'OWMS' AND joha.ATTRIBUTE18 = '' THEN NULL
            WHEN joha.context = 'OWMS' AND CAST(joha.ATTRIBUTE18 AS BIGINT) IS NOT NULL THEN CAST(joha.ATTRIBUTE18 AS BIGINT)
            ELSE NULL
       END
AND HCA.ACCOUNT_NUMBER IS NOT NULL

)
SELECT 
  joha.order_header_id as BRIDGE_ORDER_NUMBER,
cast(jrs.salesrep_number as bigint) as SALES_REP_NUMBER,
hca.account_number as ORACLE_CUSTOMER_NUMBER,
joha.service_order_id as SERVICE_ORDER_NUMBER,
joha.oracle_order_number as ORACLE_ORDER_NUMBER,
date(date_format(joha.received_date, 'dd-MM-yyyy')) as ORDER_ENTERED_DATE,
date(date_format(joha.requested_delivery_date, 'dd-MM-yyyy')) as ORDER_REQUESTED_DELIVERY_DATE,
date(date_format(joha.requested_ship_date, 'dd-MM-yyyy')) as ORDER_SCHEDULED_SHIP_DATE,
date(date_format(joha.creation_date, 'dd-MM-yyyy HH:mm:ss')) as ORDER_CREATED_DATE,
cast(NULL as DATE) as ORDER_IMPORTED_DATE,
cast(NULL as bigint) as ORDER_CYCLE_DURATION,
joha.order_group as ORDER_GROUP,
ott.name as ORDER_TYPE,
joha.order_class as ORDER_CLASS,
joha.order_source as ORDER_SOURCE,
joha.status as STATUS,
joha.status_reason as STATUS_REASON,
joha.prepaid_flag as PREPAID_IND,
joha.sales_channel_code as SALES_CHANNEL,
jcl_cst.meaning as CUSTOMER_SERVICE_TEAM_NAME,
joha.return_number as RETURN_NUMBER,
joha.creation_code as CREATION_CODE,
joha.creation_reason as CREATION_REASON,
joha.collection_doc_replace_flag as COLLECTION_DOC_REPLACE_FLAG,
joha.collection_doc_exclude_flag as COLLECTION_DOC_EXCLUDE_FLAG,
joha.shipment_priority_code as SHIPMENT_PRIORITY_CODE,
CASE joha.context WHEN 'GREG' THEN joha.attribute1 ELSE NULL END as GREG_WHO,
CASE joha.context WHEN 'GREG' THEN joha.attribute2 ELSE NULL END as GREG_ORDER_NUMBER,
CASE joha.context WHEN 'GREG' THEN joha.attribute3 ELSE NULL END as GREG_WHAT_WAS_ORDERED,
CASE joha.context WHEN 'GREG' THEN joha.attribute4 ELSE NULL END as GREG_WHAT_WAS_RECEIVED,
CASE joha.context WHEN 'GREG' THEN joha.attribute5 ELSE NULL END as YB_JOB_NUMBER,
CASE joha.context WHEN 'GREG' THEN joha.attribute6 ELSE NULL END as YB_FILE_YEAR,
CASE joha.context WHEN 'GREG' THEN joha.attribute7 ELSE NULL END as YB_CUSTOMER_NUMBER,
CASE joha.context WHEN 'GREG' THEN joha.attribute8 ELSE NULL END as YB_ADVISOR_NAME,
CASE joha.context WHEN 'GREG' THEN joha.attribute9 ELSE NULL END as YB_CUSTOMER_PHONE_NUMBER,
CASE joha.context WHEN 'GREG' THEN joha.attribute10 ELSE NULL END as YB_PLANT_CODE,
CASE joha.context WHEN 'GREG' THEN joha.attribute11 ELSE NULL END as YB_JDS_VENDOR,
CASE joha.context WHEN 'GREG' THEN joha.attribute13 when 'DIPL' then joha.attribute13 ELSE NULL END as ORDER_RECEIPT_METHOD,
joha.salesrep_tx_type as SALES_REP_TRANSACTION_TYPE,
CASE joha.context WHEN 'ANNC' THEN joha.attribute1 ELSE NULL END as ANNC_ADD_ORDER,
CASE joha.context WHEN 'ANNC' THEN joha.attribute2 ELSE NULL END as ANNC_CLOSED_DATE,
CASE joha.context WHEN 'ANNC' THEN joha.attribute7 ELSE NULL END as ANNC_WHAT_WAS_ORDERED,
CASE joha.context WHEN 'ANNC' THEN joha.attribute8 ELSE NULL END as ANNC_WHAT_WAS_RECEIVED,
CASE joha.context WHEN 'DIPL' THEN joha.attribute1 ELSE NULL END as DIPL_WHO,
cast(CASE joha.context WHEN 'DIPL' THEN joha.attribute2 ELSE NULL END as bigint) as DIPL_ORDER_NUMBER,
CASE joha.context WHEN 'DIPL' THEN joha.attribute3 ELSE NULL END as DIPL_WHAT_WAS_ORDERED,
CASE joha.context WHEN 'DIPL' THEN joha.attribute4 ELSE NULL END as DIPL_WHAT_WAS_RECEIVED,
CASE joha.context WHEN 'DIPL' THEN joha.attribute14 when 'GREG' then joha.attribute17 ELSE NULL END as ORDERED_BY_NAME,
CASE joha.context WHEN 'DIPL' THEN joha.attribute15 when 'GREG' then joha.attribute18 ELSE NULL END as ORDERED_BY_PHONE,
CASE joha.context WHEN 'DIPL' THEN joha.attribute16 when 'GREG' then joha.attribute19 ELSE NULL END as ORDERED_BY_EMAIL,
joha.combined_order_header_id as COMBINED_BRIDGE_ORDER,
joha.combined_order_flag as COMBINED_ORDER_FLAG,
fndu.user_name as CREATED_BY,
date(date_format(joha.creation_date, 'dd-MM-yyyy HH:mm:ss')) as CREATION_DATE,
joha.comments as COMMENTS,
CASE joha.context WHEN 'LFXP' THEN joha.attribute1 ELSE NULL END as LFXP_WHO,
CASE joha.context WHEN 'LFXP' THEN joha.attribute2 ELSE NULL END as LFXP_ORDER_NUMBER,
CASE joha.context WHEN 'LFXP' THEN joha.attribute3 ELSE NULL END as LFXP_WHAT_WAS_ORDERED,
CASE joha.context WHEN 'LFXP' THEN joha.attribute4 ELSE NULL END as LFXP_WHAT_WAS_RECEIVED,
CASE joha.context WHEN 'LFXP' THEN joha.attribute5 ELSE NULL END as BRIDGE_ORDER_GRAD_DATE,
cast (NULL as DOUBLE) as GRAD_YEAR,
CASE joha.context WHEN 'SAPC' THEN SUBSTR (joha.attribute2, 1, (INSTR (joha.attribute2, '-') - 1)) ELSE NULL END as PHOTOGRAPHER_NUMBER,
CASE joha.context WHEN 'SAPC' THEN SUBSTR (joha.attribute2, (INSTR (joha.attribute2, '-') + 1), LENGTH(joha.attribute2)) ELSE NULL END as PHOTOGRAPHER_NAME,
REPLACE(joha.source_order_reference,CHR(31),'') as SOURCE_ORDER_REFERENCE,
CASE joha.context WHEN 'YBMS' THEN joha.attribute1 ELSE NULL END as YBMS_JOB_NUMBER,
CASE joha.context WHEN 'YBMS' THEN joha.attribute2 ELSE NULL END as YBMS_YEARBOOK_YEAR,
CASE joha.context WHEN 'YBMS' THEN joha.attribute3 ELSE NULL END as YBMS_PLANT,
CASE joha.context WHEN 'YBMS' THEN joha.attribute4 ELSE NULL END as YBMS_PROGRAM,
CASE joha.context WHEN 'YBMS' THEN joha.attribute5 ELSE NULL END as YBMS_TRIM_SIZE,
cast(CASE joha.context WHEN 'YBMS' THEN joha.attribute6 ELSE NULL END as bigint) as YBMS_PAGES,
cast(CASE joha.context WHEN 'YBMS' THEN joha.attribute7 ELSE NULL END as bigint) as YBMS_COPIES,
cast(CASE joha.context WHEN 'YBMS' THEN joha.attribute8 ELSE NULL END as bigint) as YBMS_LARGE_LABEL_QTY,
cast(CASE joha.context WHEN 'YBMS' THEN joha.attribute9 ELSE NULL END as bigint) as YBMS_SMALL_LABEL_QTY,
CASE joha.context WHEN 'YBMS' THEN joha.attribute10 ELSE NULL END as YBMS_YEARTECH_ONLINE,
cast(CASE joha.context WHEN 'YBMS' THEN joha.attribute11 ELSE NULL END as bigint) as YBMS_SERVICE_ORDER_ID,
cast(CASE joha.context WHEN 'YBMS' THEN joha.attribute12 ELSE NULL END as date) as YBMS_MF_KIT_REQUES_DATE,
cast(CASE joha.context WHEN 'YBMS' THEN joha.attribute13 ELSE NULL END as bigint) as YBMS_COMMISSION_PCT,
CASE joha.context WHEN 'YBMS' THEN joha.attribute14 ELSE NULL END as YBMS_YTO_INDICATOR,
cast(CASE joha.context WHEN 'DIPL' THEN joha.attribute6 ELSE NULL END as date) as OP_RECEIVED_DATE,
UPPER (joha.mfg_instructions) as MFG_INSTRUCTIONS,
CASE joha.context WHEN 'GREG' THEN joha.attribute20 ELSE NULL END as GREG_GRAD_TYPE_DATE,
CASE joha.context WHEN 'DIPL' THEN joha.attribute11 ELSE NULL END as SUBMITTER_TYPE,
coalesce(jsr.attribute3, '') as SHOP_HANDLING_CODE,
date(date_format(joha.LAST_UPDATE_DATE, 'dd-MM-yyyy')) as LAST_UPDATE_DATE,
CASE joha.context WHEN 'JLRY' THEN joha.attribute18 ELSE NULL END as SCHEDULE_TYPE,
joha.purchase_order_number as PURCHASE_ORDER_NUMBER,
CASE joha.context WHEN 'JLRY' THEN joha.attribute17 when 'ANNC' then joha.attribute17 ELSE NULL END as ZDP_INDICATOR,
joha.attribute17 as ZDP_STATUS,
CASE joha.context WHEN 'JLRY' THEN joha.attribute3 when 'JEWL' then joha.attribute3 ELSE NULL END as SAMPLE_ORDER_TYPE,
CASE joha.context WHEN 'OWMS' THEN joha.attribute12 ELSE NULL END as OWMS_TEAM,
CASE joha.context WHEN 'OWMS' THEN joha.attribute13 ELSE NULL END as OWMS_ORDER_CATEGORY,
CASE joha.context WHEN 'OWMS' THEN joha.attribute14 ELSE NULL END as OWMS_ORDER_TYPE,
CASE joha.context WHEN 'OWMS' THEN joha.attribute15 ELSE NULL END as OWMS_JOB_NUM,
CASE joha.context WHEN 'OWMS' THEN joha.attribute16 ELSE NULL END as OWMS_STUFF_DATE,
CASE joha.context WHEN 'OWMS' THEN joha.attribute17 ELSE NULL END as OWMS_CD_CODE,

    CASE
    WHEN joha.context = 'OWMS' THEN HCA.ACCOUNT_NUMBER
    ELSE NULL
    END AS OWMS_CUST_NUM,
    CASE joha.context WHEN 'OWMS' THEN joha.attribute20 ELSE NULL END as OWMS_ORDER_INFO,
joha.merge_eligible_flag as MERGE_ELIGIBLE_FLAG,
    CASE joha.order_group WHEN 'ANNC' THEN joha.attribute18 ELSE NULL END as COMBINED_NDPD_IND
  FROM 
    rawdb.joe_order_headers_all joha
       LEFT JOIN rawdb.fnd_user fndu ON fndu.user_id = joha.created_by
       LEFT JOIN rawdb.jfn_common_lookups jcl_cst
          ON     joha.customer_service_team = jcl_cst.lookup_code
             AND 'CUSTOMER_SERVICE_TEAMS_' || joha.order_group =
                    jcl_cst.lookup_type
       LEFT JOIN rawdb.jfn_common_lookups jcl_sc
          ON     joha.sales_channel_code = jcl_sc.lookup_code
             AND 'SALES_CHANNEL' = jcl_sc.lookup_type
       LEFT JOIN rawdb.oe_transaction_types_all ota
          ON joha.order_type_id = ota.transaction_type_id
       LEFT JOIN rawdb.oe_transaction_types_tl ott
          ON     ota.transaction_type_id = ott.transaction_type_id
             AND 'US' = ott.language
       LEFT JOIN rawdb.jtf_rs_salesreps jrs
          ON joha.salesrep_id = jrs.salesrep_id AND joha.org_id = jrs.org_id
       LEFT JOIN rawdb.hz_cust_accounts hca
          ON joha.customer_id = hca.cust_account_id
       LEFT JOIN rawdb.joe_so_reports jsr
          ON     joha.service_order_id = jsr.service_order_id -- Added new by madishs
             AND jsr.report_name = 'MFG_PROC'
       LEFT JOIN cte_xxxx
          on cte_xxxx.order_header_id = joha.order_header_id """ 
        
        df = sqlContext.sql(sql)
        writePath = "s3://jostens-data-{}-analysis/{}/".format(envPrefix,table)
        print ('writing to {}'.format(writePath))
        df.write.mode("overwrite").parquet(writePath)
        
        
except Exception as e:
    exceptionMessage = 'Exception occurred in {} ==> {}'.format(jobName, e)
    print("Glue ETL Exception Message: {}".format(exceptionMessage))
    raise e           