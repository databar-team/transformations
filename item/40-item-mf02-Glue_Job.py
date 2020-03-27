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
        sql = """--=========================================================
-- Notes re this transformation:
	--Primary Key is descriptor_ID and segment_name
	--loads into a table name EDW_ITEMS_DESCRIPTORS
	--"friendly" column names follow as column alias
--=========================================================

SELECT jd.inventory_item_id as inventory_item_id,
       msi.segment1                                 AS item_number,
       t_struct.id_flex_structure_name                 AS structure_name,
       fifsm.segment_name as segment_name,
       case fifsm.application_column_name WHEN
               'SEGMENT2' THEN jd.segment2 WHEN
               'SEGMENT3' THEN jd.segment3 WHEN
               'SEGMENT4' THEN jd.segment4 WHEN
               'SEGMENT5' THEN jd.segment5 WHEN
               'SEGMENT6' THEN jd.segment6 WHEN
               'SEGMENT7' THEN jd.segment7 WHEN
               'SEGMENT8' THEN jd.segment8 WHEN
               'SEGMENT9' THEN jd.segment9 WHEN
               'SEGMENT10' THEN jd.segment10 WHEN
               'SEGMENT11' THEN jd.segment11 WHEN
               'SEGMENT12' THEN jd.segment12 WHEN
               'SEGMENT13' THEN jd.segment13 WHEN
               'SEGMENT14' THEN jd.segment14 WHEN
               'SEGMENT15' THEN jd.segment15 WHEN
               'SEGMENT16' THEN jd.segment16 WHEN
               'SEGMENT17' THEN jd.segment17 WHEN
               'SEGMENT18' THEN jd.segment18 WHEN
               'SEGMENT19' THEN jd.segment19 WHEN
               'SEGMENT20' THEN jd.segment20 WHEN
               'SEGMENT21' THEN jd.segment21 WHEN
               'SEGMENT22' THEN jd.segment22 WHEN
               'SEGMENT23' THEN jd.segment23 WHEN
               'SEGMENT24' THEN jd.segment24 WHEN
               'SEGMENT25' THEN jd.segment25 WHEN
               'SEGMENT26' THEN jd.segment26 WHEN
               'SEGMENT27' THEN jd.segment27 WHEN
               'SEGMENT28' THEN jd.segment28 WHEN
               'SEGMENT29' THEN jd.segment29 WHEN
               'SEGMENT30' THEN jd.segment30 WHEN
               'SEGMENT31' THEN jd.segment31 WHEN
               'SEGMENT32' THEN jd.segment32 WHEN
               'SEGMENT33' THEN jd.segment33 WHEN
               'SEGMENT34' THEN jd.segment34 WHEN
               'SEGMENT35' THEN jd.segment35 WHEN
               'SEGMENT36' THEN jd.segment36 WHEN
               'SEGMENT37' THEN jd.segment37 WHEN
               'SEGMENT38' THEN jd.segment38 WHEN
               'SEGMENT39' THEN jd.segment39 WHEN
               'SEGMENT40' THEN jd.segment40 WHEN
               'SEGMENT41' THEN jd.segment41 WHEN
               'SEGMENT42' THEN jd.segment42 WHEN
               'SEGMENT43' THEN jd.segment43 WHEN
               'SEGMENT44' THEN jd.segment44 WHEN
               'SEGMENT45' THEN jd.segment45 WHEN
               'SEGMENT46' THEN jd.segment46 WHEN
               'SEGMENT47' THEN jd.segment47 WHEN
               'SEGMENT48' THEN jd.segment48 WHEN
               'SEGMENT49' THEN jd.segment49 WHEN
               'SEGMENT50'THEN jd.segment50 ELSE
               NULL END   AS segment_value,
       SUBSTR (fifsm.application_column_name, 8, 2) segment_number,
       jd.descriptor_id as descriptor_id
  FROM rawdb.FND_ID_FLEX_SEGMENTS_TL T_seg
       INNER JOIN rawdb.FND_ID_FLEX_SEGMENTS fifsm
          ON     fifsm.APPLICATION_ID = T_seg.APPLICATION_ID
             AND fifsm.ID_FLEX_CODE = T_seg.ID_FLEX_CODE
             AND fifsm.ID_FLEX_NUM = T_seg.ID_FLEX_NUM
             AND fifsm.APPLICATION_COLUMN_NAME =
                    T_seg.APPLICATION_COLUMN_NAME
       INNER JOIN rawdb.FND_ID_FLEX_STRUCTURES fifst
          ON     fifsm.application_id = fifst.application_id
             AND fifsm.id_flex_code = fifst.id_flex_code
             AND fifsm.id_flex_num = fifst.id_flex_num
       INNER JOIN rawdb.FND_ID_FLEX_STRUCTURES_TL T_struct
          ON     fifst.APPLICATION_ID = T_struct.APPLICATION_ID
             AND fifst.ID_FLEX_CODE = T_struct.ID_FLEX_CODE
             AND fifst.ID_FLEX_NUM = T_struct.ID_FLEX_NUM
       INNER JOIN rawdb.FND_APPLICATION fa
          ON fifst.application_id = fa.application_id
       INNER JOIN rawdb.FND_APPLICATION_TL fat
          ON fa.APPLICATION_ID = fat.APPLICATION_ID
       INNER JOIN rawdb.jmf_descriptors jd ON jd.item_type_id = fifst.id_flex_num
       LEFT JOIN
       (SELECT b.INVENTORY_ITEM_ID, b.ORGANIZATION_ID, b.SEGMENT1
          FROM rawdb.MTL_SYSTEM_ITEMS_TL T
               INNER JOIN rawdb.MTL_SYSTEM_ITEMS_B B
                  ON     B.INVENTORY_ITEM_ID = T.INVENTORY_ITEM_ID
                     AND B.ORGANIZATION_ID = T.ORGANIZATION_ID
         WHERE T.LANGUAGE = 'US') msi
          ON jd.inventory_item_id = msi.inventory_item_id
 WHERE     
       fa.application_short_name = 'XXJMF'                   -- CR R12EDW.
       AND fifst.id_flex_code = 'JDES'
       AND fifsm.application_column_name <> 'SEGMENT1'
       AND fifst.enabled_flag = 'Y'
       AND (msi.organization_id = 121 OR msi.organization_id IS NULL) -- Master Org
       AND T_seg.LANGUAGE = 'US'
       AND t_struct.LANGUAGE = 'US'
       AND fat.LANGUAGE = 'US'""" 
        df = sqlContext.sql(sql)
        writePath = "s3://jostens-data-{}-analysis/{}/".format(envPrefix,table)
        print ('writing to {}'.format(writePath))
        df.write.mode("overwrite").parquet(writePath)
except Exception as e:
    exceptionMessage = 'Exception occurred in {} ==> {}'.format(jobName, e)
    print("Glue ETL Exception Message: {}".format(exceptionMessage))
    raise e   
