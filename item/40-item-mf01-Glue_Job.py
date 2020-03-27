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
        sql = """SELECT
       msi.inventory_item_id  as INVENTORY_ITEM_ID,
       msi.organization_id    as ORGANIZATION_ID,
       msi.cost_of_sales_account  as COGS_CODE_COMBINATION_ID ,
       msi.sales_account   as SALES_CODE_COMBINATION_ID,
       msi.expense_account    as EXPENSE_CODE_COMBINATION_ID,
       mp.organization_code   as ORGANIZATION_CODE,
       hou.name   as ORGANIZATION_NAME,
       msi.segment1   as ITEM_NUMBER,
       msi.std_lot_size    as STD_LOT_SIZE,
       msi_tl.description  as DESCRIPTION,
       fcl.meaning       as ITEM_TYPE,
       msi.inventory_item_status_code   as ITEM_STATUS,
       msi.unit_weight   as UNIT_WEIGHT,
       msi.weight_uom_code    as   WEIGHT_UOM_CODE,
       mc_inv.segment1    as INVENTORY_CATEGORY,
       mc_pur.segment1    as PURCHASING_CATEGORY,
       mc_gl.segment1    as GL_PRODUCT_CATEGORY,
       mc_pc.segment1    as PROD_CLASS_CATEGORY,
       mc_ps.segment1    as PROD_SUB_CATEGORY,
       mc_pur.segment2    as PURCHASING_SUB_CLASS,
       msi.primary_uom_code   as PRIMARY_UNIT_OF_MEASURE,
       CASE msi.attribute2 WHEN 'GENERIC' THEN 'Y' ELSE 'N' END   as GENERIC_FLAG,
       msi.costing_enabled_flag   as COSTING_ENABLED_FLAG,
       msi.inventory_asset_flag   as INVENTORY_ASSET_FLAG,
       CASE misi.secondary_inventory WHEN NULL THEN 'N' ELSE 'Y' END   as  DROPSHIP_FLAG,
       ml.meaning   as   PLANNING_MAKE_BUY,
       coalesce (cic.material_cost, 0) as MATERIAL_COST,
       coalesce (cic.resource_cost, 0) as RESOURCE_COST,
       coalesce (cic.outside_processing_cost, 0) as OUTSIDE_PROCESSING_COST,
       coalesce (cic.material_overhead_cost, 0) as MATERIAL_OVERHEAD_COST,
       coalesce (cic.overhead_cost, 0) as OVERHEAD_COST,
       coalesce (cic.item_cost, 0) as ITEM_COST,
       UPPER (REPLACE (jd.invoice_usage_desc, CHR (10), NULL)) as DESCRIPTOR_INVOICE_USAGE,
       -- upper (jd.invoice_usage_desc),
       jd.item_type_id    ITEM_TYPE_ID,
       UPPER (msi.shippable_item_flag) as SHIPPABLE_ITEM_FLAG,
       -- UPPER (msi.shippable_item_flag) SHIPPABLE_ITEM_FLAG,
       CASE msi.inventory_planning_code WHEN 2 THEN msi.min_minmax_quantity ELSE NULL END as MIN_MINMAX_QUANTITY,
       CASE msi.inventory_planning_code WHEN 2 THEN msi.max_minmax_quantity ELSE NULL END as MAX_MINMAX_QUANTITY,
       msi.lot_control_code  as  LOT_CONTROL_CODE,
       msi.full_lead_time   as   FULL_LEAD_TIME
 
FROM
       rawdb.mtl_system_items_b msi
       INNER JOIN rawdb.mtl_system_items_tl msi_tl ON    
              msi.organization_id = msi_tl.organization_id
            AND msi.inventory_item_id = msi_tl.inventory_item_id
       INNER JOIN rawdb.fnd_common_lookups fcl ON fcl.lookup_code = msi.item_type
       INNER JOIN rawdb.mtl_parameters mp ON msi.organization_id = mp.organization_id
       INNER JOIN rawdb.hr_all_organization_units hou ON msi.organization_id = hou.organization_id
       LEFT JOIN rawdb.mtl_item_sub_inventories misi  ON    
                     msi.organization_id = misi.organization_id
            AND msi.inventory_item_id = misi.inventory_item_id
            AND misi.secondary_inventory = 'FG-DROPSHP'
              LEFT JOIN rawdb.mtl_item_categories mic_pur ON    
                     msi.organization_id = mic_pur.organization_id
             AND msi.inventory_item_id = mic_pur.inventory_item_id
             AND mic_pur.category_set_id = 3 --category_set_name = 'PURCHASING'
              LEFT JOIN rawdb.mtl_categories_b mc_pur ON mic_pur.category_id = mc_pur.category_id
              LEFT JOIN rawdb.mtl_item_categories mic_gl
          ON     msi.organization_id = mic_gl.organization_id
             AND msi.inventory_item_id = mic_gl.inventory_item_id
             AND mic_gl.category_set_id = 47 --category_set_name = 'GL PRODUCT'
              LEFT JOIN rawdb.mtl_categories_b mc_gl ON mic_gl.category_id = mc_gl.category_id
        LEFT JOIN rawdb.mtl_item_categories mic_pc
          ON     msi.organization_id = mic_pc.organization_id
             AND msi.inventory_item_id = mic_pc.inventory_item_id
             AND mic_pc.category_set_id = 23 --category_set_name = 'PROD CLASS'
        LEFT JOIN rawdb.mtl_categories_b mc_pc ON mic_pc.category_id = mc_pc.category_id
        LEFT JOIN rawdb.mtl_item_categories mic_ps
          ON     msi.organization_id = mic_ps.organization_id
             AND msi.inventory_item_id = mic_ps.inventory_item_id
             AND mic_ps.category_set_id = 24  --category_set_name = 'PROD SUB'
        LEFT JOIN rawdb.mtl_categories_b mc_ps ON mic_ps.category_id = mc_ps.category_id
        LEFT JOIN rawdb.mtl_item_categories mic_inv
          ON     msi.organization_id = mic_inv.organization_id
             AND msi.inventory_item_id = mic_inv.inventory_item_id
             AND mic_inv.category_set_id = 1 -- category_set_name = 'Inventory'
        LEFT JOIN rawdb.mtl_categories_b mc_inv ON mic_inv.category_id = mc_inv.category_id
       LEFT JOIN rawdb.cst_item_costs cic
          ON     cic.organization_id = msi.organization_id
             AND cic.inventory_item_id = msi.inventory_item_id
             AND cic.cost_type_id = 1                                -- Frozen
       LEFT JOIN rawdb.jmf_descriptors jd ON jd.inventory_item_id = msi.inventory_item_id
       LEFT JOIN (
              SELECT  
                 B.APPLICATION_ID,
                 B.ID_FLEX_CODE,
                 B.ID_FLEX_NUM,
                 B.ID_FLEX_STRUCTURE_CODE,
                 B.LAST_UPDATE_DATE,
                 B.LAST_UPDATED_BY,
                 B.CREATION_DATE,
                 B.CREATED_BY,
                 B.LAST_UPDATE_LOGIN,
                 B.CONCATENATED_SEGMENT_DELIMITER,
                 B.CROSS_SEGMENT_VALIDATION_FLAG,
                 B.DYNAMIC_INSERTS_ALLOWED_FLAG,
                 B.ENABLED_FLAG,
                 B.FREEZE_FLEX_DEFINITION_FLAG,
                 B.FREEZE_STRUCTURED_HIER_FLAG,
                 B.SHORTHAND_ENABLED_FLAG,
                 B.SHORTHAND_LENGTH,
                 B.STRUCTURE_VIEW_NAME,
                 T.ID_FLEX_STRUCTURE_NAME,
                 T.DESCRIPTION,
                 T.SHORTHAND_PROMPT
              FROM
                rawdb.FND_ID_FLEX_STRUCTURES_TL T, rawdb.FND_ID_FLEX_STRUCTURES B
              WHERE    
                 B.APPLICATION_ID = T.APPLICATION_ID
                 AND B.ID_FLEX_CODE = T.ID_FLEX_CODE
                 AND B.ID_FLEX_NUM = T.ID_FLEX_NUM
        ) as  fifs
          ON     fifs.id_flex_num = jd.item_type_id
             AND fifs.id_flex_code = 'JDES'
             AND fifs.application_id = 20005 --application_short_name = 'XXJMF'
       LEFT JOIN rawdb.fnd_lookup_values ml
          ON     ml.lookup_code = cast(msi.planning_make_buy_code as string)
             AND ml.lookup_type = 'MTL_PLANNING_MAKE_BUY'
             AND ml.security_group_id = 0
             AND ml.view_application_id = 700
             AND ml.language = 'US'
       WHERE    
              msi.organization_id <> 121                            -- Master Org
              AND msi_tl.language = 'US'
              AND fcl.lookup_type = 'ITEM_TYPE'""" 
        df = sqlContext.sql(sql)
        writePath = "s3://jostens-data-{}-analysis/{}/".format(envPrefix,table)
        print ('writing to {}'.format(writePath))
        df.write.mode("overwrite").parquet(writePath)
except Exception as e:
    exceptionMessage = 'Exception occurred in {} ==> {}'.format(jobName, e)
    print("Glue ETL Exception Message: {}".format(exceptionMessage))
    raise e   
