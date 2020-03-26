SELECT 
       msi.inventory_item_id                             "INVENTORY_ITEM_ID",
       msi.organization_id                               "ORGANIZATION_ID",
       msi.cost_of_sales_account                         "COGS_CODE_COMBINATION_ID",
       msi.sales_account                                 "SALES_CODE_COMBINATION_ID",
       msi.expense_account                               "EXPENSE_CODE_COMBINATION_ID",
       mp.organization_code                              "ORGANIZATION_CODE",
       hou.name                                          "ORGANIZATION_NAME",
       msi.segment1                                      "ITEM_NUMBER",
       msi.std_lot_size                                  "STD_LOT_SIZE",
       --* delete any newline characters
       REPLACE (msi_tl.description, CHR (10), NULL)      "DESCRIPTION",
       fcl.meaning                                       "ITEM_TYPE",
       msi.inventory_item_status_code                    "ITEM_STATUS",
       msi.unit_weight                                   "UNIT_WEIGHT",
       msi.weight_uom_code                               "WEIGHT_UOM_CODE",
       mc_inv.segment1                                /* Inventory Category */
                      "INVENTORY_CATEGORY",
       mc_pur.segment1                               /* Purchasing Category */
                      "PURCHASING_CATEGORY",
       mc_gl.segment1                                /* GL Product Category */
                     "GL_PRODUCT_CATEGORY",
       mc_pc.segment1                                /* Prod Class Category */
                     "PROD_CLASS_CATEGORY",
       mc_ps.segment1                                  /* Prod Sub Category */
                     "PROD_SUB_CATEGORY",
       mc_pur.segment2                              /* Purchasing Sub Class */
                      "PURCHASING_SUB_CLASS",
       fifs.id_flex_structure_name                       "STRUCTURE_NAME",
       msi.primary_uom_code                              "PRIMARY_UNIT_OF_MEASURE",
       DECODE (msi.attribute2, 'GENERIC ITEM', 'Y', 'N') "GENERIC_FLAG",
       msi.costing_enabled_flag
          "COSTING_ENABLED_FLAG",
       msi.inventory_asset_flag
          "INVENTORY_ASSET_FLAG",
       /* Dropship_ind */
       DECODE (misi.secondary_inventory, NULL, 'N', 'Y') "DROPSHIP_FLAG",
       ml.meaning                                 /* planning_make_buy_code */
                 "PLANNING_MAKE_BUY",
       NVL (cic.material_cost, 0)                        "MATERIAL_COST",
       NVL (cic.resource_cost, 0)                        "RESOURCE_COST",
       NVL (cic.outside_processing_cost, 0)
          "OUTSIDE_PROCESSING_COST",
       NVL (cic.material_overhead_cost, 0)
          "MATERIAL_OVERHEAD_COST",
       NVL (cic.overhead_cost, 0)                        "OVERHEAD_COST",
       NVL (cic.item_cost, 0)                            "ITEM_COST",
       UPPER (REPLACE (jd.invoice_usage_desc, CHR (10), NULL))
          "DESCRIPTOR_INVOICE_USAGE",
       jd.item_type_id                                   "ITEM_TYPE_ID",
       UPPER (msi.shippable_item_flag)
          "SHIPPABLE_ITEM_FLAG",
       CASE msi.inventory_planning_code
          WHEN 2 THEN msi.min_minmax_quantity
          ELSE NULL
       END
          "MIN_MINMAX_QUANTITY",
       CASE msi.inventory_planning_code
          WHEN 2 THEN msi.max_minmax_quantity
          ELSE NULL
       END
          "MAX_MINMAX_QUANTITY",
       msi.lot_control_code                              "LOT_CONTROL_CODE",
       msi.full_lead_time                                "FULL_LEAD_TIME"
  FROM mtl_system_items_b msi
       INNER JOIN mtl_system_items_tl msi_tl
          ON     msi.organization_id = msi_tl.organization_id
             AND msi.inventory_item_id = msi_tl.inventory_item_id
       INNER JOIN fnd_common_lookups fcl ON fcl.lookup_code = msi.item_type
       INNER JOIN mtl_parameters mp
          ON msi.organization_id = mp.organization_id
       INNER JOIN hr_all_organization_units hou
          ON msi.organization_id = hou.organization_id
       LEFT JOIN mtl_item_sub_inventories misi
          ON     msi.organization_id = misi.organization_id
             AND msi.inventory_item_id = misi.inventory_item_id
             AND misi.secondary_inventory = 'FG-DROPSHP'
       LEFT JOIN mtl_item_categories mic_pur
          ON     msi.organization_id = mic_pur.organization_id
             AND msi.inventory_item_id = mic_pur.inventory_item_id
             AND mic_pur.category_set_id = 3 --category_set_name = 'PURCHASING'
       LEFT JOIN mtl_categories_b mc_pur
          ON mic_pur.category_id = mc_pur.category_id
       LEFT JOIN mtl_item_categories mic_gl
          ON     msi.organization_id = mic_gl.organization_id
             AND msi.inventory_item_id = mic_gl.inventory_item_id
             AND mic_gl.category_set_id = 47 --category_set_name = 'GL PRODUCT'
       LEFT JOIN mtl_categories_b mc_gl
          ON mic_gl.category_id = mc_gl.category_id
       LEFT JOIN mtl_item_categories mic_pc
          ON     msi.organization_id = mic_pc.organization_id
             AND msi.inventory_item_id = mic_pc.inventory_item_id
             AND mic_pc.category_set_id = 23 --category_set_name = 'PROD CLASS'
       LEFT JOIN mtl_categories_b mc_pc
          ON mic_pc.category_id = mc_pc.category_id
       LEFT JOIN mtl_item_categories mic_ps
          ON     msi.organization_id = mic_ps.organization_id
             AND msi.inventory_item_id = mic_ps.inventory_item_id
             AND mic_ps.category_set_id = 24  --category_set_name = 'PROD SUB'
       LEFT JOIN mtl_categories_b mc_ps
          ON mic_ps.category_id = mc_ps.category_id
       LEFT JOIN mtl_item_categories mic_inv
          ON     msi.organization_id = mic_inv.organization_id
             AND msi.inventory_item_id = mic_inv.inventory_item_id
             AND mic_inv.category_set_id = 1 -- category_set_name = 'Inventory'
       LEFT JOIN mtl_categories_b mc_inv
          ON mic_inv.category_id = mc_inv.category_id
       LEFT JOIN cst_item_costs cic
          ON     cic.organization_id = msi.organization_id
             AND cic.inventory_item_id = msi.inventory_item_id
             AND cic.cost_type_id = 1                                -- Frozen
       LEFT JOIN jmf_descriptors jd
          ON jd.inventory_item_id = msi.inventory_item_id
       LEFT JOIN fnd_id_flex_structures_vl fifs
          ON     fifs.id_flex_num = jd.item_type_id
             AND fifs.id_flex_code = 'JDES'
             AND fifs.application_id = 20005 --application_short_name = 'XXJMF'
       LEFT JOIN fnd_lookup_values ml
          ON     ml.lookup_code = TO_CHAR (msi.planning_make_buy_code)
             AND ml.lookup_type = 'MTL_PLANNING_MAKE_BUY'
             AND ml.security_group_id = 0
             AND ml.view_application_id = 700
             AND ml.language = 'US'
 WHERE     msi.organization_id <> 121                            -- Master Org
       AND msi_tl.language = 'US'
       AND fcl.lookup_type = 'ITEM_TYPE';
       
       
