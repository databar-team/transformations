--=========================================================
-- Notes re this transformation:
	--Primary Key is descriptor_ID and segment_name
	--loads into a table name EDW_ITEMS_DESCRIPTORS
	--"friendly" column names follow as column alias
--=========================================================

SELECT jd.inventory_item_id,
       msi.segment1                                 AS item_number,
       t_struct.id_flex_structure_name                 AS structure_name,
       fifsm.segment_name,
       DECODE (fifsm.application_column_name,
               'SEGMENT2', jd.segment2,
               'SEGMENT3', jd.segment3,
               'SEGMENT4', jd.segment4,
               'SEGMENT5', jd.segment5,
               'SEGMENT6', jd.segment6,
               'SEGMENT7', jd.segment7,
               'SEGMENT8', jd.segment8,
               'SEGMENT9', jd.segment9,
               'SEGMENT10', jd.segment10,
               'SEGMENT11', jd.segment11,
               'SEGMENT12', jd.segment12,
               'SEGMENT13', jd.segment13,
               'SEGMENT14', jd.segment14,
               'SEGMENT15', jd.segment15,
               'SEGMENT16', jd.segment16,
               'SEGMENT17', jd.segment17,
               'SEGMENT18', jd.segment18,
               'SEGMENT19', jd.segment19,
               'SEGMENT20', jd.segment20,
               'SEGMENT21', jd.segment21,
               'SEGMENT22', jd.segment22,
               'SEGMENT23', jd.segment23,
               'SEGMENT24', jd.segment24,
               'SEGMENT25', jd.segment25,
               'SEGMENT26', jd.segment26,
               'SEGMENT27', jd.segment27,
               'SEGMENT28', jd.segment28,
               'SEGMENT29', jd.segment29,
               'SEGMENT30', jd.segment30,
               'SEGMENT31', jd.segment31,
               'SEGMENT32', jd.segment32,
               'SEGMENT33', jd.segment33,
               'SEGMENT34', jd.segment34,
               'SEGMENT35', jd.segment35,
               'SEGMENT36', jd.segment36,
               'SEGMENT37', jd.segment37,
               'SEGMENT38', jd.segment38,
               'SEGMENT39', jd.segment39,
               'SEGMENT40', jd.segment40,
               'SEGMENT41', jd.segment41,
               'SEGMENT42', jd.segment42,
               'SEGMENT43', jd.segment43,
               'SEGMENT44', jd.segment44,
               'SEGMENT45', jd.segment45,
               'SEGMENT46', jd.segment46,
               'SEGMENT47', jd.segment47,
               'SEGMENT48', jd.segment48,
               'SEGMENT49', jd.segment49,
               'SEGMENT50', jd.segment50,
               NULL)
          AS segment_value,
       SUBSTR (fifsm.application_column_name, 8, 2) segment_number,
       jd.descriptor_id
  FROM FND_ID_FLEX_SEGMENTS_TL T_seg
       INNER JOIN FND_ID_FLEX_SEGMENTS fifsm
          ON     fifsm.APPLICATION_ID = T_seg.APPLICATION_ID
             AND fifsm.ID_FLEX_CODE = T_seg.ID_FLEX_CODE
             AND fifsm.ID_FLEX_NUM = T_seg.ID_FLEX_NUM
             AND fifsm.APPLICATION_COLUMN_NAME =
                    T_seg.APPLICATION_COLUMN_NAME
       INNER JOIN FND_ID_FLEX_STRUCTURES fifst
          ON     fifsm.application_id = fifst.application_id
             AND fifsm.id_flex_code = fifst.id_flex_code
             AND fifsm.id_flex_num = fifst.id_flex_num
       INNER JOIN FND_ID_FLEX_STRUCTURES_TL T_struct
          ON     fifst.APPLICATION_ID = T_struct.APPLICATION_ID
             AND fifst.ID_FLEX_CODE = T_struct.ID_FLEX_CODE
             AND fifst.ID_FLEX_NUM = T_struct.ID_FLEX_NUM
       INNER JOIN FND_APPLICATION fa
          ON fifst.application_id = fa.application_id
       INNER JOIN FND_APPLICATION_TL fat
          ON fa.APPLICATION_ID = fat.APPLICATION_ID
       INNER JOIN jmf_descriptors jd ON jd.item_type_id = fifst.id_flex_num
       LEFT JOIN
       (SELECT b.INVENTORY_ITEM_ID, b.ORGANIZATION_ID, b.SEGMENT1
          FROM MTL_SYSTEM_ITEMS_TL T
               INNER JOIN MTL_SYSTEM_ITEMS_B B
                  ON     B.INVENTORY_ITEM_ID = T.INVENTORY_ITEM_ID
                     AND B.ORGANIZATION_ID = T.ORGANIZATION_ID
         WHERE T.LANGUAGE = USERENV ('LANG')) msi
          ON jd.inventory_item_id = msi.inventory_item_id
 WHERE     
       AND fa.application_short_name = 'XXJMF'                   -- CR R12EDW.
       AND fifst.id_flex_code = 'JDES'
       AND fifsm.application_column_name <> 'SEGMENT1'
       AND fifst.enabled_flag = 'Y'
       AND (msi.organization_id = 121 OR msi.organization_id IS NULL) -- Master Org
       AND T_seg.LANGUAGE = USERENV ('LANG')
       AND t_struct.LANGUAGE = USERENV ('LANG')
       AND fat.LANGUAGE = USERENV ('LANG');
	   
