--=========================================================
-- Notes re this transformation:
	--Primary Key is descriptor_ID and segment_name
	--loads into a table name EDW_ITEMS_DESCRIPTORS
	--"friendly" column names follow as column alias
--=========================================================

SELECT jd.inventory_item_id,
       msi.segment1                                 AS item_number,
       fifst.id_flex_structure_name                 AS structure_name,
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
  FROM fnd_id_flex_segments_vl   fifsm,
       fnd_id_flex_structures_vl fifst,
       fnd_application_vl        fa,
       mtl_system_items_vl       msi,
       jmf_descriptors           jd
 WHERE     
       jd.inventory_item_id = msi.inventory_item_id(+)
       AND msi.organization_id(+) = 121                          -- Master Org
       AND fa.application_short_name = 'XXJMF'                   -- CR R12EDW.
       AND fifst.application_id = fa.application_id
       AND fifst.id_flex_code = 'JDES'
       AND jd.item_type_id = fifst.id_flex_num
       AND fifsm.application_id = fifst.application_id
       AND fifsm.id_flex_code = fifst.id_flex_code
       AND fifsm.id_flex_num = fifst.id_flex_num
       AND fifsm.application_column_name <> 'SEGMENT1'
       AND fifst.enabled_flag = 'Y';
