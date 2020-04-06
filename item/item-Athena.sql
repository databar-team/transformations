CREATE TABLE rawdb.edw_items_22
 WITH (format='Parquet', external_location='s3://jostens-data-dev-raw/edw_items_22/test/',
 parquet_compression = 'SNAPPY')
 as
 with cover_size_CTE as (

		     SELECT d.inventory_item_id, segment_value cover_size
			FROM rawdb.edw_item_descriptors d
            inner join rawdb.edw_items i
		       on d.inventory_item_id = i.inventory_item_id
			 AND d.segment_name = 'COVER SIZE'
),
dimensions_CTE as (
	SELECT d.inventory_item_id, segment_value dimensions
			FROM rawdb.edw_item_descriptors d
                        inner join rawdb.edw_items i
		       on d.inventory_item_id = i.inventory_item_id
			 AND d.segment_name = 'DIMENSIONS'
),
item_size_CTE as (
	SELECT d.inventory_item_id, segment_value item_size
			FROM rawdb.edw_item_descriptors d
                        inner join rawdb.edw_items i
		       on d.inventory_item_id = i.inventory_item_id
			 AND d.segment_name = 'SIZE'
),
style_CTE as (
	SELECT d.inventory_item_id, segment_value style
			FROM rawdb.edw_item_descriptors d
                        inner join rawdb.edw_items i
		       on d.inventory_item_id = i.inventory_item_id
			 AND d.segment_name = 'STYLE'
),
type_CTE as (
	SELECT d.inventory_item_id, segment_value type
			FROM rawdb.edw_item_descriptors d
                        inner join rawdb.edw_items i
		       on d.inventory_item_id = i.inventory_item_id
			 AND d.segment_name = 'TYPE'
),
material_CTE as (
	SELECT d.inventory_item_id, segment_value material
			FROM rawdb.edw_item_descriptors d
                        inner join rawdb.edw_items i
		       on d.inventory_item_id = i.inventory_item_id
			 AND d.segment_name = 'MATERIAL'
),
color_CTE as (
	SELECT d.inventory_item_id, segment_value color
			FROM rawdb.edw_item_descriptors d
                        inner join rawdb.edw_items i
		       on d.inventory_item_id = i.inventory_item_id
			 AND d.segment_name = 'COLOR'
),
degree_CTE as (
	SELECT d.inventory_item_id, segment_value degree
			FROM rawdb.edw_item_descriptors d
                        inner join rawdb.edw_items i
		       on d.inventory_item_id = i.inventory_item_id
			 AND d.segment_name = 'DEGREE'
),
lot_size_CTE as (
	SELECT d.inventory_item_id, segment_value lot_size
			FROM rawdb.edw_item_descriptors d
                        inner join rawdb.edw_items i
		       on d.inventory_item_id = i.inventory_item_id
			 AND d.segment_name = 'LOT SIZE'
),
metal_quality_CTE as (
	SELECT d.inventory_item_id, segment_value metal_quality
			FROM rawdb.edw_item_descriptors d
                        inner join rawdb.edw_items i
		       on d.inventory_item_id = i.inventory_item_id
			 AND d.segment_name = 'METAL QUALITY'
),
apparel_color_CTE as (
	SELECT d.inventory_item_id, segment_value apparel_color
			FROM rawdb.edw_item_descriptors d
                        inner join rawdb.edw_items i
		       on d.inventory_item_id = i.inventory_item_id
			 AND d.segment_name = 'APPAREL COLOR'
),
apparel_size_CTE as (
	SELECT d.inventory_item_id, segment_value apparel_size
			FROM rawdb.edw_item_descriptors d
                        inner join rawdb.edw_items i
		       on d.inventory_item_id = i.inventory_item_id
			 AND d.segment_name = 'APPAREL SIZE'
),
quality_CTE as (
	SELECT d.inventory_item_id, segment_value quality
			FROM rawdb.edw_item_descriptors d
                        inner join rawdb.edw_items i
		       on d.inventory_item_id = i.inventory_item_id
			 AND d.segment_name = 'QUALITY'
),
length_CTE as (
	SELECT d.inventory_item_id, segment_value length
			FROM rawdb.edw_item_descriptors d
                        inner join rawdb.edw_items i
		       on d.inventory_item_id = i.inventory_item_id
			 AND d.segment_name = 'LENGTH'
),
Design_CTE as (
	SELECT d.inventory_item_id, segment_value Design
			FROM rawdb.edw_item_descriptors d
                        inner join rawdb.edw_items i
		       on d.inventory_item_id = i.inventory_item_id
			 AND d.segment_name = 'DESIGN'
),
trim_size_CTE as (
	SELECT d.inventory_item_id, segment_value trim_size
			FROM rawdb.edw_item_descriptors d
                        inner join rawdb.edw_items i
		       on d.inventory_item_id = i.inventory_item_id
			 AND d.segment_name = 'TRIM SIZE'
)
-- insert into table rawdb.item 
select distinct cover_size_CTE.cover_size, dimensions_CTE.dimensions, 
item_size_CTE.item_size, style_CTE.style, type_CTE.type, material_CTE.material, color_CTE.color, 
degree_CTE.degree, lot_size_CTE.lot_size, metal_quality_CTE.metal_quality, 
apparel_color_CTE.apparel_color, apparel_size_CTE.apparel_size,
quality_CTE.quality, length_CTE.length, Design_CTE.Design, trim_size_CTE.trim_size, current_date as system_update_date_time  -- we want to use distinct because of '1 to many' for mf01 and mf02
from analysisdb.item_mf01 
	left join cover_size_CTE on item_mf01.inventory_item_id = cover_size_CTE.inventory_item_id
    	left join dimensions_CTE on item_mf01.inventory_item_id = dimensions_CTE.inventory_item_id
    	left join item_size_CTE on item_mf01.inventory_item_id = item_size_CTE.inventory_item_id
    	left join style_CTE on item_mf01.inventory_item_id = style_CTE.inventory_item_id
    	left join type_CTE on item_mf01.inventory_item_id = type_CTE.inventory_item_id
    	left join material_CTE on item_mf01.inventory_item_id = material_CTE.inventory_item_id
    	left join color_CTE on item_mf01.inventory_item_id = color_CTE.inventory_item_id
    	left join degree_CTE on item_mf01.inventory_item_id = degree_CTE.inventory_item_id
    	left join lot_size_CTE on item_mf01.inventory_item_id = lot_size_CTE.inventory_item_id
    	left join metal_quality_CTE on item_mf01.inventory_item_id = metal_quality_CTE.inventory_item_id
    	left join apparel_color_CTE on item_mf01.inventory_item_id = apparel_color_CTE.inventory_item_id
    	left join apparel_size_CTE on item_mf01.inventory_item_id = apparel_size_CTE.inventory_item_id
    	left join quality_CTE on item_mf01.inventory_item_id = quality_CTE.inventory_item_id
    	left join length_CTE on item_mf01.inventory_item_id = length_CTE.inventory_item_id
    	left join Design_CTE on item_mf01.inventory_item_id = Design_CTE.inventory_item_id
    	left join trim_size_CTE on item_mf01.inventory_item_id = trim_size_CTE.inventory_item_id
