CREATE TABLE rawdb.edw_item_descriptors
 WITH (format='Parquet', external_location='s3://jostens-data-dev-raw/edw_item_descriptors/test/',
 parquet_compression = 'SNAPPY')
 as select * from analysisdb.item_mf02