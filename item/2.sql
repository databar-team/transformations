CREATE TABLE rawdb.edw_items
 WITH (format='Parquet', external_location='s3://jostens-data-dev-raw/edw_items/test/',
 parquet_compression = 'SNAPPY')
 as select * from analysisdb.item_mf01