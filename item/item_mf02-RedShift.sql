Select 
 pid, 
 user_name, 
 starttime, 
 query, 
 query, 
 status 
from stv_recents 
where status='Running';
SELECT pg_terminate_backend(18709);
DROP TABLE jostens_stage.item_mf02;
CREATE TABLE jostens_stage.item_mf02(
    inventory_item_id bigint, 
  item_number varchar(50), 
  structure_name varchar(50), 
  segment_name varchar(50), 
  segment_value varchar(50), 
  segment_number varchar(50), 
  descriptor_id bigint
  );
TRUNCATE TABLE  jostens_stage.item_mf02 ;
​
COPY jostens_stage.item_mf02
FROM 's3://jostens-data-dev-analysis/item_mf02/'
FORMAT AS PARQUET
IAM_ROLE 'arn:aws:iam::147913565791:role/redshift-iam-role-s3-glue-athena' ;
​
SELECT COUNT(*) FROM jostens_stage.item_mf02;
SELECT * FROM jostens_stage.item_mf02;
DROP TABLE IF EXISTS jostens.item_mf02 CASCADE; 
​
CREATE TABLE jostens.item_mf02
SORTKEY (inventory_item_id) 
AS 
SELECT  *
FROM  jostens_stage.item_mf02;
​
select * from jostens.item_mf02 limit 5;
select count(*) from jostens.item_mf02;

--data type
select pg_get_cols ('jostens.item_mf02');