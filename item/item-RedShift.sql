Select 
 pid, 
 user_name, 
 starttime, 
 query, 
 query, 
 status 
from stv_recents 
where status='Running';
SELECT pg_terminate_backend(9840);
DROP TABLE jostens_stage.item;
CREATE TABLE jostens_stage.item(
  cover_size varchar(50), 
  dimensions varchar(50), 
  item_size varchar(50), 
  style varchar(50), 
  type varchar(50), 
  material varchar(50), 
  color varchar(50), 
  degree varchar(50), 
  lot_size varchar(50), 
  metal_quality varchar(50), 
  apparel_color varchar(50), 
  apparel_size varchar(50), 
  quality varchar(50), 
  length varchar(50), 
  design varchar(50), 
  trim_size varchar(50), 
  system_update_date_time date
);
TRUNCATE TABLE  jostens_stage.item ;
​
COPY jostens_stage.item
FROM 's3://jostens-data-dev-analysis/item/'
FORMAT AS PARQUET
IAM_ROLE 'arn:aws:iam::147913565791:role/redshift-iam-role-s3-glue-athena' ;
​
SELECT COUNT(*) FROM jostens_stage.item;
DROP TABLE IF EXISTS jostens.item CASCADE; 
​
CREATE TABLE jostens.item
SORTKEY (design) 
AS 
SELECT  *
FROM  jostens_stage.item  ;
​
select * from jostens.item limit 5;
select count(*) from jostens.item;

--data type
select pg_get_cols ('jostens.item');