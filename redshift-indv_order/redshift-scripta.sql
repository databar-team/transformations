TRUNCATE TABLE  jostens_stage.sales_rep ;
​
COPY jostens_stage.sales_rep
FROM 's3://jostens-data-dev-analysis/sales_rep/'
FORMAT AS PARQUET
IAM_ROLE 'arn:aws:iam::147913565791:role/redshift-iam-role-s3-glue-athena' ;
​
DROP TABLE IF EXISTS jostens.sales_rep; 
​
CREATE TABLE jostens.sales_rep
SORTKEY (salesrep_number) 
AS 
SELECT  *
FROM  jostens_stage.sales_rep  ;
​
​
/*
SELECT *
FROM   jostens.sales_rep 
LIMIT  200 ;
​
SELECT COUNT(*)
FROM   jostens.sales_rep ;
​
SELECT salesrep_number
      ,COUNT(*)
FROM  jostens.sales_rep
GROUP BY salesrep_number
HAVING COUNT(*) > 1 ;
*/
