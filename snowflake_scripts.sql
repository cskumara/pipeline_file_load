create database ETL;

use database ETL;

create schema file_load;

use schema file_load;

create or replace table file_info
(
    ID NUMBER,
    FILE_PATH VARCHAR(2000),
    FILENAME VARCHAR(3000),
    FILE_TYPE VARCHAR(2000),
    DELIMITED VARCHAR(2000),
    ACTIVE VARCHAR(1000),
    LOAD_DATE DATE
);

CREATE OR REPLACE TABLE FILE_LOAD_INFO 
(
FILENAME VARCHAR(2000),
STATUS VARCHAR(2000),
RECORDS NUMBER
);

INSERT INTO FILE_INFO 
SELECT 1,'/Users/guna/kumaran/','user_info','.csv',';','Y',current_date
UNION SELECT 2,'/Users/guna/kumaran/','TESTFILE20230821','.TXT','|','Y',CURRENT_DATE;

select * from file_info;

SELECT * FROM FILE_LOAD_INFO;

