---> create UdacityDWHProject database
CREATE DATABASE UdacityDWHProject;

---> create schema UdacityDWHProject.Staging
CREATE SCHEMA UdacityDWHProject.Staging;

---> create file format
CREATE FILE FORMAT UdacityDWHProject.Staging.json_ff
  TYPE = JSON
  COMPRESSION = 'AUTO'
  STRIP_OUTER_ARRAY = FALSE
  SKIP_BYTE_ORDER_MARK = TRUE;

---> create a stage
CREATE OR REPLACE STAGE UdacityDWHProject.Staging.Covid
file_format = UdacityDWHProject.Staging.json_ff;

---> use snowsql to load data
PUT file://C:/Users/samue/Downloads/yelp_academic_dataset_covid_features.json @COVID AUTO_COMPRESS=TRUE;

---> create table 
CREATE TABLE "UDACITYDWHPROJECT"."STAGING"."Covid" ( Call_To_Action_enabled VARCHAR , Covid_Banner VARCHAR , Grubhub_enabled VARCHAR , Request_a_Quote_Enabled VARCHAR , Temporary_Closed_Until VARCHAR , Virtual_Services_Offered VARCHAR , business_id VARCHAR , delivery_or_takeout VARCHAR , highlights VARCHAR ); 

---> load data
COPY INTO "UDACITYDWHPROJECT"."STAGING"."Covid" 
FROM (SELECT $1:"Call To Action enabled"::VARCHAR, $1:"Covid Banner"::VARCHAR, $1:"Grubhub enabled"::VARCHAR, $1:"Request a Quote Enabled"::VARCHAR, $1:"Temporary Closed Until"::VARCHAR, $1:"Virtual Services Offered"::VARCHAR, $1:business_id::VARCHAR, $1:"delivery or takeout"::VARCHAR, $1:highlights::VARCHAR
	FROM '@"UDACITYDWHPROJECT"."STAGING"."COVID"') 
FILES = ('yelp_academic_dataset_covid_features.json.gz') 
FILE_FORMAT = 'UDACITYDWHPROJECT.STAGING.JSON_FF' 
ON_ERROR=ABORT_STATEMENT;
-- For more details, see: https://docs.snowflake.com/en/sql-reference/sql/copy-into-table

---> create a stage for Business
CREATE OR REPLACE STAGE UdacityDWHProject.Staging.Business
file_format = UdacityDWHProject.Staging.json_ff;

---> use snowsql to load data
PUT file://C:/Users/samue/Downloads/yelp_academic_dataset_business.json @Business AUTO_COMPRESS=TRUE;

---> create Business table
CREATE TABLE "UDACITYDWHPROJECT"."STAGING"."Business" ( address VARCHAR , attributes VARCHAR , business_id VARCHAR , categories VARCHAR , city VARCHAR , hours VARCHAR , is_open NUMBER(38, 0) , latitude NUMBER(38, 10) , longitude NUMBER(38, 10) , name VARCHAR , postal_code VARCHAR , review_count NUMBER(38, 0) , stars NUMBER(38, 1) , state VARCHAR ); 

---> load data
COPY INTO "UDACITYDWHPROJECT"."STAGING"."Business" 
FROM (SELECT $1:address::VARCHAR, $1:attributes::VARCHAR, $1:business_id::VARCHAR, $1:categories::VARCHAR, $1:city::VARCHAR, $1:hours::VARCHAR, $1:is_open::NUMBER(1, 0), $1:latitude::NUMBER(12, 10), $1:longitude::NUMBER(13, 10), $1:name::VARCHAR, $1:postal_code::VARCHAR, $1:review_count::NUMBER(4, 0), $1:stars::NUMBER(2, 1), $1:state::VARCHAR
	FROM '@"UDACITYDWHPROJECT"."STAGING"."BUSINESS"') 
FILES = ('yelp_academic_dataset_business.json.gz') 
FILE_FORMAT = 'UDACITYDWHPROJECT.STAGING.JSON_FF' 
ON_ERROR=ABORT_STATEMENT;

---> create a stage for Tips
CREATE OR REPLACE STAGE UdacityDWHProject.Staging.Tips
file_format = UdacityDWHProject.Staging.json_ff;

---> use snowsql to load Tips data
PUT file://C:/Users/samue/Downloads/yelp_academic_dataset_tip.json @Tips AUTO_COMPRESS=TRUE;

---> create the Tips table
CREATE TABLE "UDACITYDWHPROJECT"."STAGING"."Tips" ( business_id VARCHAR , compliment_count NUMBER(38, 0) , date TIMESTAMP_NTZ , text VARCHAR , user_id VARCHAR ); 

COPY INTO "UDACITYDWHPROJECT"."STAGING"."Tips" 
FROM (SELECT $1:business_id::VARCHAR, $1:compliment_count::NUMBER(1, 0), $1:date::TIMESTAMP_NTZ, $1:text::VARCHAR, $1:user_id::VARCHAR
	FROM '@"UDACITYDWHPROJECT"."STAGING"."TIPS"') 
FILES = ('yelp_academic_dataset_tip.json.gz') 
FILE_FORMAT = 'UDACITYDWHPROJECT.STAGING.JSON_FF' 
ON_ERROR=ABORT_STATEMENT;

---> create a stage for Tips
CREATE OR REPLACE STAGE UdacityDWHProject.Staging.Check_in
file_format = UdacityDWHProject.Staging.json_ff;

---> use snowsql to load Check_in data
PUT file://C:/Users/samue/Downloads/yelp_academic_dataset_checkin.json @Check_in AUTO_COMPRESS=TRUE PARALLEL=2;

---> create check_in table
CREATE TABLE "UDACITYDWHPROJECT"."STAGING"."Check_in" ( business_id VARCHAR , date VARCHAR ); 

---> load check_in data
COPY INTO "UDACITYDWHPROJECT"."STAGING"."Check_in" 
FROM (SELECT $1:business_id::VARCHAR, $1:date::VARCHAR
	FROM '@"UDACITYDWHPROJECT"."STAGING"."CHECK_IN"') 
FILES = ('yelp_academic_dataset_checkin.json.gz') 
FILE_FORMAT = 'UDACITYDWHPROJECT.STAGING.JSON_FF' 
ON_ERROR=ABORT_STATEMENT;

---> create warehouse
CREATE OR REPLACE WAREHOUSE udacity_de_wh
    WAREHOUSE_SIZE = 'small'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'data engineering warehouse for udacity project';

USE WAREHOUSE udacity_de_wh;

---> create a stage for customer
CREATE OR REPLACE STAGE UdacityDWHProject.Staging.Customer
file_format = UdacityDWHProject.Staging.json_ff;

---> use snowsql to load customer data
PUT file://C:/Users/samue/Downloads/yelp_academic_dataset_user.json @Customer AUTO_COMPRESS=TRUE PARALLEL=8;

---> create customer table
CREATE TABLE "UDACITYDWHPROJECT"."STAGING"."Customer" ( average_stars NUMBER(38, 2) , compliment_cool NUMBER(38, 0) , compliment_cute NUMBER(38, 0) , compliment_funny NUMBER(38, 0) , compliment_hot NUMBER(38, 0) , compliment_list NUMBER(38, 0) , compliment_more NUMBER(38, 0) , compliment_note NUMBER(38, 0) , compliment_photos NUMBER(38, 0) , compliment_plain NUMBER(38, 0) , compliment_profile NUMBER(38, 0) , compliment_writer NUMBER(38, 0) , cool NUMBER(38, 0) , elite VARCHAR , fans NUMBER(38, 0) , friends VARCHAR , funny NUMBER(38, 0) , name VARCHAR , review_count NUMBER(38, 0) , useful NUMBER(38, 0) , user_id VARCHAR , yelping_since TIMESTAMP_NTZ ); 

COPY INTO "UDACITYDWHPROJECT"."STAGING"."Customer" 
FROM (SELECT $1:average_stars::NUMBER(3, 2), $1:compliment_cool::NUMBER(6, 0), $1:compliment_cute::NUMBER(6, 0), $1:compliment_funny::NUMBER(6, 0), $1:compliment_hot::NUMBER(6, 0), $1:compliment_list::NUMBER(5, 0), $1:compliment_more::NUMBER(5, 0), $1:compliment_note::NUMBER(5, 0), $1:compliment_photos::NUMBER(6, 0), $1:compliment_plain::NUMBER(6, 0), $1:compliment_profile::NUMBER(5, 0), $1:compliment_writer::NUMBER(5, 0), $1:cool::NUMBER(6, 0), $1:elite::VARCHAR, $1:fans::NUMBER(6, 0), $1:friends::VARCHAR, $1:funny::NUMBER(6, 0), $1:name::VARCHAR, $1:review_count::NUMBER(6, 0), $1:useful::NUMBER(6, 0), $1:user_id::VARCHAR, $1:yelping_since::TIMESTAMP_NTZ
	FROM '@"UDACITYDWHPROJECT"."STAGING"."CUSTOMER"') 
FILES = ('yelp_academic_dataset_user.json.gz') 
FILE_FORMAT = 'UDACITYDWHPROJECT.STAGING.JSON_FF' 
ON_ERROR=ABORT_STATEMENT;

---> create a stage for review
CREATE OR REPLACE STAGE UdacityDWHProject.Staging.Review
file_format = UdacityDWHProject.Staging.json_ff;

---> use snowsql to load review data
PUT file://C:/Users/samue/Downloads/yelp_academic_dataset_review.json @Review AUTO_COMPRESS=TRUE PARALLEL=16;

---> create review table 
CREATE TABLE "UDACITYDWHPROJECT"."STAGING"."Review" ( business_id VARCHAR , cool NUMBER(38, 0) , date TIMESTAMP_NTZ , funny NUMBER(38, 0) , review_id VARCHAR , stars NUMBER(38, 1) , text VARCHAR , useful NUMBER(38, 0) , user_id VARCHAR ); 

---> load data to review table 
COPY INTO "UDACITYDWHPROJECT"."STAGING"."Review" 
FROM (SELECT $1:business_id::VARCHAR, $1:cool::NUMBER(4, 0), $1:date::TIMESTAMP_NTZ, $1:funny::NUMBER(4, 0), $1:review_id::VARCHAR, $1:stars::NUMBER(4, 1), $1:text::VARCHAR, $1:useful::NUMBER(4, 0), $1:user_id::VARCHAR
	FROM '@"UDACITYDWHPROJECT"."STAGING"."REVIEW"') 
FILES = ('yelp_academic_dataset_review.json.gz') 
FILE_FORMAT = 'UDACITYDWHPROJECT.STAGING.JSON_FF' 
ON_ERROR=ABORT_STATEMENT;

---> CSV file format creation
CREATE OR REPLACE FILE FORMAT UdacityDWHProject.Staging.csv_ff 
    TYPE=CSV
    SKIP_HEADER=1
    FIELD_DELIMITER=','
    TRIM_SPACE=TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY='"'
    REPLACE_INVALID_CHARACTERS=TRUE
    DATE_FORMAT=AUTO
    TIME_FORMAT=AUTO
    TIMESTAMP_FORMAT=AUTO; 

---> create a stage for temperature
CREATE OR REPLACE STAGE UdacityDWHProject.Staging.Temperature
file_format = UdacityDWHProject.Staging.csv_ff;

---> create a stage for precipitation
CREATE OR REPLACE STAGE UdacityDWHProject.Staging.Precipitation
file_format = UdacityDWHProject.Staging.csv_ff;

---> create precipitation table 
CREATE TABLE "UDACITYDWHPROJECT"."STAGING"."Precipitation" ( date NUMBER(38, 0) , precipitation VARCHAR , precipitation_normal NUMBER(38, 2) ); 

---> load data to precipitation table 
COPY INTO "UDACITYDWHPROJECT"."STAGING"."Precipitation" 
FROM (SELECT $1, $2, $3
	FROM '@"UDACITYDWHPROJECT"."STAGING"."PRECIPITATION"') 
FILES = ('USC00366886-PHILADELPHIA_FRANKLIN_INSTITUTE-precipitation-inch.csv')
FILE_FORMAT = '"UDACITYDWHPROJECT"."STAGING"."CSV_FF"' 
ON_ERROR=ABORT_STATEMENT;

---> create temperature table
CREATE TABLE "UDACITYDWHPROJECT"."STAGING"."Temperature" ( date NUMBER(38, 0) , min NUMBER(38, 1) , max NUMBER(38, 1) , normal_min NUMBER(38, 1) , normal_max NUMBER(38, 1) ); 

---> load data to temperature table 
COPY INTO "UDACITYDWHPROJECT"."STAGING"."Temperature" 
FROM (SELECT $1, $2, $3, $4, $5
	FROM '@"UDACITYDWHPROJECT"."STAGING"."TEMPERATURE"') 
FILES = ('USC00366886-temperature-degreeF.csv')
FILE_FORMAT = '"UDACITYDWHPROJECT"."STAGING"."CSV_FF"' 
ON_ERROR=ABORT_STATEMENT;

