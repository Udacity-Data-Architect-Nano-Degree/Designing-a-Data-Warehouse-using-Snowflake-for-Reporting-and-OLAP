-- Ingest data from STAGING.Business to ODS.BUSINESS
INSERT INTO UDACITYDWHPROJECT.ODS.BUSINESS (
  BUSINESS_ID, NAME, ADDRESS, CITY, STATE, POSTAL_CODE, LATITUDE, LONGITUDE, STARS, REVIEW_COUNT, IS_OPEN, CATEGORIES
)
SELECT
  BUSINESS_ID, NAME, ADDRESS, CITY, STATE, POSTAL_CODE, LATITUDE, LONGITUDE, STARS, REVIEW_COUNT, IS_OPEN, CATEGORIES
FROM UDACITYDWHPROJECT.STAGING."Business";

-- Ingest data from STAGING.Customer to ODS.CUSTOMER  
INSERT INTO UDACITYDWHPROJECT.ODS.CUSTOMER (
  USER_ID, NAME, REVIEW_COUNT, YELPING_SINCE, AVERAGE_STARS, FANS, ELITE
)
SELECT
  USER_ID, NAME, REVIEW_COUNT, YELPING_SINCE, AVERAGE_STARS, FANS, ELITE
FROM UDACITYDWHPROJECT.STAGING."Customer";

-- Populate DATE table
-- Create a temporary sequence to generate dates
CREATE OR REPLACE SEQUENCE date_seq START = 0 INCREMENT = 1;

-- Insert the data
INSERT INTO UDACITYDWHPROJECT.ODS.DATE
SELECT 
    TO_NUMBER(TO_CHAR(DATEADD(day, SEQ4(), '2005-02-16'), 'YYYYMMDD')),
    DATEADD(day, SEQ4(), '2005-02-16'),
    YEAR(DATEADD(day, SEQ4(), '2005-02-16')),
    MONTH(DATEADD(day, SEQ4(), '2005-02-16')),
    DAY(DATEADD(day, SEQ4(), '2005-02-16')),
    QUARTER(DATEADD(day, SEQ4(), '2005-02-16')),
    DAYOFWEEK(DATEADD(day, SEQ4(), '2005-02-16')) + 1,
    WEEKOFYEAR(DATEADD(day, SEQ4(), '2005-02-16'))
FROM TABLE(GENERATOR(ROWCOUNT=>6181))  -- This will generate dates from 2018-01-01 to 2024-12-31
WHERE DATEADD(day, SEQ4(), '2005-02-16') <= '2022-01-19';


-- Insert Precipitation data
INSERT INTO UDACITYDWHPROJECT.ODS.WEATHER (
    DATE_SK,
    PRECIPITATION,
    PRECIPITATION_NORMAL
)
SELECT
    DATE::NUMBER AS DATE_SK,
    TRY_CAST(PRECIPITATION AS FLOAT) AS PRECIPITATION,
    PRECIPITATION_NORMAL
FROM UDACITYDWHPROJECT.STAGING."Precipitation";

-- Update with Temperature data
MERGE INTO UDACITYDWHPROJECT.ODS.WEATHER W
USING (
    SELECT 
        DATE::NUMBER AS DATE_SK,
        MIN AS TEMPERATURE_MIN,
        MAX AS TEMPERATURE_MAX,
        NORMAL_MIN AS TEMPERATURE_NORMAL_MIN,
        NORMAL_MAX AS TEMPERATURE_NORMAL_MAX
    FROM UDACITYDWHPROJECT.STAGING."Temperature"
) T
ON W.DATE_SK = T.DATE_SK
WHEN MATCHED THEN UPDATE SET
    W.TEMPERATURE_MIN = T.TEMPERATURE_MIN,
    W.TEMPERATURE_MAX = T.TEMPERATURE_MAX,
    W.TEMPERATURE_NORMAL_MIN = T.TEMPERATURE_NORMAL_MIN,
    W.TEMPERATURE_NORMAL_MAX = T.TEMPERATURE_NORMAL_MAX;

-- Ingest data from STAGING.Check_in to ODS.CHECKIN
INSERT INTO UDACITYDWHPROJECT.ODS.CHECKIN (
    BUSINESS_SK,
    CHECKIN_COUNT
)
WITH split_dates AS (
    SELECT 
        C.BUSINESS_ID,
        COUNT(value) as checkin_count
    FROM UDACITYDWHPROJECT.STAGING."Check_in" C,
    TABLE(SPLIT_TO_TABLE(C.DATE, ':')) 
    WHERE TRIM(value) != ''
    GROUP BY C.BUSINESS_ID
)
SELECT
    B.BUSINESS_SK,
    split_dates.checkin_count
FROM split_dates
JOIN UDACITYDWHPROJECT.ODS.BUSINESS B 
    ON split_dates.BUSINESS_ID = B.BUSINESS_ID;


-- Ingest data from STAGING.Review to ODS.FACT_REVIEW
INSERT INTO UDACITYDWHPROJECT.ODS.REVIEW (
  REVIEW_ID,
  BUSINESS_SK,
  CUSTOMER_SK,
  DATE_SK,
  STARS,
  USEFUL,
  FUNNY,
  COOL,
  TEXT
)
SELECT
  R.REVIEW_ID,
  B.BUSINESS_SK,
  C.CUSTOMER_SK,
  D.DATE_SK,
  R.STARS,
  R.USEFUL,
  R.FUNNY,
  R.COOL,
  R.TEXT
FROM UDACITYDWHPROJECT.STAGING."Review" R
JOIN UDACITYDWHPROJECT.ODS.BUSINESS B ON R.BUSINESS_ID = B.BUSINESS_ID
JOIN UDACITYDWHPROJECT.ODS.CUSTOMER C ON R.USER_ID = C.USER_ID
JOIN UDACITYDWHPROJECT.ODS.DATE D ON DATE_TRUNC('day', R.DATE) = D.FULL_DATE;


-- Ingest data from STAGING.Covid to ODS.FACT_COVID
INSERT INTO UDACITYDWHPROJECT.ODS.COVID (
  BUSINESS_SK,
  DELIVERY_OR_TAKEOUT,
  VIRTUAL_SERVICES_OFFERED,
  TEMPORARY_CLOSED_UNTIL
)
SELECT
  B.BUSINESS_SK,
  C.DELIVERY_OR_TAKEOUT,
  C.VIRTUAL_SERVICES_OFFERED,
  C.TEMPORARY_CLOSED_UNTIL
FROM UDACITYDWHPROJECT.STAGING."Covid" C
JOIN UDACITYDWHPROJECT.ODS.BUSINESS B ON C.BUSINESS_ID = B.BUSINESS_ID;

-- insert data from staging to ODS
INSERT INTO UDACITYDWHPROJECT.ODS.TIPS (
    BUSINESS_SK,
    CUSTOMER_SK,
    DATE_SK,
    COMPLIMENT_COUNT,
    TIP_TEXT
)
SELECT
    B.BUSINESS_SK,
    C.CUSTOMER_SK,
    D.DATE_SK,
    T.COMPLIMENT_COUNT,
    T.TEXT as TIP_TEXT
FROM UDACITYDWHPROJECT.STAGING."Tips" T
JOIN UDACITYDWHPROJECT.ODS.BUSINESS B 
    ON T.BUSINESS_ID = B.BUSINESS_ID
    AND B.IS_CURRENT = TRUE
JOIN UDACITYDWHPROJECT.ODS.CUSTOMER C 
    ON T.USER_ID = C.USER_ID
    AND C.IS_CURRENT = TRUE
JOIN UDACITYDWHPROJECT.ODS.DATE D
    ON DATE(T.DATE) = D.FULL_DATE; 


