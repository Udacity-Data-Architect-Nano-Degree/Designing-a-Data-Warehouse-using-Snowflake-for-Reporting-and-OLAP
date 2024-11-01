
SELECT 
    B.NAME AS "Business Name",
    W.TEMPERATURE_MIN AS "Min Temperature",
    W.TEMPERATURE_MAX AS "Max Temperature",
    W.PRECIPITATION AS "Precipitation",
    R.STARS AS "Rating",
    R.DATE_SK AS "Review Date"
FROM 
    UDACITYDWHPROJECT.ODS.REVIEW R
JOIN 
    UDACITYDWHPROJECT.ODS.BUSINESS B ON R.BUSINESS_SK = B.BUSINESS_SK
JOIN 
    UDACITYDWHPROJECT.ODS.DATE D ON R.DATE_SK = D.DATE_SK
JOIN 
    UDACITYDWHPROJECT.ODS.WEATHER W ON D.DATE_SK = W.DATE_SK
WHERE 
    B.STATE = 'PA'
    AND R.DATE_SK BETWEEN '20180101' AND '20221231'
    AND R.DATE_SK BETWEEN W.DATE_SK - 1 AND W.DATE_SK + 1;