
-- Weather impact on customer behavior

SELECT 
    c.ELITE,
    w.TEMPERATURE_MAX,
    AVG(f.RATING) as avg_rating,
    COUNT(*) as review_count
FROM FACT_BUSINESS_RATINGS f
JOIN DIM_CUSTOMER c ON f.CUSTOMER_SK = c.CUSTOMER_SK
JOIN DIM_WEATHER w ON f.WEATHER_SK = w.WEATHER_SK
WHERE c.IS_CURRENT = TRUE
GROUP BY c.ELITE, w.TEMPERATURE_MAX;