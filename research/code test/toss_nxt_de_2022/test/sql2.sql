-- 코드를 입력하세요
WITH

transactions AS (
    SELECT
        *
    FROM MERCHANTS
    WHERE NAME LIKE "%강남%" OR NAME LIKE "%논현%"
)

SELECT 
    t.ID AS market_id,
    t.NAME AS market_name,
    COUNT(DISTINCT IF(cu.AMOUNT > 0 , cu.ID, NULL)) AS purchase_cnt
FROM transactions AS t
LEFT JOIN CARD_USAGES AS cu
ON t.ID = cu.MERCHANT_ID
GROUP BY market_id, market_name
ORDER BY market_id