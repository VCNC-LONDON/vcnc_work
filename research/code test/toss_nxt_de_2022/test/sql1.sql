-- 코드를 입력하세요
-- 조회하고자 하는 유저의 ID: barracudafried1489
SELECT
    DISTINCT ITEM
FROM PURCHASES
WHERE USER_ID = "barracudafried1489"
ORDER BY PURCHASE_DATE DESC