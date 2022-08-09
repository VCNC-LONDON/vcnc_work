-- 코드를 입력하세요

WITH

user AS (
    SELECT
        tag.USER_ID AS user_id,
        IF(tag.AGE IS NULL, ROUND(SUM(tag.AGE) OVER () / COUNT(IF(tag.age IS NOT NULL, tag.id, NULL)) OVER ()), tag.AGE) AS age_pad,
        tag.GENDER AS gender,
        tu.TRADE_YN AS trade_yn
    FROM TOSS_AGE_GENDER AS tag
    LEFT JOIN TOSS_USER AS tu
    ON tu.ID = tag.USER_ID
),

community_msg AS (
    SELECT
        USER_ID AS msg_user_id,
        COUNT(ID) AS msg_cnt
    FROM TOSS_COMMUNITY_MESSAGE
    GROUP BY msg_user_id
),

calc AS (
    SELECT
        gender,
        (CASE
            WHEN age_pad < 20 THEN "20대 미만"
            WHEN age_pad BETWEEN 20 AND 29 THEN "20대 이상 30대 미만"
            WHEN age_pad BETWEEN 30 AND 39 THEN "30대 이상 40대 미만"
            WHEN age_pad BETWEEN 40 AND 49 THEN "40대 이상 50대 미만"
            WHEN age_pad BETWEEN 50 AND 59 THEN "50대 이상 60대 미만"
            WHEN age_pad >= 60 THEN "60대 이상"
        END) AS age_div,
        SUM(IFNULL(msg_cnt,0)) AS msg_cnt
    FROM user AS u
    LEFT JOIN community_msg AS cm
    ON u.user_id = cm.msg_user_id
    WHERE trade_yn = "Y"
    GROUP BY gender, age_div
)
SELECT 
    * 
FROM calc 
WHERE msg_cnt > 0 
ORDER BY msg_cnt DESC , age_div DESC , gender
