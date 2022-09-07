SELECT 
  COUNT(1)
FROM tada.ride 
WHERE date_kr BETWEEN DATE("{execute_timestamp}","Asia/Seoul") - 1 AND DATE("{execute_timestamp}", "Asia/Seoul")
AND created_at >= TIMESTAMP_SUB("{execute_timestamp}", INTERVAL 3 HOUR)