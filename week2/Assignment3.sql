-- 3. Gross Revenue가 가장 큰 UserID 10개 찾기

SELECT userid, SUM(amount) revenue
  FROM raw_data.user_session_channel A
       LEFT OUTER JOIN raw_data.session_transaction B ON A.sessionid = B.sessionid
 WHERE B.sessionid IS NOT NULL
 GROUP BY 1
 ORDER BY 2 DESC
 LIMIT 10;

