-- 4-1. 본인 스키마 밑에 CTAS 로 테이블 만들기
DROP TABLE IF EXISTS gracia10.user_session_channel;
DROP TABLE IF EXISTS gracia10.session_timestamp;
DROP TABLE IF EXISTS gracia10.session_transaction;
CREATE TABLE gracia10.user_session_channel AS
SELECT *
  FROM raw_data.user_session_channel;
CREATE TABLE gracia10.session_timestamp AS
SELECT *
  FROM raw_data.session_timestamp;
CREATE TABLE gracia10.session_transaction AS
SELECT *
  FROM raw_data.session_transaction;

-- 4-2. 채널 & 월별 매출액 테이블 만들기
SELECT TO_CHAR(B.ts, 'YYYY-MM')                                 "year-month"
     , A.channel                                                channel
     , COUNT(DISTINCT A.userid)                                 uniqueUsers
     , COUNT(DISTINCT CASE WHEN C.amount > 0 THEN A.userid END) paidUsers
     , ROUND(paidUsers * 100.0 / NULLIF(uniqueUsers, 0), 2)     conversionRate
     , SUM(C.amount)                                            grossRevenue
     , SUM(CASE WHEN refunded IS FALSE THEN C.amount END)       netRevenue
  FROM gracia10.user_session_channel A
       INNER JOIN gracia10.session_timestamp B ON A.sessionid = B.sessionid
       LEFT OUTER JOIN gracia10.session_transaction C ON A.sessionid = C.sessionid
 GROUP BY 1, 2
 ORDER BY 1, 2;



















