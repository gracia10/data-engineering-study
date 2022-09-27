-- 1. SQL 복습


SELECT channel
  FROM raw_data.user_session_channel
 LIMIT 10;
SELECT DISTINCT channel
  FROM raw_data.user_session_channel
 LIMIT 10;
SELECT COUNT(1)
  FROM raw_data.user_session_channel
 LIMIT 10;


-- 1) COUNT 함수 조건별 결과값 ------------------------------------------------------------------------------
DROP TABLE IF EXISTS gracia10.test_table;
CREATE TABLE gracia10.test_table
(
    value int
);
INSERT INTO gracia10.test_table
VALUES (NULL),
       (1),
       (1),
       (0),
       (0),
       (4),
       (3);
SELECT COUNT(1), COUNT(VALUE), COUNT(DISTINCT VALUE), COUNT(NULL)
  FROM gracia10.test_table;
-- 7 6 4 0


-- 2) 데이터 품질 검사 -------------------------------------------------------------------------------------
DROP TABLE IF EXISTS gracia10.test_channel;
CREATE TABLE gracia10.test_channel
(
    channel     varchar(32) PRIMARY KEY,
    description varchar(64) DEFAULT 'test'
);
INSERT INTO gracia10.test_channel
VALUES ('FACEBOOK', 'test'),
       ('GOOGLE', 'test');
INSERT INTO gracia10.test_channel
VALUES ('FACEBOOK'),
       ('GOOGLE');

-- 2-1) 중복레코드 확인
SELECT COUNT(1)
  FROM (SELECT DISTINCT *
          FROM gracia10.test_channel) ch;

-- 2-2) PK uniqueness 확인
SELECT channel, COUNT(1)
  FROM gracia10.test_channel
 GROUP BY 1
 ORDER BY 2 DESC
 LIMIT 1;

-- 2-3) (타임스탬프) 최근 업데이트된 레코드 확인
SELECT MAX(ts), MIN(ts)
  FROM raw_data.session_timestamp;

-- 3) CASE WHEN ---------------------------------------------------------------------------------------
SELECT LEFT(ts, 7),
       CASE
           WHEN COUNT(1) >= 15000 THEN '>= 15,000'
           WHEN COUNT(1) < 10000 THEN '< 10,000'
           ELSE '10000 and 15000'
           END
  FROM raw_data.session_timestamp
 GROUP BY 1
 ORDER BY 1;

-- 4) 공백이 있는 필드 , 예약어를 필드명으로 사용할 경우 ----------------------------------------------------------
DROP TABLE IF EXISTS gracia10.test;
-- CREATE TABLE gracia10.test (
--     group int primary key,           -- keyword
--     'mailing address' varchar(32)    -- "'mailing address'"
-- );
CREATE TABLE gracia10.test
(
    "group"           int PRIMARY KEY,
    "mailing address" varchar(32)
);
SELECT "group"
  FROM gracia10.test;

-- 5) NULL --------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS gracia10.test_boolean;
CREATE TABLE gracia10.test_boolean
(
    value boolean
);
INSERT INTO gracia10.test_boolean
VALUES (TRUE),
       (FALSE),
       (TRUE),
       (TRUE),
       (TRUE),
       (NULL);

-- 5-1) Boolean 필터링과 NULL과의 관계
SELECT COUNT(1)
  FROM gracia10.test_boolean
 WHERE value IS FALSE; -- 1
SELECT COUNT(1)
  FROM gracia10.test_boolean
 WHERE value IS NOT TRUE;
-- 5

-- 5-2) NULL 연산
SELECT COUNT(NULL); -- 0
SELECT 1 / NULL; -- NULL
SELECT 0 + NULL;
-- NULL

-- 5-3) NULL 치환
SELECT value, 100 / NULLIF(value, 0)
  FROM gracia10.test_table;
SELECT value, COALESCE(value, 1)
  FROM gracia10.test_table;

-- 6) 요약(+CTAS) ----------------------------------------------------------------------------------------
SELECT EXTRACT(HOUR FROM ts), COUNT(1) AS session_count
  FROM raw_data.session_timestamp
 GROUP BY 1
 ORDER BY 2 DESC
 LIMIT 1;

SELECT EXTRACT(DOW FROM ts), EXTRACT(HOUR FROM ts), COUNT(1)
  FROM raw_data.session_timestamp
 GROUP BY 1, 2
 ORDER BY 3 DESC
 LIMIT 1;

DROP TABLE IF EXISTS gracia10.monthly_active_user_summary;
CREATE TABLE gracia10.monthly_active_user_summary AS
SELECT TO_CHAR(ts, 'YYYY-MM') AS month, COUNT(DISTINCT userid)
  FROM raw_data.user_session_channel A
       JOIN raw_data.session_timestamp B ON A.sessionid = B.sessionid
 GROUP BY 1
 ORDER BY 1 DESC;
SELECT *
  FROM gracia10.monthly_active_user_summary;

-- 7) UNION , UNION ALL ----------------------------------------------------------------------------------------
SELECT 'newjeans' AS first_name, 'hearim' AS last_name
 UNION
SELECT 'newjeans', 'minji'
 UNION
SELECT 'newjeans', 'minji';

-- 8) JOIN -----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS gracia10.vital;
CREATE TABLE gracia10.vital
(
    UserID  int,
    VitalID int,
    Date    date,
    Weight  int
);
INSERT INTO gracia10.vital
VALUES (100, 1, '2020-01-01', 75),
       (100, 3, '2020-01-02', 78),
       (101, 2, '2020-01-01', 90),
       (101, 4, '2020-01-02', 95);

DROP TABLE IF EXISTS gracia10.alert;
CREATE TABLE gracia10.alert
(
    AlertID   int,
    VitalID   int,
    AlertType varchar(32),
    Date      date,
    UserID    int
);
INSERT INTO gracia10.alert
VALUES (1, 4, 'WeightIncrease', '2020-01-01', 101),
       (2, NULL, 'MissingVital', '2020-01-04', 100),
       (3, NULL, 'MissingVital', '2020-01-04', 101);

-- 조건절이 없는 CROSS JOIN
SELECT *
  FROM gracia10.Vital v
       CROSS JOIN gracia10.Alert a;

-- SELF JOIN
SELECT *
  FROM gracia10.Vital v1
       JOIN gracia10.Vital v2 ON v1.vitalID = v2.vitalID;



-- 9) Window function -----------------------------------------------------------------------------------------

-- 유저별 파티셔닝 후, 시간순으로 정렬한 뒤, 순서에 대한 필드 추가
SELECT userid, ts, channel, ROW_NUMBER() OVER (PARTITION BY userid ORDER BY ts) seq
  FROM raw_data.user_session_channel A
       INNER JOIN raw_data.session_timestamp B ON A.sessionid = B.sessionid
 WHERE userid IN (251);

-- 유저별 파티셔닝 후, 시간순으로 정렬한 뒤, 파티션별 첫번째/마지막 로우의 채널 필드 추가
SELECT userid
     , ts
     , channel
     , FIRST_VALUE(channel)
       OVER (PARTITION BY userid ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
     , LAST_VALUE(channel)
       OVER (PARTITION BY userid ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
  FROM raw_data.user_session_channel A
       INNER JOIN raw_data.session_timestamp B ON A.sessionid = B.sessionid
 WHERE userid IN (251);