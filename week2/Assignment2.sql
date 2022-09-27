-- 2. 사용자별로 처음과 마지막 채널 알아내기 (사용자ID, 첫번째 채널, 마지막 채널)

-- ROW_NUMBER 을 사용한 경우
  WITH visited_channel AS (SELECT A.userid
                                , A.channel
                                , ROW_NUMBER() OVER (PARTITION BY A.userid ORDER BY B.ts ASC)  first_visited_seq
                                , ROW_NUMBER() OVER (PARTITION BY A.userid ORDER BY B.ts DESC) last_visited_seq
                             FROM raw_data.user_session_channel A
                                  INNER JOIN raw_data.session_timestamp B ON A.sessionid = B.sessionid)
SELECT userid
     , MAX(CASE WHEN first_visited_seq = 1 THEN channel ELSE '' END) first_channel
     , MAX(CASE WHEN last_visited_seq = 1 THEN channel ELSE '' END)  last_channel
  FROM visited_channel
 GROUP BY 1
 ORDER BY 1
;


-- FIRST_VALUE / LAST_VLAUE 를 사용한 경우
  WITH visited_channel AS (SELECT A.userid
                                , FIRST_VALUE(A.channel)
                                  OVER (PARTITION BY A.userid ORDER BY B.ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) first_channel
                                , LAST_VALUE(A.channel)
                                  OVER (PARTITION BY A.userid ORDER BY B.ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_channel
                             FROM raw_data.user_session_channel A
                                  INNER JOIN raw_data.session_timestamp B ON A.sessionid = B.sessionid)
SELECT DISTINCT *
  FROM visited_channel
 ORDER BY 1
;