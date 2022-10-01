-- 2. 사용자별로 처음과 마지막 채널 알아내기 (사용자ID, 첫번째 채널, 마지막 채널)

-- 2-1-1) ROW_NUMBER 을 사용한 경우1
  WITH visited_channel AS (SELECT A.userid
                                , A.channel
                                , ROW_NUMBER() OVER (PARTITION BY A.userid ORDER BY B.ts ASC)  first_visited_seq
                                , ROW_NUMBER() OVER (PARTITION BY A.userid ORDER BY B.ts DESC) last_visited_seq
                             FROM raw_data.user_session_channel A
                                  INNER JOIN raw_data.session_timestamp B ON A.sessionid = B.sessionid)
SELECT userid
     , MAX(CASE WHEN first_visited_seq = 1 THEN channel END) first_channel
     , MAX(CASE WHEN last_visited_seq = 1 THEN channel END)  last_channel
  FROM visited_channel
 GROUP BY 1
 ORDER BY 1
;

-- 2-1-2) ROW_NUMBER 을 사용한 경우2
  WITH cte AS (SELECT userid
                    , channel
                    , (ROW_NUMBER() OVER (PARTITION BY usc.userid ORDER BY st.ts ASC))  arn
                    , (ROW_NUMBER() OVER (PARTITION BY usc.userid ORDER BY st.ts DESC)) drn
                 FROM raw_data.user_session_channel usc
                      JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid)
SELECT cte1.userid, cte1.channel AS first_touch, cte2.channel AS last_touch
  FROM cte cte1
       JOIN cte cte2 ON cte1.userid = cte2.userid
 WHERE cte1.arn = 1
   AND cte2.drn = 1
 ORDER BY 1;


-- 2-2) FIRST_VALUE / LAST_VLAUE 를 사용한 경우
SELECT DISTINCT A.userid
              , FIRST_VALUE(A.channel)
                OVER (PARTITION BY A.userid ORDER BY B.ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) first_channel
              , LAST_VALUE(A.channel)
                OVER (PARTITION BY A.userid ORDER BY B.ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_channel
  FROM raw_data.user_session_channel A
       INNER JOIN raw_data.session_timestamp B ON A.sessionid = B.sessionid
 ORDER BY 1
;