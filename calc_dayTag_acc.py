# 호선별 열차 도착 정확도 table

from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date, timedelta

# Create a BigQuery client
KEY_PATH = "C:/Users/82105/Downloads/gifted-chimera-392409-099c75ba0e9d.json"
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
client = bigquery.Client(credentials= credentials, project= credentials.project_id)


base_table_name1 = 'gifted-chimera-392409.2023_06_01.weekTag_acc'
query1 = f'''

--tts_all2에서 호선 별로 accord가 1인 비율 계산
CREATE OR REPLACE TABLE `{base_table_name1}`
    AS
WITH weekDayTag AS (
  SELECT 
    *,
    CASE EXTRACT(DAYOFWEEK FROM arriveDate)
        WHEN 2 THEN "월"
        WHEN 3 THEN "화"
        WHEN 4 THEN "수"
        WHEN 5 THEN "목"
        WHEN 6 THEN "금"
        WHEN 7 THEN "토"
        WHEN 1 THEN "일"
    END AS weekDay
  FROM `gifted-chimera-392409.2023_06_01.tts_all2`
),
WeekDayTag_counts AS (
  SELECT 
    weekDay,
    COUNT(*) AS total,
    COUNTIF(accord = 1) AS accord_count
  FROM weekDayTag
  GROUP BY weekDay
)
SELECT
  w.weekTag,
  w.lineNum,
  w.stationNm,
  w.inOutTag,
  w.arriveTime,
  w.arriveDate,
  w.accord,
  w.weekDay,
  IFNULL(wc.accord_count, 0) / wc.total AS accord_ratio
FROM
  weekDayTag w
LEFT JOIN
  WeekDayTag_counts wc
ON
  w.weekDay = wc.weekDay
ORDER BY
  w.weekDay;
    
'''

job = client.query(query1).result()
