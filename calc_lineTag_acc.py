# 호선별 열차 도착 정확도 table

from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date, timedelta

# Create a BigQuery client
KEY_PATH = "C:/Users/82105/Downloads/gifted-chimera-392409-099c75ba0e9d.json"
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
client = bigquery.Client(credentials= credentials, project= credentials.project_id)


base_table_name1 = 'gifted-chimera-392409.2023_06_01.lineTag_acc'
query1 = f'''

--tts_all2에서 호선 별로 accord가 1인 비율 계산
CREATE OR REPLACE TABLE `{base_table_name1}`
    AS
WITH
linetag_counts AS (
  SELECT 
    lineNum,
    COUNT(*) AS total,
    COUNTIF(accord = 1) AS accord_count
  FROM `gifted-chimera-392409.2023_06_01.tts_all2`
  GROUP BY lineNum
)
SELECT
  t.weekTag,
  t.lineNum,
  t.stationNm,
  t.inOutTag,
  t.arriveTime,
  t.arriveDate,
  t.accord,
  IFNULL(lc.accord_count, 0) / lc.total AS accord_ratio
FROM
  `gifted-chimera-392409.2023_06_01.tts_all2` t
LEFT JOIN
  linetag_counts lc
ON
  t.lineNum = lc.lineNum
ORDER BY
  t.lineNum;
    
'''

job = client.query(query1).result()
