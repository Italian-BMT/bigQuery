# 시간별(2시간 간격으로 24시간 분할) 열차 도착 정확도 table

from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date, timedelta

# Create a BigQuery client
KEY_PATH = "C:/Users/82105/Downloads/gifted-chimera-392409-099c75ba0e9d.json"
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
client = bigquery.Client(credentials= credentials, project= credentials.project_id)


base_table_name1 = 'gifted-chimera-392409.2023_06_01.timeTag_acc'
base_table_name2 = 'gifted-chimera-392409.2023_06_01.realtime-final-after'
query1 = f'''


--tts_all2에서 시간대 별로 accord가 1인 비율 계산
CREATE OR REPLACE TABLE `{base_table_name1}`
    AS
WITH timetags AS (
  SELECT 
    *,
    CASE 
      WHEN arriveTime>= '00:00:00' AND arriveTime<'01:00:00' THEN 'time1(0_1시)'
      WHEN arriveTime>= '01:00:00' AND arriveTime<'04:00:00' THEN 'time2(1_4시)'
      WHEN arriveTime>= '04:00:00' AND arriveTime<'06:00:00' THEN 'time3(4_6시)'
      WHEN arriveTime>= '06:00:00' AND arriveTime<'08:00:00' THEN 'time4(6_8시)'
      WHEN arriveTime>= '08:00:00' AND arriveTime<'10:00:00' THEN 'time5(8_10시)'
      WHEN arriveTime>= '10:00:00' AND arriveTime<'12:00:00' THEN 'time6(10_12시)'
      WHEN arriveTime>= '12:00:00' AND arriveTime<'14:00:00' THEN 'time7(12_14시)'
      WHEN arriveTime>= '14:00:00' AND arriveTime<'16:00:00' THEN 'time8(14_16시)'
      WHEN arriveTime>= '16:00:00' AND arriveTime<'18:00:00' THEN 'time9(16_18시)'
      WHEN arriveTime>= '18:00:00' AND arriveTime<'20:00:00' THEN 'time_10(18_20시)'
      WHEN arriveTime>= '20:00:00' AND arriveTime<'22:00:00' THEN 'time_11(20_22시)'
      WHEN arriveTime>= '22:00:00' AND arriveTime<'23:59:59' THEN 'time_12(22_24시)'
    END AS timetag
  FROM `gifted-chimera-392409.2023_06_01.tts_all2`
),
timetag_counts AS (
  SELECT 
    timetag,
    COUNT(*) AS total,
    COUNTIF(accord = 1) AS accord_count
  FROM timetags
  GROUP BY timetag
)
SELECT
  t.weekTag,
  t.lineNum,
  t.stationNm,
  t.inOutTag,
  t.arriveTime,
  t.arriveDate,
  t.accord,
  t.timetag,
  IFNULL(tc.accord_count, 0) / tc.total AS accord_ratio
FROM
  timetags t
LEFT JOIN
  timetag_counts tc
ON
  t.timetag = tc.timetag
ORDER BY
  t.timetag;

    
'''

job = client.query(query1).result()

