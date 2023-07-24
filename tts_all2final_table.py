from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date, timedelta

# Create a BigQuery client
KEY_PATH = "C:/Users/82105/Downloads/gifted-chimera-392409-099c75ba0e9d.json"
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
client = bigquery.Client(credentials= credentials, project= credentials.project_id)


base_table_name1 = 'gifted-chimera-392409.2023_06_01.tts_all2'
base_table_name2 = 'gifted-chimera-392409.2023_06_01.realtime-final-after'
query1 = f'''


    --timetable에서 realtime과 일치하는 것만 accord=1로 설정.

    ALTER TABLE `{base_table_name1}`
    ADD COLUMN accord INT64;

    ALTER TABLE `{base_table_name1}`
    ALTER COLUMN accord SET DEFAULT 0;

    UPDATE `{base_table_name1}`
    SET accord = 0
    WHERE TRUE;
    
    --tts_all 최종 변형

    UPDATE `{base_table_name1}` AS tts
    SET tts.accord = 1
    WHERE EXISTS (
        SELECT 1
        FROM `{base_table_name2}` AS rtf
        WHERE STRING(tts.arriveTime) = rtf.arriveTime
            AND tts.stationNm = rtf.stationNm
            AND tts.arriveDate = rtf.arriveDate
            AND tts.lineNum = rtf.lineNum
    );
'''

job = client.query(query1).result()

