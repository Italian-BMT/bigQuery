from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date, timedelta

# Create a BigQuery client
KEY_PATH = "C:/Users/82105/Downloads/gifted-chimera-392409-099c75ba0e9d.json"
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
client = bigquery.Client(credentials= credentials, project= credentials.project_id)


base_table_name = 'gifted-chimera-392409.2023_06_01.timetable_count'

query1 = f'''
    CREATE OR REPLACE TABLE `{base_table_name}`
    AS
    SELECT arriveDate, COUNT(*) AS count
    FROM `gifted-chimera-392409.2023_06_01.tts_all`
    GROUP BY arriveDate
    ORDER BY arriveDate;
    
    
'''

job = client.query(query1).result()