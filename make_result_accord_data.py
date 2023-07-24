from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date, timedelta

# Create a BigQuery client
KEY_PATH = "C:/Users/82105/Downloads/gifted-chimera-392409-099c75ba0e9d.json"
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
client = bigquery.Client(credentials= credentials, project= credentials.project_id)

# Define the start and end dates
start_date = date(2023, 6, 1)
end_date = date(2023, 6, 30)

# Define the base table name
base_table_name = 'gifted-chimera-392409.2023_06_01.tts_all2'

query1 = f'''
    
        CREATE TABLE `{base_table_name}` (
            weekTag INTEGER,
            lineNum STRING,
            stationNm STRING,
            inOutTag INTEGER,
            arriveTime TIME,
            arriveDate DATE
        );
'''

job = client.query(query1).result()
# Loop through dates and create tables
current_date = start_date
while current_date <= end_date:
    # Generate the new table name
    table_name = f'gifted-chimera-392409.2023_06_01.tts_{current_date.strftime("%m%d")}'
    
    # Create a new table using the schema of the base table
    query2 = f'''
        -- timetables 테이블 합치기
        CREATE OR REPLACE TABLE `{base_table_name}`
        AS
        SELECT *
        FROM `{base_table_name}`
        UNION ALL
        SELECT *
        FROM `{table_name}`;
        
    '''
    
    # Execute the query to create the table
    job = client.query(query2).result()
    
    # Move to the next date
    current_date += timedelta(days=1)


query3 = f'''
        --realtime 데이터 전처리 (second 절삭)

        CREATE OR REPLACE TABLE `gifted-chimera-392409.2023_06_01.realtime-final-after`
        AS
        SELECT
        -- 다른 열들도 필요한 경우 여기에 추가합니다
        stationNm,
        FORMAT_TIME('%H:%M:00', arriveTime) AS arriveTime,
        arriveDate,
        weekTag,
        lineNum,
        inOutTag
        -- 원하는 열들을 여기에 추가합니다
        FROM
        `gifted-chimera-392409.2023_06_01.realtime-final-before`;
    
    
        -- 제시간에 도착한 데이터만 추출한 테이블 만들기
        CREATE OR REPLACE TABLE `gifted-chimera-392409.2023_06_01.result_accord_data`
        AS
        SELECT DISTINCT(t1.arriveTime), t1.arriveDate, t1.lineNum, t1.stationNm, t1.inOutTag
        FROM `gifted-chimera-392409.2023_06_01.tts_all` AS t1
        JOIN `gifted-chimera-392409.2023_06_01.realtime-final-after` AS t2
        ON t1.arriveDate = t2.arriveDate
        AND t1.lineNum = t2.lineNum
        AND t1.stationNm = t2.stationNm
        AND t1.inOutTag = t2.inOutTag
        AND STRING(t1.arriveTime) = t2.arriveTime
        ORDER BY lineNum, stationNm, arriveTime;
'''

job = client.query(query3).result()

result = job.result() # 정상 실행 확인
df = job.to_dataframe() #Bigquery에서 가져온 내용을 pandas로 변경
result.total_rows # 가져온 테이블 rows 수 확인
