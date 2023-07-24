from google.cloud import storage
from google.oauth2 import service_account
import os
#서비스 계정 인증 정보가 담긴 JSON 파일 경로
#KEY_PATH = "C:/Users/82105/Downloads/gifted-chimera-392409-099c75ba0e9d.json"

#Credentials 객체 생성
#credentials = service_account.Credentials.from_service_account_file(KEY_PATH)

#구글 스토리지 클라이언트 객체 생성
#client = storage.Client(credentials= credentials, project= credentials.project_id)


def upload_to_gcs(local_path, gcs_bucket, file_path):
    # 인증 및 클라이언트 초기화
    KEY_PATH = "C:/Users/82105/Downloads/gifted-chimera-392409-099c75ba0e9d.json"
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = storage.Client(credentials= credentials, project= credentials.project_id)
    bucket = client.bucket(gcs_bucket)
    
    
    gsc_path = os.path.join(gcs_folder, date_folder, os.path.basename(local_path))
    #print(gcs_folder)
    # 로컬 파일을 GCS에 업로드
    print(file_path)
    blob_name = f"{file_path}"
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)

    #print(f"{local_path}을(를) {gcs_bucket} 버킷의 {blob_name} 경로로 업로드했습니다.")

# 업로드할 로컬 폴더 경로
local_folder = "C:/Users/82105/BOAZ_ADV/realtime"

# 업로드할 GCS 버킷 및 폴더 경로
gcs_bucket = "bmt-realtime"
gcs_folder = "realtime"

# 로컬 폴더 내의 .json 파일 업로드
'''import os
for folder_name in os.listdir(local_folder):
    for filename in os.listdir(local_folder/folder_name):
        print(filename)
        if filename.endswith(".json"):
            local_path = os.path.join(local_folder, filename)
            print(local_path)
            upload_to_gcs(local_path, gcs_bucket, gcs_folder)
'''
# 로컬 폴더 내의 파일들을 재귀적으로 탐색하여 GCS에 업로드
for root, dirs, files in os.walk(local_folder):
    for file in files:
        if file.endswith(".json"):
            local_path = os.path.join(root, file)
            date_folder = os.path.basename(root)
            file_path = os.path.basename(file)
            #print(file_path)
            upload_to_gcs(local_path, gcs_bucket, file_path)

