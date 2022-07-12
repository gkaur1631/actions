import base64
import requests
import json
import sys
import ingestionerrors
import re
import sqlalchemy
import os
import pymysql
pymysql.install_as_MySQLdb()

from google.cloud import secretmanager
from google.cloud import firestore

project_id =  os.environ['PROJECT_ID']
secret_id  = os.environ['SLACK_WEBHOOK_URL']
secret_id_pwd =os.environ['INGESTION_DB_PSWD_URL']

# Build the parent name from the project.
parent = f"projects/{project_id}"

lastJobId='sample'

    # Create the Secret Manager client.
client = secretmanager.SecretManagerServiceClient()

#fetch the pwd
request = secretmanager.AccessSecretVersionRequest(
        name = f"{parent}/secrets/{secret_id_pwd}/versions/latest",
    )
response = client.access_secret_version(request)
db_pass = response.payload.data.decode("UTF-8")
db_config = {
    "pool_size": 5,
    "max_overflow": 2,
    "pool_timeout": 70,  
    "pool_recycle": 1800,  
}

db_user =os.environ['DB_USER']
db_name = os.environ['DB_NAME']
db_socket_dir =os.environ['DB_SOCKET_DIR'] 
instance_connection_name = os.environ['INGESTION_SQL_INSTANCE_NAME']

pool = sqlalchemy.create_engine(
    sqlalchemy.engine.url.URL.create(
        drivername="mysql+pymysql",
        username=db_user, 
        password=db_pass,  
        database=db_name,  
        query={
            "unix_socket": "{}/{}".format(
                db_socket_dir, 
                instance_connection_name)  
        }
    ),
    **db_config
)

request = secretmanager.AccessSecretVersionRequest(
        name = f"{parent}/secrets/{secret_id}/versions/latest",
    )
response = client.access_secret_version(request)
slack_url = response.payload.data.decode("UTF-8")


def convertGenToArray(gen):
    data = []
    for view in gen:
        view = view.to_dict()
        if 'id' not in view:
            continue
        data.append(view)
    return data


def add_job_id(id):
    db = firestore.Client(project=project_id)
    doc_ref = db.collection(u'dataflowIds').document()
    doc_ref.set(id)

def check_if_id_present(id):
    db = firestore.Client(project=project_id)
    ids_list = db.collection(u'dataflowIds').stream().limit(50)
    data = []
    data = convertGenToArray( ids_list )
    # if length of data 
    if(id in data):
        return True
    else:
        return False


class failed_job:
    def __init__(self, job_id, message):
        self.job_id = job_id
        self.message = message
def notify(failedJob):
    global lastJobId
    if( lastJobId != failedJob.job_id) and not any(msg in failedJob.message for msg in ingestionerrors.toBeSkipped):
        parsedMessage = parse_messages(failedJob.job_id,failedJob.message)
        url = f"{slack_url}"
        message = (f"Job {failedJob.job_id} has failed because {parsedMessage}")
        title = (f"New Incoming Message :zap:")
        lastJobId = failedJob.job_id
        slack_data = {
        "username": "NotificationBot",
        "icon_emoji": ":satellite:",
        "attachments": [
            {
                "color": "#9733EE",
                "fields": [
                    {
                        "title": title,
                        "value": message,
                        "short": "false",
                    }
                ]
            }
        ]
    }
        byte_length = str(sys.getsizeof(slack_data))
        headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
        response = requests.post(url, data=json.dumps(slack_data), headers=headers)
        if response.status_code != 200:
           raise Exception(response.status_code, response.text)

def parse_messages(jobIdStr,msg):
    ingestionId=''
    ingestionFileName=''
    jobId =int(re.search(r'\d+', jobIdStr).group())
    if("gssd-dist-delivery" in jobIdStr):
        ingestionId,ingestionFileName =fetch_ingestion_id_GSSD(jobId)
    elif("gcm-dist-delivery" in jobIdStr):
        ingestionId,ingestionFileName =fetch_ingestion_id_GCM(jobId)
        print(ingestionId)
        print(ingestionFileName)
    return parse_ingestion_messages(msg,ingestionId,ingestionFileName)

def fetch_ingestion_id_GSSD(jobId):
    try:
        with pool.connect() as dbhandler:
            result = dbhandler.execute(f"select id, created_at ,json_extract(request_configuration,'$.fields.cloud_bucket_path.value.fields.key.value') as filename from gssd_first_party_delivery_requests where id={jobId}").fetchall()
            #Find the filename in the GCM req
            for item in result:
                ingestionFileName=getIngestionFileName(item[2])
                ingestionId = findIngestionIdFromFileName(ingestionFileName)
                return ingestionId,ingestionFileName
    except Exception as e:
        print(e)

def fetch_ingestion_id_GCM(jobId):
    try:
        with pool.connect() as dbhandler:
            result=dbhandler.execute(f"select id, created_at ,json_extract(delivery_request_configuration, '$.fields.cloud_bucket_path.value.fields.key.value') as filename from gcm_delivery_requests where id={jobId}").fetchall()
            for item in result:
                ingestionFileName=getIngestionFileName(item[2])
                ingestionId = findIngestionIdFromFileName(ingestionFileName)
                return ingestionId,ingestionFileName
    except Exception as e:
        print(e)

def findIngestionIdFromFileName(name):
    try:
        with pool.connect() as dbhandler:
            result=dbhandler.execute(f"select id from ingestion_requests where filename ='{name}'").fetchall()
            print(result)
            for item in result:
                return item[0]
    except Exception as e:
        print(e)

def  getIngestionFileName(file):
        filename, file_extension = os.path.splitext(file)
        file_extension = file_extension.strip('\"')
        fileParts = file.split("/")
        filename= fileParts[2]
        index= filename.rfind("_INDIVIDUAL_")
        ingestionFileName= filename[:index] + file_extension
        return ingestionFileName


def parse_ingestion_messages(msg,ingestionId="",ingestionFileName=''):
    for error in ingestionerrors.errorMap:
        if error in msg:
            if(ingestionId != ''):
                return (f" {ingestionerrors.errorMap[error]} for ingestionId {ingestionId} and file {ingestionFileName}")
            else:
                return ingestionerrors.errorMap[error]
    else:
        if(ingestionId != ''):
            return (f" of some error that has not been added to my list of known errors, Please ask Global SRE to update the list. its for ingestionID {ingestionId} and file {ingestionFileName} ")
        else:
            return " of some error that has not been added to my list of known errors, Please ask Global SRE to update the list. "


def processErrorMessage(event, context):
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(pubsub_message)
    s = json.dumps(data, indent=4, sort_keys=True)
    payload = data["textPayload"]
    job_id = data["resource"]["labels"]["job_name"]
    if re.match('^Error message from worker:',payload):  
        notify(failed_job(job_id, payload))