
from calendar import c
import os
import boto3
import uuid
import json
from utils import get_data,generate_diff_list,transform_employees_list,transform_rub_list
from datetime import datetime
from dotenv import load_dotenv
import copy
import schedule
import time

load_dotenv()  
session = boto3.Session(
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

s3_client = session.client('s3')
s3_resource = boto3.resource('s3')

def upload_to_s3(bucket_name,data,path):
    bucket_name = os.getenv('BUCKET_SOURCE')
    obj=s3_resource.Object(bucket_name,path)
    obj.put(Body=data)
    

def get_from_s3(bucket_name,path):
    
    try:
        obj= s3_client.get_object(Bucket=bucket_name, Key=path)
        return obj['Body'].read()
    except Exception as e:
        
        return ""

def  get_last_file(prefix):
    bucket_name = os.getenv('BUCKET_SOURCE')
    response = s3_client.list_object_versions(
        Bucket=bucket_name,
        Prefix=prefix,
    )

    versions = response.get('Versions')
    if versions:
        versions.sort(key = lambda x:x.get('LastModified'))
        return json.loads(s3_client.get_object(Bucket=bucket_name, Key=versions[-1]['Key'])['Body'].read())
    return None

def main():
    print("#===== Executando JOB =====#")
    offices = os.getenv('OFFICES').split(',')
    history_bucket =os.getenv('BUCKET_SOURCE')
    import_bucket = os.getenv('BUCKET_IMPORT_OFFICE')

    for office in offices:

        # Atualiza os usuários de cada office
        last_file_response = get_last_file("history/"+office+"/users")
        if last_file_response!=None:
            last_file_content=last_file_response
        else:
            last_file_content=[]
        
        users = get_data(os.getenv('OFFICE_'+office+"_EMP_URL"),os.getenv('OFFICE_'+office+"_API_TOKEN"))
        current_content = copy.deepcopy(users['result']) 
        new_list,old_list=generate_diff_list(users['result'],last_file_content,office,"emp")
        final_response = transform_employees_list(new_list,office)
        file_name = 'users-'+(str(datetime.today())).replace(' ', '-')
        upload_to_s3(history_bucket,json.dumps(current_content,ensure_ascii=False),f"history/{office}/users/{file_name}.json")



        #Atualiza as rubcrias de cada office
        last_file_rubrics_response = get_last_file("history/"+office+"/rubrics")
        if last_file_rubrics_response!=None:
            last_file_content=last_file_rubrics_response
        else:
            last_file_content=[]
        
        rubrics = get_data(os.getenv('OFFICE_'+office+"_RUB_URL"),os.getenv('OFFICE_'+office+"_API_TOKEN"))
        current_content = copy.deepcopy(rubrics['result'])
        new_list,old_list=generate_diff_list(rubrics['result'],last_file_content,office,"rub")
        bucket_name=os.getenv('BUCKET_SOURCE')
        response = s3_client.get_object(Bucket=bucket_name, Key='history/'+office+'/key/keys.json')
        file_key =json.loads(response['Body'].read())
        final_response = transform_rub_list(file_key,new_list)
        file_name = 'users-'+(str(datetime.today())).replace(' ', '-')
        upload_to_s3(history_bucket,json.dumps(current_content,ensure_ascii=False),f"history/{office}/rubrics/{file_name}.json")

    print("#===== FIM DE EXECUÇÃO =====#")
        
        

main()
