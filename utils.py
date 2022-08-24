import requests
import json
import os
import boto3
from requests.structures import CaseInsensitiveDict
from dotenv import load_dotenv
load_dotenv()  

session = boto3.Session(
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

s3_client = session.client('s3')


def get_data(url,token):
    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers["Authorization"] = f"Bearer {token}"
    resp = requests.post(url,headers=headers)
    return resp.json()




def generate_diff_list(new_list,old_list,office_id,flag):
    bucket_name=os.getenv('BUCKET_SOURCE')
    indexes_old=[str(item.get("company_code")) if flag=="rub" else str(item.get('code'))+"-"+str(item.get("company_code")) for item in old_list]
    indexes_new=[str(item.get("company_code")) if flag=="rub" else str(item.get('code'))+"-"+str(item.get("company_code")) for item in new_list]


    response = s3_client.get_object(Bucket=bucket_name, Key='history/'+office_id+'/key/keys.json')
    file_key =json.loads(response['Body'].read())

    for item in new_list:
        item_identifier = str(item.get("company_code")) if flag=="rub" else str(item.get('code'))+"-"+str(item.get("company_code")) 
        if item_identifier not in indexes_old: 
            item['action']='create'
        else:
            old_item = old_list[indexes_old.index(str(item.get("company_code")) if flag=="rub" else str(item.get('code'))+"-"+str(item.get("company_code")))]
            if(has_changed(old_item,item)):
                item['action']='update'
                translate_object(file_key,item)
            else:
               
                item['action']='do_nothing'
                translate_object(file_key,item)

    for old_item in old_list:
        code = str(old_item.get('company_code')) if flag=="rub" else str(old_item.get('code'))+"-"+str(old_item.get("company_code"))
        
        if code not in indexes_new:
            old_item['action']='delete'
            new_list.append(old_item)
            translate_object(file_key,old_item)
        else:
            translate_object(file_key,old_item)
            old_item['action']='do_nothing'

    return new_list,old_list
        
#verifica se tem diferença entre os dictionarys
def has_changed(old_item,new_item):
    return old_item != new_item
        

def translate_object(file_key,obj):
    list_keys = file_key
    for key in list_keys:

        if obj.get(key):
            obj[list_keys[key]]=obj.get(key)
            del obj[key]

def transform_employees_list(employee_list,office_id):
    companies_codes=[]
    companies=[]
    for employee in employee_list:
        if employee.get('codigo') in companies_codes:
            companies[companies_codes.index(employee.get('codigo'))]['empregado'].append(
                employee
            )
        else:
            company = {
                "codigo":employee.get('codigo'),
                "nome":employee.get('company_name'),
                "status":employee.get('status'),
                "inscricao":employee.get('inscricao'),
                "empregado":[]
            }
            companies_codes.append(employee.get('codigo'))
            companies.append(company)
            
            companies[companies_codes.index(employee.get('codigo'))]['empregado'].append(
                employee
            )


    return companies

def transform_rub_list(file_key,rubric_list):
    for rubric_group in rubric_list:
        if rubric_group.get('grupo_rubrica'):
            translate_object(file_key,rubric_group['grupo_rubrica'])
            for rubric in rubric_group['grupo_rubrica']['rubricas']:
                del rubric['company_code']
                translate_object(file_key,rubric)
    return rubric_list

