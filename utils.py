from hashlib import new
from itertools import count
import requests
import json
import os
import boto3
from requests.structures import CaseInsensitiveDict
from dotenv import load_dotenv
import logging
import os
import time

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

    count_created=0
    count_updated=0
    count_deleted=0
    count_same = 0

    response = s3_client.get_object(Bucket=bucket_name, Key='history/'+office_id+'/key/keys.json')
    file_key =json.loads(response['Body'].read())
    with open("keys/"+office_id+"/keys.json",encoding="utf-8") as file:
        file_key =json.loads(file.read())

    for item in new_list:
        item_identifier = str(item.get("company_code")) if flag=="rub" else str(item.get('code'))+"-"+str(item.get("company_code")) 
        if item_identifier not in indexes_old: 
            item['action']='create'
            count_created+=1
        else:
            old_item = old_list[indexes_old.index(str(item.get("company_code")) if flag=="rub" else str(item.get('code'))+"-"+str(item.get("company_code")))]
            if(has_changed(old_item,item)):

                item['action']='update'
                count_updated+=1
                translate_object(file_key["rubrics"] if flag =="rub" else file_key["employees"] ,item)
            else:
                item['action']='do_nothing'
                count_same+=1
                translate_object(file_key["rubrics"] if flag =="rub" else file_key["employees"],item)
    

    for old_item in old_list:
        code = str(old_item.get('company_code')) if flag=="rub" else str(old_item.get('code'))+"-"+str(old_item.get("company_code"))
        
        if code not in indexes_new:
            old_item['action']='delete'
            count_deleted+=1
            new_list.append(old_item)
            translate_object(file_key["rubrics"] if flag =="rub" else file_key["employees"],old_item)
        else:
            translate_object(file_key["rubrics"] if flag =="rub" else file_key["employees"],old_item)

            old_item['action']='do_nothing'

    base_path= "logs/"+office_id

    if os.path.exists(base_path):
        logging.basicConfig(filename=base_path+"/results.log",encoding="utf-8",format='%(asctime)s %(message)s')
        if flag=="emp":
            logging.warning(f'#=============: Escritório {office_id}(Colaboradores)=========================#')
            logging.warning(f'funcionarios novos: {count_created}')
            logging.warning(f'funcionarios atualizados: {count_updated}')
            logging.warning(f'funcionarios deletados: {count_deleted}')
            logging.warning(f'funcionarios sem alteracao: {count_same}')
        if flag=="rub":
            logging.warning(f'#=============: Escritório {office_id}(Rúbricas)  =========================#')
            logging.warning(f'grupo de rubricas novas: {count_created}')
            logging.warning(f'grupo de rubricas atualizadas: {count_updated}')
            logging.warning(f'grupo de rubricas deletadas: {count_deleted}')
            logging.warning(f'grupo de rubricas sem alteracao: {count_same}')
    else:
        os.makedirs(base_path)
        logging.basicConfig(filename=base_path+"/results.log",encoding="utf-8",format='%(asctime)s %(message)s')
                
        if flag=="emp":
            logging.warning(f'#=============: Escritório(Colaboradores) {office_id}=========================#')
            logging.warning(f'funcionarios novos: {count_created}')
            logging.warning(f'funcionarios atualizados: {count_updated}')
            logging.warning(f'funcionarios deletados: {count_deleted}')
            logging.warning(f'funcionarios sem alteracao: {count_same}')
        if flag=="rub":
            logging.warning(f'#=============: Escritório(Rúbricas) {office_id}=========================#')
            logging.warning(f'grupo de rubricas novas: {count_created}')
            logging.warning(f'grupo de rubricas atualizadas: {count_updated}')
            logging.warning(f'grupo de rubricas deletadas: {count_deleted}')
            logging.warning(f'grupo de rubricas sem alteracao: {count_same}')

    return new_list,old_list
        
#verifica se tem diferença entre os dictionarys
def has_changed(old_item,new_item):
    return old_item != new_item
        

def translate_object(file_key, obj):
    for key in file_key:
        new_key_array = file_key.get(key).split("->")
        sub_value=None
        # primeira parte , pegar o valor original
        keys_array=key.split("->")
        multiple_keys=False
        first_key=""
        if len(keys_array)>1:
            multiple_keys =True
            first_key=keys_array[0]

        if multiple_keys:
            next_key=first_key
            i=0
            value=obj
            while next_key!=keys_array[-1]:
                next_key = keys_array[i]
                i+=1 
                value = value.get(next_key)
                if value==None:
                    break

        else:
            value = obj.get(keys_array[0])
            if obj.get(keys_array[0]):
                del obj[keys_array[0]]
        
        if len(new_key_array)>1:
            # obj[new_key_array[0]] = {}
            string_locator=''
            string_locator_old = ''


        
            if multiple_keys:
                if obj[keys_array[0]]==None:
                    obj[keys_array[0]]={}
                for old_key in keys_array:
                    string_locator_old+='["'+old_key+'"]'
                    try:
                        old_value =  eval("obj"+string_locator_old)


                    except Exception as e:
                        parent_key =string_locator_old.replace('["'+old_key+'"]',"")
                        parent_value = eval("obj"+parent_key)
                        parent_value[old_key]={}
                        
                    
                       
                    
                    

            for key_a in new_key_array:
                string_locator+='["'+key_a+'"]'
                try:
                    new_value = eval("obj"+string_locator)
                except:
                    
                    parent_key =string_locator.replace('["'+key_a+'"]',"")
                    parent_value = eval("obj"+parent_key)
                    parent_value[key_a]={}

            try:
                old_value = eval('obj'+string_locator_old)

               
            except Exception as e:
                
                old_value=None
            

            home_key =string_locator.replace('["'+new_key_array[-1]+'"]',"")
            new_home=eval("obj"+home_key)

            new_home[new_key_array[-1]]=old_value

            # del obj[keys_array[0]]
            
            # old_value = eval("obj"+string_locator_old)
            # del old_value
            
        else:
            obj[new_key_array[0]]=value
       




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
            if rubric_group.get('grupo_rubrica').get("rubricas"):
                for rubric in rubric_group['grupo_rubrica']['rubricas']:
                    del rubric['company_code']
                    translate_object(file_key["rubric_item"],rubric)
    return rubric_list


