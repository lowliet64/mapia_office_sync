
from utils import generate_diff_list,transform_employees_list
import json
import copy


with open('results/employees.json','r',encoding="utf-8") as file:

    lista_1 = json.loads(file.read())
    lista_2 = copy.deepcopy(lista_1)

    new_list,old_list =  generate_diff_list(lista_1,lista_2,"0008","emp")
    final_response = transform_employees_list(new_list,"0008")


with open('results/final.json','w',encoding='utf-8') as file:
    file.write(json.dumps(final_response,ensure_ascii=False))