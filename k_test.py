import string


original = {
    "name":"carlos",
    "code":"108",
    "group_event":None,
    "company":{
        "code":"0008-11"
    },

}


key = {
    "name":"nome",
    "code":"codigo",
    "group_event":"grupo_rubricas",
    "grupo_rubricas->process_type":"grupo_rubricas->tipo_processo->codigo"
} 

def translate(file_key, obj):
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
            obj[new_key_array[0]]={}
            string_locator='["'+new_key_array[0]+'"]'
            string_locator_old ='["'+new_key_array[0]+'"]'
            if multiple_keys:
                for old_key in keys_array[1:-1]:
                    string_locator_old+='["'+new_key_array[0]+'"]'

            for key_a in new_key_array[1:-1]:
                parent_key = eval('obj'+string_locator)
                parent_key[key_a]={}
                string_locator+='["'+key_a+'"]'
            
            new_home = eval('obj'+string_locator)
            new_home[new_key_array[-1]] = value
            #del obj[keys_array[0]]
            old_value = eval("obj"+string_locator_old)
            del old_value
            
        else:
            obj[new_key_array[0]]=value
       

            

translate(key,original)

print(original)