
def translate_object(file_key,obj):
    list_keys = file_key
    possible_keys = [key for key in obj]
    for key in file_key:
        if key in possible_keys:
            obj[list_keys[key]]=obj.get(key)
            del obj[key]

keys = {
    "car":"carro",
    "board":"placa"
}

obj={
    "car":"Ford Ka",
    "board":None
}

def test_translation():
    translate_object(keys,obj)
    assert obj["carro"]!=None

def test_translation_null_key():
    translate_object(keys,obj)
    assert  obj["placa"]==None

