import logging
import os

base_path= "logs/0008/"

if os.path.exists(base_path):
    logging.basicConfig(filename=base_path+"example.log",encoding="utf-8",format='%(asctime)s %(message)s')
    logging.warning('is when this event was logged.')
else:
    os.makedirs(base_path)
    logging.basicConfig(filename=base_path+"example.log",encoding="utf-8",format='%(asctime)s %(message)s')
    logging.warning('is when this event was logged.')