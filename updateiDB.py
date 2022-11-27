"""
@date:          23/11/2022
@author:        Rohan Vangal
@brief:         Back-end function to update database with data from multiple sites and machines via MQTT broker 
@description:   
@notes:         
"""

from sqlalchemy import create_engine
import yaml
import logging
from paho.mqtt.client import Client
import sys
import time
# import json


# from sqlalchemy.sql.elements import Null

from MqttManager import MqttManager

#Initializing logger
logging.basicConfig(filename= 'updateDB.log',
                    filemode= 'a',
                    format='%(asctime)s %(levelname)s - [%(filename)s : %(lineno)d] %(message)s',
                    datefmt='%Y/%m/%d %H:%M:%S',
                    level=logging.INFO)

# To schedule update of TagList
start_time = time.time()

if __name__ == '__main__':
    mqttMgr = MqttManager()
    mqttMgr.regenClients()
    while(True):
        mqttMgr.insertData()
        # Regenerate topic list if timer expires
        start_time = mqttMgr.regenClients(start_time)