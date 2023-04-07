"""
@date:          23/11/2022
@author:        Rohan Vangal
@brief:         Back-end function to update database with data from multiple sites and machines via MQTT broker 
@description:   
@notes:         
"""

import logging
import time
import sys

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
    # Initialize manager object
    mgr = MqttManager()
    # Generate mqtt clients for each topics
    mgr.regenClients()
    while(True):
        # Update database if data is found in the queue
        mgr.insertData()
        # Regenerate topic list if timer expires
        start_time = mgr.regenClients(start_time)
        if(not mgr.run_flag):
            break
    mgr.stopClients()
    sys.exit()