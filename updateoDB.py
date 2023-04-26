"""
@date created:  27/10/2021
@updated:       19/04/2023
@author:        Rohan Vangal
@brief:         Back-end data analysis functions to generate tables for the dashboard
@description:   Python pulls data from the input data tables, processes it and uploads relevant data
                to different output tables that can be queried by the dashboard
@notes:         
"""
import logging
import time
import sys

from SqlManager import SqlManager

#Initializing logger
logging.basicConfig(filename= 'updateDB.log',
                    filemode= 'a',
                    format='%(asctime)s %(levelname)s - [%(filename)s : %(lineno)d] %(message)s',
                    datefmt='%Y/%m/%d %H:%M:%S',
                    level=logging.INFO)

upd_tables = 0
if __name__ == '__main__':
    sm = SqlManager()

    while True:
        #Creating output tables if they don't exist
        if upd_tables == 0:
            try:
                sm.create_op_tables()
                upd_tables = 1
            except:
                logging.error('ERROR: Creating output tables failed')
                sys.exit()

        #Populating onevts table
        sm.gen_table_onevts()
        #Populating alarm tables
        sm.gen_table_alarmevts()
        
        time.sleep(10)
