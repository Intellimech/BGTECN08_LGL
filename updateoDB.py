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

import numpy as np
import pandas as pd
# import pymysql
from sqlalchemy import create_engine, MetaData, func, select, and_,\
     Table, Column, String, DateTime, Integer, Float, DefaultClause
from sqlalchemy.dialects.mysql import insert
# from sqlalchemy.sql.elements import not_
# from sqlalchemy.sql.sqltypes import DATETIME
import datetime as dt
import yaml

### USER DEFINED ###############################################################################################
#region

#Class of variables from config file
class configs:
    def __init__(self) -> None:
        # Get global variables from config file
        with open('config.yaml') as file:
            # Conversion from YAML to Python dictionary format
            configs = yaml.load(file, Loader=yaml.FullLoader)

        # Constants for updating input data tables
        conn = f"mysql+pymysql://{configs['db']['username']}:{configs['db']['password']}@{configs['db']['ip']}/{configs['db']['schema']}"
        self.engine = create_engine(conn, echo = False)

        self.t_acq = configs['db']['tab_acq']
        self.t_dat = configs['db']['tab_dat']
        self.op_tables = configs['db']['op_tables']

        self.start_date_flag = configs['db']['start_date_flag']
        self.t_obs = configs['db']['t_obs']
        self.shift_num = configs['db']['shift_num']
        self.shift_hrs = configs['db']['shift_hrs']
        #Ore potenziale di turno
        self.ore_pot_turn = self.shift_hrs*self.shift_num
        try:
            self.start_date_default = dt.datetime.strptime(configs['db']['start_date_default'], '%d/%m/%Y').strftime('%Y-%m-%d') + " 00:00:00"
        except:
            print('ERROR: Default date incompatible')
            #Make default date 01/01/2000
            self.start_date_default = dt.datetime.strptime("01/01/2000", '%d/%m/%Y').strftime('%Y-%m-%d') + " 00:00:00"

#endregion

#Initializing logger
logging.basicConfig(filename= 'updateDB.log',
                    filemode= 'a',
                    format='%(asctime)s %(levelname)s - [%(filename)s : %(lineno)d] %(message)s',
                    datefmt='%Y/%m/%d %H:%M:%S',
                    level=logging.INFO)

upd_tables = 0
if __name__ == '__main__':
    ### RUN #########################################################################################################
    while True:
        #region
        try:
            #Getting defaults ESSENTIAL
            cfg = configs()
        except:
            print('ERROR: Reading YAML file failed')
            quit()

        #Creating output tables if they don't exist
        if upd_tables == 0:
            try:
                create_op_tables(cfg.engine, cfg.op_tables)
                upd_tables = 1
            except:
                print('ERROR: Creating output tables failed')
                quit()

        #Check if start date is a preset value
        if cfg.start_date_flag == 0:
            start_date = cfg.start_date_default
            end_date = getenddate()

            #Populating onevts table
            gen_table_onevts(cfg.engine, start_date, end_date, ore_pot_turn = cfg.ore_pot_turn)
            #Populating alarm tables
            gen_table_alarmevts(cfg.engine,start_date,end_date, obs_time= cfg.t_obs)
            #Populating parameter table
            gen_table_params(cfg.engine,start_date,end_date)

        #Check if start date is a dynamic value
        else:
            (start_date,end_date) = getdates (cfg.engine, "op_onevts")
            #Populating onevts table
            gen_table_onevts(cfg.engine,start_date,end_date,ore_pot_turn = cfg.ore_pot_turn)
        
            #TODO: Check dates between op_alarmevts and op_alarmhrs which are both updated by gen_table_alarmevts
            (start_date,end_date) = getdates (cfg.engine, "op_alarmevts")
            #Populating alarm tables
            gen_table_alarmevts(cfg.engine,start_date,end_date, obs_time= cfg.t_obs)
            
            (start_date,end_date) = getdates (cfg.engine, "op_params")
            #Populating parameter table
            gen_table_params(cfg.engine,start_date,end_date)
        #endregion
        time.sleep(10)
