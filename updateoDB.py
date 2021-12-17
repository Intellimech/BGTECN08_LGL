"""
@date:          27/10/2021
@author:        Rohan Vangal
@brief:         Back-end data analysis functions to generate tables for the dashboard
@description:   Python pulls data from the input data tables, processes it and uploads relevant data
                to different output tables that can be queried by the dashboard
@notes:         
"""
# TODO: Add missing dates with zero values

import time
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

### FUNCTIONS ##################################################################################################
#region

#Function to get the number of ON/OFF events 
def getevents (inp, evt = 1):
    if evt == 0: evt = -1
    if evt == 1: evt = 1
    num_evts = np.count_nonzero(np.diff(inp) == evt)
    return (num_evts)

#region
#Function to get ON hours based on timestamps and the start and end of events
# def getonhrs (inp,timestamps):
#     evt_diff = np.diff(inp)
#     mones = np.argwhere(evt_diff == -1)
#     ones = np.argwhere(evt_diff == 1)
    
#     if mones[0] > ones[0]:
#         ones = [0] + ones
#     if ones[-1] > mones[-1]:
#         mones = mones + [len(inp)-1]
        
#     num_hrs = = divmod(duration_in_s, 3600)[0] sum(timestamps[mones] - timestamps[ones])
#endregion

#Function to get ON/OFF hours based on number of readings
def gethours (inp, evt = 1, obs_t = 0.0166):     
    num_hrs = float(np.count_nonzero(inp == evt)) * obs_t  
    return (num_hrs)

#Function to get the most recent date in the table for query
def getdates (db_engine, tablename):
    db_metadata = MetaData(bind = db_engine)
    db_metadata.reflect()
    
    with db_engine.connect() as connection:
        with connection.begin():
            table = db_metadata.tables[tablename]            
            stmt = (select(func.max(table.c.timestamp)))
            op = connection.execute(stmt).fetchall()
    
    if op[0][0] is None:
        start_date = dt.datetime(2020, 1, 1)
    else:
        start_date = op[0][0]

    #Start date of update cycle started 24 hrs before current date to include new readings on the current date
    start_date = start_date - dt.timedelta(days = 1)
    end_date = getenddate()
    return (start_date, end_date)

#Function to get tomorrow's date for query
def getenddate():
    end_date = dt.date.today() + dt.timedelta(days = 1)
    return (end_date)

#Function to get ON events and hours for each day
def gen_table_onevts(db_engine,start_date,end_date,ore_pot_turn = 24):

    db_metadata = MetaData(bind = db_engine)
    db_metadata.reflect()

    #Get data from the DB using SQL
    # query = 'SELECT\
    #             Id,\
    #             timestamp,\
    #             machine_status \
    #         FROM Acquisizioni \
    #         WHERE (machine_status REGEXP \'[0-1]\') \
    #             AND (timestamp BETWEEN \''+ start_date +'\' AND \''+ end_date +'\') \
    #         ORDER BY Id' 

    #Get data from the DB using SQLAlchemy
    itable = db_metadata.tables['Acquisizioni']
    query = select(
                    itable.c.Id,
                    itable.c.timestamp,
                    itable.c.machine_status).\
            where(and_(
                    itable.c.machine_status.regexp_match('[0-1]'),
                    itable.c.timestamp.between(start_date,end_date))).\
            order_by(itable.c.Id)

    df = pd.read_sql(query,db_engine)

    datelist = sorted(pd.unique(df['timestamp'].dt.date))
    if len(datelist) != 0:
        for adate in datelist:
            temp = df[(df['timestamp'].dt.date == adate)]
            
            on_events = getevents(temp['machine_status'].str.strip().astype(int).to_numpy())
            
            on_hrs = gethours(temp['machine_status'].to_numpy().astype(int))
            date_val = adate.strftime("%Y-%m-%d")

            with db_engine.connect() as connection:
                with connection.begin():
                    table = db_metadata.tables['op_onevts']            
                    s1 = (insert(table).
                        values( timestamp = date_val,
                                date = date_val,
                                on_evts = on_events,
                                on_hrs = on_hrs,
                                efficiency = (on_hrs/ore_pot_turn)*100 if (ore_pot_turn != 0) else None))
                    s2 = s1.on_duplicate_key_update(on_evts = on_events,
                                                    on_hrs = on_hrs,
                                                    efficiency = (on_hrs/ore_pot_turn)*100 if (ore_pot_turn != 0) else None)
                    connection.execute(s2)

#Function to get alarm events for each day at the machine level
def gen_table_alarmevts(db_engine,start_date,end_date, obs_time = 0.0166):
    db_metadata = MetaData(bind = db_engine)
    db_metadata.reflect()

    alarm_dic = {'Clean Phot': 'Clean_Phot',
                 'ElBrk Open': 'ElBrk_Open',
                 'Feeder not found': 'Feeder_not_found',
                 'Motor Lock': 'Motor_Lock',
                 'OYB Err': 'OYB_Err',
                 'PrewindErr': 'PrewindErr',
                 'SMRangeErr': 'SMRangeErr',
                 'Switch OFF': 'Switch_OFF',
                 'TensmtrErr': 'TensmtrErr',
                 'Yarn Break': 'Yarn_Break'}

    #Get data from the DB using SQL
    # query = 'SELECT\
	#             d.ID_aq,\
    #             a.timestamp,\
    #             d.ID_feeder,\
    #             d.Status\
    #         FROM dati AS d\
    #         JOIN Acquisizioni AS a\
    #         	ON d.ID_aq = a.Id\
    #         WHERE (d.Status NOT LIKE \'%%OK%%\') AND \
    #               (d.Status != \'\') AND \
    #               (timestamp BETWEEN \''+ start_date +'\' AND \''+ end_date +'\')\
    #         ORDER BY d.ID_aq'

    #Get data from the DB using SQLAlchemy
    t_dati = db_metadata.tables['dati']
    t_acq = db_metadata.tables['Acquisizioni']
    query = select(
                    t_dati.c.ID_aq,
                    t_acq.c.timestamp,
                    t_dati.c.ID_feeder,
                    t_dati.c.Status).\
            join(t_acq, t_dati.c.ID_aq == t_acq.c.Id).\
            where(and_(
                    t_dati.c.Status.notlike('%OK%'),
                    t_dati.c.Status != '',
                    t_acq.c.timestamp.between(start_date,end_date))).\
            order_by(t_dati.c.ID_aq)

    df = pd.read_sql(query,db_engine)
        
    datelist = sorted(pd.unique(df['timestamp'].dt.date))
    
    if len(datelist) != 0:
        df['Status'] = df['Status'].str.strip()
        for adate in datelist:
            temp = df[(df['timestamp'].dt.date == adate)]
            date_val = adate.strftime("%Y-%m-%d")

            alarmlist = pd.unique(temp['Status'])
            for aalarm in alarmlist:
                temp1 = temp[(temp['Status'].str.strip() == aalarm)]
                         
                evt_sum = 0
                hrs_sum = 0
                feederlist = pd.unique(temp1['ID_feeder'])
                for afeeder in feederlist:
                    temp2 = temp1[(temp1['ID_feeder'] == afeeder)]
                    templist = np.diff(temp2['ID_aq'].to_numpy().astype(int))
                    hrs_sum = hrs_sum + (((np.count_nonzero(templist))+1) * obs_time)
                    evt_sum = evt_sum + (np.count_nonzero(templist > 1)+1)
                with db_engine.connect() as connection:
                    with connection.begin():
                        table = db_metadata.tables['op_alarmevts']
                        insquery = (insert(table).
                            values({
                                'timestamp': date_val,
                                'date': date_val,
                                alarm_dic[aalarm] : evt_sum
                                })
                            ).on_duplicate_key_update({
                                alarm_dic[aalarm] : evt_sum
                                })
                        connection.execute(insquery) 

                        table = db_metadata.tables['op_alarmhrs'] 
                        insquery = (insert(table).
                            values({
                                'timestamp': date_val,
                                'date': date_val,
                                alarm_dic[aalarm] : hrs_sum
                                })
                            ).on_duplicate_key_update({
                                alarm_dic[aalarm] : hrs_sum
                                })
                        connection.execute(insquery)       

#Function to check existence of tables and create them if they don't exist
def create_op_tables(db_engine, table_list):
    for tablename in table_list:
        with db_engine.connect() as connection:
            with connection.begin():
                if not db_engine.dialect.has_table(connection, tablename):  # If table doesn't exist, Create.
                    metadata = MetaData(db_engine)
                    #TODO: Replace with 'match' case in Python 3.10
                    # Create a table with the appropriate Columns
                    if tablename == 'op_onevts':
                        Table(tablename, metadata,
                            Column('timestamp', DateTime, primary_key=True, nullable=False, unique= True), 
                            Column('date', String(20), nullable=False, unique= True),
                            Column('on_evts', Integer, nullable=True),
                            Column('on_hrs', Float, nullable=True),
                            Column('efficiency', Float, nullable=True))
                    elif tablename == 'op_alarmevts':
                        Table(tablename, metadata,
                            Column('timestamp', DateTime, primary_key=True, nullable=False, unique= True), 
                            Column('date', String(20), nullable=False, unique= True),
                            Column('Clean_Phot', Integer, server_default = '0'),
                            Column('ElBrk_Open', Integer, server_default = '0'),
                            Column('Feeder_not_found', Integer, server_default = '0'),
                            Column('Motor_Lock', Integer, server_default = '0'),
                            Column('OYB_Err', Integer, server_default = '0'),
                            Column('PrewindErr', Integer, server_default = '0'),
                            Column('SMRangeErr', Integer, server_default = '0'),
                            Column('Switch_OFF', Integer, server_default = '0'),
                            Column('TensmtrErr', Integer, server_default = '0'),
                            Column('Yarn_Break', Integer, server_default = '0'))
                    elif tablename == 'op_alarmhrs':
                        Table(tablename, metadata,
                            Column('timestamp', DateTime, primary_key=True, nullable=False, unique= True), 
                            Column('date', String(20), nullable=False, unique= True),
                            Column('Clean_Phot', Float, server_default = '0'),
                            Column('ElBrk_Open', Float, server_default = '0'),
                            Column('Feeder_not_found', Float, server_default = '0'),
                            Column('Motor_Lock', Float, server_default = '0'),
                            Column('OYB_Err', Float, server_default = '0'),
                            Column('PrewindErr', Float, server_default = '0'),
                            Column('SMRangeErr', Float, server_default = '0'),
                            Column('Switch_OFF', Float, server_default = '0'),
                            Column('TensmtrErr', Float, server_default = '0'),
                            Column('Yarn_Break', Float, server_default = '0'))
                    elif tablename == 'op_params':
                        Table(tablename, metadata,
                            Column('timestamp', DateTime, primary_key=True, nullable=False), 
                            Column('feeder', Integer, primary_key=True, nullable=False),
                            Column('T_des', Integer, nullable=True),
                            Column('T_read', Integer, nullable=True),
                            Column('Sm_steps', Integer, nullable=True),
                            Column('Real_speed', Integer, nullable=True),
                            Column('Torque', Integer, nullable=True),
                            Column('Ftc_out', Integer, nullable=True),
                            Column('Ftc_in', Integer, nullable=True),
                            Column('Meter_cons', Integer, nullable=True),
                            Column('Vbus_motor', Integer, nullable=True),
                            Column('T_offset', Integer, nullable=True))
                    else:
                        print(f'ERROR: Table "{tablename}" in list not configured!')
                        continue
                    # Implement the creation
                    metadata.create_all()
    
#Function to get machine parameters
def gen_table_params(db_engine,start_date,end_date):
    db_metadata = MetaData(bind = db_engine)
    db_metadata.reflect()
    with db_engine.connect() as connection:
        with connection.begin():
            #Get data from the DB using SQLAlchemy
            t_dati = db_metadata.tables['dati']
            t_acq = db_metadata.tables['Acquisizioni']
            t_op = db_metadata.tables['op_params']
            sel = select(
                            t_acq.c.timestamp,
                            t_dati.c.ID_feeder.label('feeder'),
                            t_dati.c.T_des,
                            t_dati.c.T_read,
                            t_dati.c.Sm_steps,
                            t_dati.c.Real_speed,
                            t_dati.c.Torque,
                            t_dati.c.Ftc_out,
                            t_dati.c.Ftc_in,
                            t_dati.c.Meter_cons,
                            t_dati.c.Vbus_motor,
                            t_dati.c.T_offset).\
                    join(t_acq, t_dati.c.ID_aq == t_acq.c.Id).\
                    where(t_acq.c.timestamp.between(start_date,end_date)).\
                    order_by(t_acq.c.timestamp)
            insert_stmt = insert(t_op).\
                    from_select([   'timestamp',
                                    'feeder',
                                    'T_des',
                                    'T_read',
                                    'Sm_steps',
                                    'Real_speed',
                                    'Torque',
                                    'Ftc_out',
                                    'Ftc_in',
                                    'Meter_cons',
                                    'Vbus_motor',
                                    'T_offset'], sel)
            on_conflict_stmt = insert_stmt.on_duplicate_key_update(
                                    T_des = insert_stmt.inserted.T_des,
                                    T_read = insert_stmt.inserted.T_read,
                                    Sm_steps = insert_stmt.inserted.Sm_steps,
                                    Real_speed = insert_stmt.inserted.Real_speed,
                                    Torque = insert_stmt.inserted.Torque,
                                    Ftc_out = insert_stmt.inserted.Ftc_out,
                                    Ftc_in = insert_stmt.inserted.Ftc_in,
                                    Meter_cons = insert_stmt.inserted.Meter_cons,
                                    Vbus_motor = insert_stmt.inserted.Vbus_motor,
                                    T_offset = insert_stmt.inserted.T_offset)
            connection.execute(on_conflict_stmt)
#endregion

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
