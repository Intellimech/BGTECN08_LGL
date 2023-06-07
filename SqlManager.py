import yaml
import logging
import sys
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, MetaData, func, select, and_,\
     Table, Column, String, DateTime, Integer, Float, DefaultClause, ForeignKey, inspect       
from sqlalchemy.dialects.mysql import insert
# from sqlalchemy.sql.elements import not_
# from sqlalchemy.sql.sqltypes import DATETIME
import datetime as dt
import yaml

class SqlManager:
    """ Class to manage SQL queries and operations on the database
    """
    def __init__(self) -> None:
        """ Get constants from config file
        """
        configs = {}
        try:
            with open('configs.yaml') as file:
                # Conversion from YAML to Python dictionary format
                configs = yaml.load(file, Loader=yaml.FullLoader)
            
            # Constants for updating input data tables
            conn = f"mysql+pymysql://{configs['db']['username']}:{configs['db']['password']}@{configs['db']['ip']}/{configs['db']['schema']}"
            self.engine = create_engine(conn, echo = False)
            self.schema = {configs['db']['schema']}

            self.t_acq = configs['db']['tab_acq']
            self.t_dat = configs['db']['tab_dat']
            self.op_tables = configs['db']['op_tables']

            self.alarm_list = configs['db']['alarm_list']

            self.start_date_flag = configs['db']['start_date_flag']
            self.t_obs = configs['db']['t_obs']
            self.shift_num = configs['db']['shift_num']
            self.shift_hrs = configs['db']['shift_hrs']
            #Ore potenziale di turno
            self.ore_pot_turn = self.shift_hrs*self.shift_num
            try:
                self.start_date_default = dt.datetime.strptime(configs['db']['start_date_default'], '%d/%m/%Y').strftime('%Y-%m-%d') + " 00:00:00"
            except:
                logging.warning('ERROR: Default date incompatible')
                #Make default date 01/01/2000
                self.start_date_default = dt.datetime.strptime("01/01/2000", '%d/%m/%Y').strftime('%Y-%m-%d') + " 00:00:00" 
        except:
            logging.warning('Reading configuration file failed. Using default values')
            
            #Default values
            conn = f"mysql+pymysql://lglUsr2021:lglPwd1202@192.168.3.225/prLgl"
            self.engine = create_engine(conn, echo = False)
            self.schema = "prLgl"
            self.t_acq = "Acquisizioni"
            self.t_dat = "dati"
            self.op_tables = ["op_onevts","op_alarmevts","op_alarmhrs","op_params"]

            self.start_date_flag = 0
            self.t_obs = 0.0166
            self.shift_num = 3
            self.shift_hrs = 8
            #Ore potenziale di turno
            self.ore_pot_turn = self.shift_hrs*self.shift_num
            self.start_date_default = dt.datetime.strptime("01/01/2000", '%d/%m/%Y').strftime('%Y-%m-%d') + " 00:00:00" 
               
    def getevents (self, inp, evt = 1):
        """ Function to get the number of ON/OFF events
            @param inp: Input array
            @param evt: Event to count (0 = OFF, 1 = ON)
            @return   : Number of events
        """
        if evt == 0: evt = -1
        if evt == 1: evt = 1
        num_evts = np.count_nonzero(np.diff(inp) == evt)
        return (num_evts)

    def gethours (self, inp, evt = 1): 
        """ Function to get ON/OFF hours based on number of readings
            @param inp: Input array
            @param evt: Event to count (0 = OFF, 1 = ON)
            @return   : Number of hours
        """   
        num_hrs = float(np.count_nonzero(inp == evt)) * self.t_obs  
        return (num_hrs)

    def getdates (self, tablename, machine_id = None):
        """ Function to get the most recent date in the table for query
            @param tablename: Name of the table to query
            @param machine_id: ID of the machine to query
            @return   : Start and end dates for query       
        """
        db_metadata = MetaData(bind = self.engine)
        db_metadata.reflect()
        
        with self.engine.connect() as connection:
            with connection.begin():
                table = db_metadata.tables[tablename] 
                if machine_id is None:
                    # stmt = (select(func.max(table.c.timestamp)))
                    subquery = select([table.c.MachineDescriptor, func.max(table.c.timestamp).label('timestamp')]).group_by(table.c.MachineDescriptor)
                    stmt = (select(func.min(subquery.c.timestamp)))
                else:
                    stmt = (select(func.max(table.c.timestamp)).where(table.c.MachineID == machine_id))
                op = connection.execute(stmt).fetchall()
        
        if op[0][0] is None:
            start_date = self.start_date_default
            # start_date = dt.datetime(2020, 1, 1)
        else:
            start_date = op[0][0]

        #Start date of update cycle started 24 hrs before current date to include new readings on the current date
        start_date = start_date - dt.timedelta(days = 1)
        end_date = self.getenddate()
        return (start_date, end_date)

    def getenddate(self):
        """ Gets tomorrow's date for query
            @param: None
            @return: Tomorrow's date
        """
        end_date = dt.date.today() + dt.timedelta(days = 1)
        return (end_date)

    def gen_table_onevts(self,start_date = None, end_date = None):
        """ Generate table to store daily count of ON/OFF events
            @param start_date: Start date of query
            @param end_date: End date of query    
            @return: None
        """
        if start_date is None or self.start_date_flag == 0:
            start_date = self.start_date_default
            end_date = self.getenddate()
        else:
            (start_date,end_dt) = self.getdates ("op_onevts")
            if end_date is None:
                end_date = end_dt

        db_metadata = MetaData(bind = self.engine)
        db_metadata.reflect()
#region
        #Get data from the DB using SQL
        # query = 'SELECT\
        #             Id,\
        #             timestamp,\
        #             machine_status \
        #         FROM Acquisizioni \
        #         WHERE (machine_status REGEXP \'[0-1]\') \
        #             AND (timestamp BETWEEN \''+ start_date +'\' AND \''+ end_date +'\') \
        #         ORDER BY Id' 
#endregion
        #Get data from the DB using SQLAlchemy
        itable = db_metadata.tables['Acquisizioni']
        query = select(
                        itable.c.Id,
                        itable.c.timestamp,
                        itable.c.MachineID,
                        itable.c.MachineDescriptor,
                        itable.c.machine_status).\
                where(and_(
                        itable.c.machine_status.regexp_match('[0-1]'),
                        itable.c.MachineID.is_not(None),
                        itable.c.MachineDescriptor.is_not(None),
                        itable.c.timestamp.between(start_date,end_date))).\
                order_by(itable.c.Id)

        dframe = pd.read_sql(query,self.engine)
        
        for (mid,mdesc) in dframe[['MachineID','MachineDescriptor']].apply(tuple,axis=1).unique():
            df = dframe[(dframe['MachineID']== mid) & (dframe['MachineDescriptor']== mdesc)]
                        
            datelist = sorted(pd.unique(df['timestamp'].dt.date))
            if len(datelist) != 0:
                for adate in datelist:
                    temp = df[(df['timestamp'].dt.date == adate)]
                    
                    on_events = self.getevents(temp['machine_status'].str.strip().astype(int).to_numpy())
                    
                    on_hrs = self.gethours(temp['machine_status'].to_numpy().astype(int))
                    date_val = adate.strftime("%Y-%m-%d")

                    with self.engine.connect() as connection:
                        with connection.begin():
                            table = db_metadata.tables['op_onevts']            
                            s1 = (insert(table).
                                values( timestamp = date_val,
                                        MachineID = mid,
                                        MachineDescriptor = mdesc,
                                        date = date_val,
                                        on_evts = on_events,
                                        on_hrs = on_hrs,
                                        efficiency = (on_hrs/self.ore_pot_turn)*100 if (self.ore_pot_turn != 0) else None))
                            s2 = s1.on_duplicate_key_update(on_evts = on_events,
                                                            on_hrs = on_hrs,
                                                            efficiency = (on_hrs/self.ore_pot_turn)*100 if (self.ore_pot_turn != 0) else None)
                            connection.execute(s2)

    def gen_table_alarmevts(self,start_date = None, end_date = None):
        """ Updates table with alarm events for each day at the machine level
            @param start_date: Start date of query
            @param end_date: End date of query
            @return: None
        """
        if 'op_alarmevts' not in self.op_tables and 'op_alarmhrs' not in self.op_tables:
            return
        
        if start_date is None or self.start_date_flag == 0:
            start_date = self.start_date_default
            end_date = self.getenddate()
        else:
            (start_date,end_dt) = self.getdates("op_alrmevts")
            if end_date is None:
                end_date = end_dt

        # if start_date is None:
        #     start_date = self.start_date_default
        # if end_date is None:
        #     end_date = self.getenddate()

        db_metadata = MetaData(bind = self.engine)
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
                    'Yarn Break': 'Yarn_Break',
                    'Time Error': 'Time_Error',
                    'VBMot Fail': 'VBMot_Fail',
                    'Force Stop': 'Force_Stop',
                    'I2t Error' : 'I2t_Error',
                    'Cell Fault' : 'Cell_Fault',
                    'IOffsetErr' : 'IOffsetErr',
                    'MotHallErr' : 'MotHallErr',
                    'BrkLimit': 'BrkLimit',
                    'E2p NotIni' :'E2pNotIni',
                    'CellOffErr': 'CellOffErr',
                    'YCC Alarm' : 'YCC_Alarm'}
#region
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
#endregion
        #Get data from the DB using SQLAlchemy
        t_dati = db_metadata.tables['dati']
        t_acq = db_metadata.tables['Acquisizioni']
        query = select(
                        t_dati.c.ID_aq,
                        t_acq.c.timestamp,
                        t_acq.c.MachineID,
                        t_acq.c.MachineDescriptor,
                        t_dati.c.ID_feeder,
                        t_dati.c.Status).\
                join(t_acq, t_dati.c.ID_aq == t_acq.c.Id).\
                where(and_(
                        t_dati.c.Status.notlike('%OK%'),
                        t_dati.c.Status != '',
                        t_acq.c.MachineID.is_not(None),
                        t_acq.c.MachineDescriptor.is_not(None),
                        t_acq.c.timestamp.between(start_date,end_date))).\
                order_by(t_dati.c.ID_aq)

        dframe = pd.read_sql(query,self.engine)
        dframe['Status'] = dframe['Status'].str.strip()

        #For each machine found in the query
        for (mid,mdesc) in dframe[['MachineID','MachineDescriptor']].apply(tuple,axis=1).unique():
            df = dframe[(dframe['MachineID']== mid) & (dframe['MachineDescriptor']== mdesc)]
                    
            datelist = sorted(pd.unique(df['timestamp'].dt.date))
            if len(datelist) != 0:

                #For each date found in the query
                for adate in datelist:
                    temp = df[(df['timestamp'].dt.date == adate)]
                    date_val = adate.strftime("%Y-%m-%d")
                    
                    #Sum of alarm event and hours not included in self.alarm_list
                    others_evt_sum = 0
                    others_hrs_sum = 0

                    #For each alarm found in the query
                    alarmlist = pd.unique(temp['Status'])
                    for aalarm in alarmlist:
                        temp1 = temp[(temp['Status'].str.strip() == aalarm)]
                                
                        evt_sum = 0
                        hrs_sum = 0
                        feederlist = pd.unique(temp1['ID_feeder'])

                        #For each feeder found in the query
                        for afeeder in feederlist:
                            temp2 = temp1[(temp1['ID_feeder'] == afeeder)]
                            templist = np.diff(temp2['ID_aq'].to_numpy().astype(int))
                            hrs_sum = hrs_sum + (((np.count_nonzero(templist))+1) * self.t_obs)
                            evt_sum = evt_sum + (np.count_nonzero(templist > 1)+1)

                            if aalarm not in self.alarm_list:
                                others_evt_sum = others_evt_sum + evt_sum
                                others_hrs_sum = others_hrs_sum + hrs_sum
                                
                        with self.engine.connect() as connection:
                            with connection.begin():
                                if 'op_alarmevts' in self.op_tables:
                                    table = db_metadata.tables['op_alarmevts']
                                    if aalarm not in self.alarm_list:
                                        insquery = (insert(table).
                                            values({
                                                'timestamp': date_val,
                                                'date': date_val,
                                                'MachineID' : mid,
                                                'MachineDescriptor' : mdesc,
                                                'Other Alarms' : others_evt_sum
                                                })
                                            ).on_duplicate_key_update({
                                                'Other Alarms' : others_evt_sum
                                                })
                                    else:
                                        insquery = (insert(table).
                                            values({
                                                'timestamp': date_val,
                                                'date': date_val,
                                                'MachineID' : mid,
                                                'MachineDescriptor' : mdesc,
                                                f'{aalarm}' : evt_sum
                                                })
                                            ).on_duplicate_key_update({
                                                f'{aalarm}' : evt_sum
                                                })
                                    connection.execute(insquery) 

                                if 'op_alarmhrs' in self.op_tables:
                                    table = db_metadata.tables['op_alarmhrs'] 
                                    if aalarm not in self.alarm_list:
                                        insquery = (insert(table).
                                            values({
                                                'timestamp': date_val,
                                                'date': date_val,
                                                'MachineID' : mid,
                                                'MachineDescriptor' : mdesc,
                                                'Other Alarms' : others_hrs_sum
                                                })
                                            ).on_duplicate_key_update({
                                                'Other Alarms' : others_hrs_sum
                                                })
                                    else:
                                        insquery = (insert(table).
                                            values({
                                                'timestamp': date_val,
                                                'date': date_val,
                                                'MachineID' : mid,
                                                'MachineDescriptor' : mdesc,
                                                f'{aalarm}' : hrs_sum
                                                })
                                            ).on_duplicate_key_update({
                                                f'{aalarm}' : hrs_sum
                                                })
                                    connection.execute(insquery)       

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

    def create_op_tables(self):
        """ Function to check existence of output tables and create them if they don't exist 
        """
        for tablename in self.op_tables:
            with self.engine.connect() as connection:
                with connection.begin():
                    if not self.engine.dialect.has_table(connection, tablename):  # If table doesn't exist, Create.
                        metadata = MetaData(self.engine)
                        fkeyTbl = Table ('Machines', metadata, autoload_with=self.engine)
                        #TODO: Replace with 'match' case in Python 3.10
                        # Create a table with the appropriate Columns
                        if tablename == 'op_onevts':
                            Table(tablename, metadata,
                                Column('timestamp', DateTime, primary_key=True, nullable=False),
                                Column('MachineID', String(50), ForeignKey('Machines.MachineID'), primary_key=True, nullable=False),
                                Column('MachineDescriptor', String(50), nullable=False),
                                Column('date', String(20), nullable=False),
                                Column('on_evts', Integer, nullable=True),
                                Column('on_hrs', Float, nullable=True),
                                Column('efficiency', Float, nullable=True))
                        
                        elif tablename == 'op_alarmevts':
                            Table(tablename, metadata,
                                Column('timestamp', DateTime, primary_key=True, nullable=False), 
                                Column('MachineID', String(50), ForeignKey("Machines.MachineID"), primary_key=True, nullable=False),
                                Column('MachineDescriptor', String(50), nullable=False),
                                Column('date', String(20), nullable=False, unique= True),
                                Column('Clean Phot', Integer, server_default = '0'),
                                Column('ElBrk Open', Integer, server_default = '0'),
                                Column('Feeder not found', Integer, server_default = '0'),
                                Column('Motor Lock', Integer, server_default = '0'),
                                Column('OYB Err', Integer, server_default = '0'),
                                Column('PrewindErr', Integer, server_default = '0'),
                                Column('SMRangeErr', Integer, server_default = '0'),
                                Column('Switch OFF', Integer, server_default = '0'),
                                Column('TensmtrErr', Integer, server_default = '0'),
                                Column('Yarn Break', Integer, server_default = '0'),
                                Column('Other Alarms', Integer, server_default = '0'))
                            
                        elif tablename == 'op_alarmhrs':
                            Table(tablename, metadata,
                                Column('timestamp', DateTime, primary_key=True, nullable=False), 
                                Column('MachineID', String(50), ForeignKey("Machines.MachineID"), primary_key=True,  nullable=False),
                                Column('MachineDescriptor', String(50), nullable=False),
                                Column('date', String(20), nullable=False, unique= True),
                                Column('Clean Phot', Float, server_default = '0'),
                                Column('ElBrk Open', Float, server_default = '0'),
                                Column('Feeder not found', Float, server_default = '0'),
                                Column('Motor Lock', Float, server_default = '0'),
                                Column('OYB Err', Float, server_default = '0'),
                                Column('PrewindErr', Float, server_default = '0'),
                                Column('SMRangeErr', Float, server_default = '0'),
                                Column('Switch OFF', Float, server_default = '0'),
                                Column('TensmtrErr', Float, server_default = '0'),
                                Column('Yarn Break', Float, server_default = '0'),
                                Column('Other Alarms', Float, server_default = '0'))
                        
                        elif tablename == 'op_params':
                            Table(tablename, metadata,
                                Column('timestamp', DateTime, primary_key=True, nullable=False), 
                                Column('MachineID', String(50), ForeignKey("Machines.MachineID"), primary_key=True, nullable=False),
                                Column('MachineDescriptor', String(50), nullable=False),
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
                            logging.error(f'ERROR: Table "{tablename}" in list not configured!')
                            continue
                        # Implement the creation
                        metadata.create_all()
        
# if __name__ == '__main__':
#     sm = SqlManager()
#     # sm.create_op_tables()
#     # sm.gen_table_onevts()
#     sm.gen_table_alarmevts()