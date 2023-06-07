import yaml
import logging
import sys
from sqlalchemy import create_engine, MetaData, select, and_
from sqlalchemy.dialects.mysql.dml import Insert 
import datetime as dt

class SqlManager:
    log_dflt = {
                "MachineID":'Error',
                "FeederID": '0',
                "Timestamp": 0,
                "AlertCategory": 'yarn-break',
                "AlertForecastMins": 1440
                }
    realtime_dflt = {
                     "MachineID":'Error',
                     "FeederID": '0'
                    }

    dat_fields = {"Status":"Status",
              "T.read dgr":"T_read",
              "SM Steps":"Sm_steps",
              "Feeder":"ID_feeder",
              "T. des dgr":"T_des",
              "VBus Motor":"Vbus_motor",
              "Real Speed":"Real_speed",
              "I Ftc Out":"Ftc_out",
              "I Ftc In":"Ftc_in",
              "Meter Cons":"Meter_cons",
              "T Offset":"T_offset",
              "Torque mNm": "Torque"}

    def __init__(self) -> None:
            """ Get constants from config file
            """
            configs = {}
            try:
                with open('configs.yaml') as file:
                    # Conversion from YAML to Python dictionary format
                    configs = yaml.load(file, Loader=yaml.FullLoader)
            except:
                logging.error('Reading configuration file failed')
                sys.exit()
            
            # Constants for updating input data tables
            conn = f"mysql+pymysql://{configs['db']['username']}:{configs['db']['password']}@{configs['db']['ip']}/{configs['db']['schema']}"
            self.engine = create_engine(conn, echo = False)
            self.t_acq = configs['db']['tab_acq']
            self.t_dat = configs['db']['tab_dat']
            self.insertQ = []
            self.run_flag = True
    
    def genTopicList(self):
        """Generates list of topics to subscribe to
        """
        db_metadata = MetaData(bind = self.engine)
        db_metadata.reflect()
        with self.engine.connect() as connection:
            with connection.begin():
                table = db_metadata.tables["Machines"]            
                stmt = select(table.c.MachineID.distinct())
                retlist=[]
                try:
                    for result in connection.execute(stmt):
                        retlist.append(result[0])
                except Exception as e:
                    logging.error(f"ERROR in SELECT in genTopicList:\n{str(e)}")
                    self.run_flag = False
                    retlist.clear()
                return retlist

    def insert_dat(self, ip_json, pk):
        """Inserts json data in the dati table
        """
        db_metadata = MetaData(bind = self.engine)
        db_metadata.reflect()

        feeder_list = ip_json["Feeders"]

        for row in feeder_list:
            #Copy default values
            dic_values = {}
            # dic_values = dat_dflt
            dic_values["ID_aq"] = pk
            #Fill values from JSON
            for key in row.keys():
                inkey = key
                key = key.rstrip().lstrip()
                # print(f"_{key}_")
                if key == "Status":
                    dic_values[self.dat_fields[key]] = str(row[inkey]) if row[inkey] != '' else None
                else:
                    dic_values[self.dat_fields[key]] = int(row[inkey]) if row[inkey] not in ['', 'Parameter not found'] else None
            
            with self.engine.connect() as connection:
                with connection.begin():
                    table = db_metadata.tables[self.t_dat]            
                    stmt_ins = (Insert(table).
                        values(dic_values))
                    stmt_dup = stmt_ins.on_duplicate_key_update(dic_values)
                    try:
                        connection.execute(stmt_dup)
                    except Exception as e:
                        logging.error(f"ERROR inserting data to 'Dati':\n{str(e)}")

    def insertToTables(self, ip_json):
        """
        Inserts json data into the tables LogPrediction Data and RtpredictionData
        @param ip_json: Input JSON message to extract data from
        """
        db_metadata = MetaData(bind = self.engine)
        db_metadata.reflect()

        #Copy default values
        log_dic_values = self.log_dflt
        rt_dic_values = self.realtime_dflt

        #Get primary key values from JSON
        timestamp = dt.datetime.fromtimestamp(ip_json["timestamp"]/1000.0)    #Convert from epoch time in ms to datetime
        log_dic_values["Timestamp"] = timestamp
        rt_dic_values["Timestamp"] = timestamp

        for elem in ip_json["metadatas"]:
            # Add MachineID to value dictionary
            if elem["name"] == "machine-id":
                log_dic_values["MachineID"] = elem["text"]
                rt_dic_values["MachineID"] = elem["text"]
            # Add FeederID to value dictionary
            if elem["name"] == "feeder-id":
                log_dic_values["FeederID"] = int(elem["text"])
                rt_dic_values["FeederID"] = int(elem["text"])
        
        #Insert data into RtPredictionData
        #TODO: Establish alarm codes
        rt_dic_values["MachineStatus"] = 'Alarm'
        with self.engine.connect() as connection:
            with connection.begin():
                table = db_metadata.tables["RtPredictionData"]            
                stmt_ins = (Insert(table).
                    values(rt_dic_values))
                stmt_dup = stmt_ins.on_duplicate_key_update(rt_dic_values)
                try:
                    connection.execute(stmt_dup)
                except Exception as e:
                    logging.error(f"ERROR inserting data to 'LogPredictionData':\n{str(e)}")

        #Get diagnostics
        for elem in ip_json["diagnostics"]:
            # Add MachineID to value dictionary
            if elem["name"] == "alert-category":
                log_dic_values["AlertCategory"] = elem["value"]
            # Add MachineID to value dictionary
            if elem["name"] == "alert-forecast-minutes":
                log_dic_values["AlertForecastMins"] = int(elem["value"])

        #Insert data into LogPredictionData
        with self.engine.connect() as connection:
            with connection.begin():
                table = db_metadata.tables["LogPredictionData"]            
                stmt_ins = (Insert(table).
                    values(log_dic_values))
                stmt_dup = stmt_ins.on_duplicate_key_update(log_dic_values)
                try:
                    connection.execute(stmt_dup)
                except Exception as e:
                    logging.error(f"ERROR inserting data to 'LogPredictionData':\n{str(e)}")
    
    def insertData(self):
        while len(self.insertQ) > 0:
            inJSON = self.insertQ.pop()
            self.insertToTables(inJSON)

import json
if __name__ == "__main__":
    sm = SqlManager()
    str_json = """{
    "gatewayId":"gtw-lgl-alarm",
    "deviceId":"Macchina08-53",
    "timestamp":1682698026401,
    "diagnostics":[
        {
            "name":"alert-forecast-minutes",
            "value":"1440"
        },
        {
            "name":"alert-category",
            "value":"yarn-break"
        }
    ],
    "metadatas":[
        {
            "name":"machine-id",
            "text":"Macchina08"
        },
        {
            "name":"feeder-id",
            "text":"53"
        }
    ]
    }"""
    py_json = json.loads(str_json)
    sm.insertToTables(py_json)