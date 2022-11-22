"""
@date:          23/11/2022
@author:        Rohan Vangal
@brief:         Back-end function to update database with data from multiple sites and machines via MQTT broker 
@description:   
@notes:         
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import yaml

engine = create_engine("mysql://user:pass@host/dbname", future=True)
Session = sessionmaker(bind=engine, future=True)

# from paho.mqtt.client import Client
# import time

# import json
# from sqlalchemy import create_engine, MetaData, func, select, and_,\
#      Table, Column, String, DateTime, Integer, Float, DefaultClause
# from sqlalchemy.dialects.mysql import insert
# import datetime as dt
# from sqlalchemy.sql.elements import Null

# error_codes =  {1: "incorrect protocol version",
#                 2: "invalid client identifier",
#                 3: "server unavailable",
#                 4: "bad username or password",
#                 5: "not authorised"}

# acq_dflt = { "filename":'Error',
#              "timestamp":'01-01-2000',
#              "conn_status": 'test',
#              "machine_status": 0,
#              "inv_speed": 0,
#              "machine_power": 1
#              }

# dat_fields = {"Status":"Status",
#               "T.read dgr":"T_read",
#               "SM Steps":"Sm_steps",
#               "Feeder":"ID_feeder",
#               "T. des dgr":"T_des",
#               "VBus Motor":"Vbus_motor",
#               "Real Speed":"Real_speed",
#               "I Ftc Out":"Ftc_out",
#               "I Ftc In":"Ftc_in",
#               "Meter Cons":"Meter_cons",
#               "T Offset":"T_offset",
#               "Torque mNm": "Torque"}

class SqlManager:
    def __init__(self) -> None:
            """ Get constants from config file
            """
            with open('configs.yaml') as file:
                # Conversion from YAML to Python dictionary format
                configs = yaml.load(file, Loader=yaml.FullLoader)
            
            # Constants for updating input data tables
            conn = f"mysql+pymysql://{configs['db']['username']}:{configs['db']['password']}@{configs['db']['ip']}/{configs['db']['schema']}"
            self.engine = create_engine(conn, echo = False)
            self.t_acq = configs['db']['tab_acq']
            self.t_dat = configs['db']['tab_dat']
    
    def generateTopicList(self):
        """Generates list of topics to subscribe to
        """
        session = sessionmaker(bind=self.engine, future=True)
        stmt = select()


class MqttManager(SqlManager):
    """ Class to manage MQTT operations
    """
    def __init__(self) -> None:
        """ Get constants from config file
        """
        with open('configs.yaml') as file:
            # Conversion from YAML to Python dictionary format
            configs = yaml.load(file, Loader=yaml.FullLoader)
        
        # Constants for MQTT communication
        self.client = configs['mqtt']['client']
        self.username = configs['mqtt']['username']
        self.password = configs['mqtt']['password']
        self.broker = configs['mqtt']['broker']
        self.port = configs['mqtt']['port']
        # self.topic = configs['mqtt']['topic']

# # Function to check if the connection to the broker is successful               
# def on_connect(client, userdata, flags, rc):
#     if rc == 0:
#         print("Successfully Connected")
#         client.connected_flag = True
#     else:
#         print(f"Error: {error_codes[rc]}")

# # Function to print message on the topic
# def on_message(client, userdata, message):
#     try:
#         str_json = str(message.payload.decode("utf-8"))
#         py_json = json.loads(str_json)
#     except:
#         print('ERROR: Reading JSON file failed')
#     # try:
#     id_acq = insert_acq(py_json,cfg.engine,cfg.t_acq)
#     insert_dat(py_json,cfg.engine,cfg.t_dat,id_acq)
#     # except:
#     #     print('ERROR: Problem inserting into MYSQL DB')

# # Function insert json data in the Acquisizioni table
# def insert_acq(ip_json, engine, tablename):
#     db_metadata = MetaData(bind = engine)
#     db_metadata.reflect()

#     temp = ip_json["GeneralInfo"]
#     #Copy default values
#     dic_values = acq_dflt
#     #Fill values from JSON
#     # for key in temp.keys():
#     #     if acq_fields[key] in dic_values.keys():
#     #         if key == "Customer" and "Time" in temp.keys():
#     #             dic_values[acq_fields[key]] = temp[key]+temp["Time"]
#     #         else:
#     #             dic_values[acq_fields[key]] = temp[key]
#     dic_values["filename"] = str(temp["Customer"]+temp["Time"])
#     dic_values["timestamp"] = dt.datetime.strptime(temp["Time"], '%d_%m_%Y-%H_%M_%S')    #.strftime('%Y-%m-%d')
#     dic_values["conn_status"] = str(temp["Connection_Status"]) if temp["Connection_Status"] != '' else dic_values["conn_status"]
#     dic_values["machine_status"] = int(temp["MachineStatus"]) if temp["MachineStatus"] != '' else dic_values["machine_status"]
#     dic_values["inv_speed"] = int(temp["InverterSpeed"]) if temp["InverterSpeed"] != '' else dic_values["inv_speed"]

#     # print(dic_values)
#     with engine.connect() as connection:
#         with connection.begin():
#             table = db_metadata.tables[tablename] 
#             #BUG with ON_DUPLICATE_UPDATE that autoincrements even when duplicate is found  
#             #SOLUTION check an entry for the same filename as you are about to enter
#             #         if it exists update the table, else it inserts         
#             stmt_sel =  table.select().\
#                         where(table.c.filename == dic_values["filename"])
#             res_sel = connection.execute(stmt_sel)

#             if res_sel.rowcount > 0:
#                 stmt = table.update().where(table.c.filename == dic_values["filename"]).values(dic_values)
#                 result = connection.execute(stmt)
#                 id = res_sel.all()[0][0]
#             else:
#                 stmt = (insert(table).values(dic_values))
#                 result = connection.execute(stmt)
#                 #Get autoincremented
#                 id = result.inserted_primary_key[0]
#         return(id)

# # Function insert json data in the dati table
# def insert_dat(ip_json, engine, tablename, pk):
#     db_metadata = MetaData(bind = engine)
#     db_metadata.reflect()

#     feeder_list = ip_json["Feeders"]

#     for row in feeder_list:
#         #Copy default values
#         dic_values = {}
#         # dic_values = dat_dflt
#         dic_values["ID_aq"] = pk
#         #Fill values from JSON
#         for key in row.keys():
#             inkey = key
#             key = key.rstrip().lstrip()
#             # print(f"_{key}_")
#             if key == "Status":
#                 dic_values[dat_fields[key]] = str(row[inkey]) if row[inkey] != '' else None
#             else:
#                 dic_values[dat_fields[key]] = int(row[inkey]) if row[inkey] not in ['', 'Parameter not found'] else None
        
#         with engine.connect() as connection:
#             with connection.begin():
#                 table = db_metadata.tables[tablename]            
#                 stmt_ins = (insert(table).
#                     values(dic_values))
#                 stmt_dup = stmt_ins.on_duplicate_key_update(dic_values)
#                 connection.execute(stmt_dup)

# #TODO: Create task to update every hour
# def getTopics():
#     ''' Get list of topics to subscribe to. Topics are in the format <customerName>_<MCU UID>
#     '''

# if __name__ == '__main__':
#     while True:
#         try:
#             #Getting defaults ESSENTIAL
#             cfg = configs()
#         except:
#             print('ERROR: Reading YAML file failed')
#             quit()

#         # try:
#         #Creating MQTT client
#         client = Client(client_id= cfg.client)
#         client.connected_flag = False

#         #Bind functions to client 
#         client.on_connect = on_connect
#         client.on_message = on_message

#         client.username_pw_set(cfg.username, cfg.password)
#         client.connect(cfg.broker, port= cfg.port)
#         client.loop_start()
#         while not client.connected_flag: #wait in loop
#             time.sleep(1)
#         client.subscribe(cfg.topic)
#         # except:
#         #     print('ERROR: Problem connecting to MQTT broker')
#         #     client.loop_stop()    # Stop loop 
#         #     client.disconnect()   # disconnect
#         #     quit()
#         client.loop_forever()
#         time.sleep(10)