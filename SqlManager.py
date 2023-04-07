import yaml
import logging
import sys
from sqlalchemy import create_engine, MetaData, select, and_
from sqlalchemy.dialects.mysql.dml import Insert 
import datetime as dt

class SqlManager:
    acq_dflt = {
                "filename":'Error',
                "timestamp":'01-01-2000',
                "conn_status": 'test',
                "machine_status": 0,
                "inv_speed": 0,
                "machine_power": 1,
                "RoundCount": 0
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

    def insert_acq(self, ip_json):
        """
        Inserts json data in the Acquisizioni table
        
        :param ip_json: Input JSON message
        :param engine: SQL engine
        :return: Returns id_acq of the inserted line for use in updating data table.
                 Returns none if json does not contain the necessary fields
        """
        db_metadata = MetaData(bind = self.engine)
        db_metadata.reflect()

        temp = ip_json["GeneralInfo"]
        topic = ip_json["Topic"]
        #Copy default values
        dic_values = self.acq_dflt
        #Fill values from JSON
        # for key in temp.keys():
        #     if acq_fields[key] in dic_values.keys():
        #         if key == "Customer" and "Time" in temp.keys():
        #             dic_values[acq_fields[key]] = temp[key]+temp["Time"]
        #         else:
        #             dic_values[acq_fields[key]] = temp[key]
        
        # Check primary keys in JSON message
        acq_primary_keys = ["Customer","Time","MachineDescriptor"]
        ifresult = True
        if not all(pkey in temp.keys() for pkey in acq_primary_keys):
            logging.info("Message recieved did not contain the necessary fields")
            return (None)

        # Add data to a dictionary to be inserted
        dic_values["filename"] = str(temp["Customer"]+temp["Time"])
        dic_values["timestamp"] = dt.datetime.strptime(temp["Time"], '%d_%m_%Y-%H_%M_%S')    #.strftime('%Y-%m-%d')
        dic_values["conn_status"] = str(temp["Connection_Status"]) if temp["Connection_Status"] != '' else dic_values["conn_status"]
        dic_values["machine_status"] = int(temp["MachineStatus"]) if temp["MachineStatus"] != '' else dic_values["machine_status"]
        dic_values["inv_speed"] = int(temp["InverterSpeed"]) if temp["InverterSpeed"] != '' else dic_values["inv_speed"]
        dic_values["RoundCount"] = int(temp["RoundCnt"]) if temp["RoundCnt"] != '' else dic_values["RoundCount"]
        dic_values["MachineDescriptor"] = str(temp["MachineDescriptor"])
        dic_values["MachineID"] = topic

        # print(dic_values)
        with self.engine.connect() as connection:
            with connection.begin():
                table = db_metadata.tables[self.t_acq] 
                #BUG with ON_DUPLICATE_UPDATE that autoincrements even when duplicate is found  
                #SOLUTION check an entry for the same filename as you are about to enter
                #         if it exists update the table, else it inserts         
                stmt_sel =  table.select().\
                            where(and_(
                                table.c.filename == dic_values["filename"],
                                table.c.MachineDescriptor == dic_values["MachineDescriptor"]))
                try:
                    res_sel = connection.execute(stmt_sel)
                except Exception as e:
                    logging.error(f"SQL ERROR in insert_acq():\n{str(e)}")
                    self.run_flag = False
                    return(None)

                if res_sel.rowcount > 0:
                    stmt = table.update().\
                            where(and_(
                                table.c.filename == dic_values["filename"],
                                table.c.MachineDescriptor == dic_values["MachineDescriptor"]
                                )).values(dic_values)
                    try:
                        result = connection.execute(stmt)
                    except Exception as e:
                        logging.error(f"SQL ERROR in insert_acq():\n{str(e)}")
                        self.run_flag = False
                        return(None)
                    id = res_sel.all()[0][0]
                else:
                    stmt = (Insert(table).values(dic_values))
                    try:
                        result = connection.execute(stmt)
                    except Exception as e:
                        logging.error(f"SQL ERROR in insert_acq():\n{str(e)}")
                        self.run_flag = False
                        return(None)
                    #Get autoincremented
                    id = result.inserted_primary_key[0]
            return(id)
        return(None)
    
    def insertData(self):
        while len(self.insertQ) > 0:
            inJSON = self.insertQ.pop()
            id_acq = self.insert_acq(inJSON)
            if(id_acq):
                self.insert_dat(inJSON,id_acq)