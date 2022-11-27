import sys
import logging
import yaml
import time
from paho.mqtt.client import Client
import json
from SqlManager import SqlManager

class MqttManager(SqlManager):
    """ Class to manage MQTT operations
    """
    error_codes =  {1: "incorrect protocol version",
                    2: "invalid client identifier",
                    3: "server unavailable",
                    4: "bad username or password",
                    5: "not authorised"}

    # def __init__(self,obj) -> None:
    def __init__(self) -> None:
        """ Get constants from config file
        """
        # Initialize SqlManagerClass
        super().__init__()
        configs = {}
        try:
            with open('configs.yaml') as file:
                # Conversion from YAML to Python dictionary format
                configs = yaml.load(file, Loader=yaml.FullLoader)
        except:
            logging.error('Reading configuration file failed')
            sys.exit()
        
        # Constants for MQTT communication
        self.username_mqtt = configs['mqtt']['username']
        self.password_mqtt = configs['mqtt']['password']
        self.broker_mqtt = configs['mqtt']['broker']
        self.port_mqtt = configs['mqtt']['port']
        self.reset_time_s = int(configs['mqtt']['taglist_reset_mins']) * 60
        #Get available topics from the DB
        self.topics = self.generateTopicList()
        
        self.clientlist = []

    def genClientName(self):
        """Generates new client names for multiple threads"""
        return ("LGL"+str(int(round(time.time() * 1000))))

    def genClients(self):
        self.clientlist = []
        for topic in self.topics:
            client = Client(self.genClientName())
            client.connected_flag = False
            try:
                client.username_pw_set(self.username_mqtt, self.password_mqtt)
                client.connect(self.broker_mqtt, port = self.port_mqtt)
            except:
                continue

            #Connect to callbacks
            client.on_connect = self.on_connect
            client.on_message = self.on_message

            client.loop_start()
            while not client.connected_flag:
                time.sleep(1)

            client.subscribe(topic)

            self.clientlist.append(client)

    def stopClients(self):
        """Stops all clients and waits for completion of execution
        """
        for client in self.clientlist:
            client.disconnect()
            client.loop_stop()
        time.sleep(10)

    def regenClients(self, start_time = None):
        """Override to regenerate clients and a new topic list based on input timestamp
        """
        if(not start_time or time.time()-start_time > self.reset_time_s):
            self.stopClients()
            self.topics = self.generateTopicList()
            self.genClients()
            return(time.time())
        return(start_time)

    def on_connect(self, client, userdata, flags, rc):
        """ Checks if the connection to the broker is successful
        """
        if rc == 0:
            logging.info("Successfully connected to MQTT broker")
            client.connected_flag = True
        else:
            logging.warning(f"Connection error: {self.error_codes[rc]}")
            client.loop_stop()

    def on_message(self, client, userdata, message):
        """ Prints message on the topic
        """
        # print("message received",str(message.payload.decode("utf-8")))
        try:
            str_json = str(message.payload.decode("utf-8"))
            py_json = json.loads(str_json)
            py_json["Topic"] = str(message.topic)
            self.insertQ.append(py_json)
        except:
            logging.error('ERROR: Reading JSON file failed')
        
        # # try:
        # id_acq = insert_acq(py_json,cfg.engine,cfg.t_acq)
        # insert_dat(py_json,cfg.engine,cfg.t_dat,id_acq)
        # # except:
        # #     print('ERROR: Problem inserting into MYSQL DB')