# LGL Database Updater
This solution updates the internal database with data from analysis of Yarn break faults published via MQTT
## Features
- Multi thread subscription manager that gets data from MQTT
- A queue manages the insertion of data to the MySQL database

## Deployment
The project is intended for deployment using the updateiDB.py file
Dependencies can be installed via the requirements.txt file
All configuration parameters can be modified in the configs.yaml file

## Note on format of JSON
Please refer to TestJSON.txt for an example of the format of the message periodically received

## TODO


## Credits
**Client**: LGL
**Owner**:  Consorzio Intellimech
**Developers**: [Rohan Vangal][rosava], Luca Fasanotti

[//]: # (These are reference links used in the body of this note and get stripped out when the markdown processor does its job. There is no need to format nicely because it shouldn't be seen. Thanks SO - http://stackoverflow.com/questions/4823468/store-comments-in-markdown-syntax)
   [rosava]: <https://github.com/RohanVangal>
