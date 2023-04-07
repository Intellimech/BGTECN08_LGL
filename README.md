# LGL Database Updater
This solution updates the internal database with data from multiple sites published via MQTT
## Features
- Multi thread subscription manager that gets data from MQTT
- A queue manages the insertion of data to the MySQL database
- Periodically, the topic list to subscribe to is updated from the database

## Deployment
The project is intended for deployment using the updateiDB.py file
Dependencies can be installed via the requirements.txt file
All configuration parameters can be modified in the configs.yaml file

## TODO
- Check concurrency problems with queue

## Credits
**Client**: LGL
**Owner**:  Consorzio Intellimech
**Developers**: [Rohan Vangal][rosava], Luca Fasanotti

[//]: # (These are reference links used in the body of this note and get stripped out when the markdown processor does its job. There is no need to format nicely because it shouldn't be seen. Thanks SO - http://stackoverflow.com/questions/4823468/store-comments-in-markdown-syntax)
   [rosava]: <https://github.com/RohanVangal>
