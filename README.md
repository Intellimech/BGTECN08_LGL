# OUTPUT TABLE GENERATOR FOR GRAFANA

The updateoDB.py file generates a set of tables containing processed data at the machine level for each day:

|   | Table Name   | Description                                                    |   |   |
|---|--------------|----------------------------------------------------------------|---|---|
| 1 | op_alarmevts | no. of alarm events encountered each day by type of alarm      |   |   |
| 2 | op_alarmhrs  | no. of hours spent in an alarm state each day by type of alarm |   |   |
| 3 | op_onevts    | no. of machine ON/OFF events and hours each day                |   |   |