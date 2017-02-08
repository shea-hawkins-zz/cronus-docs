## Cronus

# Connect To The Lab
In order to develop on the lab, a number of ports need to be forwarded to the local machine. 
The ports are as follows:

# In order of appearance: yarn resource manager, hdfs, hadoop application manager, hdfs manager, spark history server
1) ssh -L 8032:localhost:8032 -L 9000:localhost:9000 -L 8088:localhost:8088 -L 50070:localhost:50070 -L 18080:localhost:18080 lab@spiralsoft.io -p 2201

2) submitting applications: 
