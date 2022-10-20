# DSCI 511 Project

## Intro

//TODO:











## EDFS

Common interface for all EDFS implementations: `src/EDFS.py`

### Firebase





### MySQL



### MongoDB

Implementations:

- `src/MongoFS.py`
- `src/MongoFS_test.py`



#### Test plan

Change `conn_str` within `MongoFS_test.py` to connect to different cluster. Before connect to the MongoDB cluster, whitelist the IP address of calling machine.

Under project directory, invoke test function by calling `python3 src/MongoFS_test.py`