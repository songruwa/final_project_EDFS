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









## Backend

Backend service based on `Flask`.



### Supported APIs:



#### PUT `/api/v1/mkdir`

- Params:

  - directory_path - the path of the new folder to be created, should be already encoded using encodeURIComponent()
- db - the database used for EDFS, should be one of the "mongo", "mysql", "firebase"
- Return:
  - res - "success" or "failed"



#### GET `/api/v1/ls`

- Params
  - directory_path - the full directory for the ls command
  - db - the database used for EDFS, should be one of the "mongo", "mysql", "firebase"

- Return:
  - children - list of string, each element representing a child file/folder



#### GET /api/v1/cat

- Params
  - file_path - the full directory for the file to be read
  - db - the database used for EDFS, should be one of the "mongo", "mysql", "firebase"

- Return:
  - content - list of string, each element is one line of data from the file



#### PUT `/api/v1/rm`

- Params
  - file_path - the full directory for the file to be removed
  - db - the database used for EDFS, should be one of the "mongo", "mysql", "firebase"
- Return:
  - res - "success" or "failed"



#### PUT `/api/v1/put`

***NOT IMPLEMENTED***



#### GET `/api/v1/getPartitionLocations`

- Params
  - file_path - the full directory for the file
  - db - the database used for EDFS, should be one of the "mongo", "mysql", "firebase"

- Return:
  - partitionLocations - list, different db returns different content



#### GET `/api/v1/readPartition`

- Params
  - file_path - the full directory for the file to be read
  - partition_num - the patition number, 1-indexed
  - db - the database used for EDFS, should be one of the "mongo", "mysql", "firebase"

- Return:
  - content - list of string, each element is one line of data from the file

### Test plan

1. Import Postman config file `./util/DSCI551.postman_collection.json`
2. Boot service locally using `python3 ./src/server.py`
3. Call APIs in Postman

