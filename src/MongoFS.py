import pymongo
import json

import math
from EDFS import EDFSInterface


'''
    An EDFS implementation using Mongo DB. Directory structures are stored in a database called "namenode",
    data partitions are stored inside a database called "datanode"

    namenode structure: documents within single collection Database(namenode).Collection(directory).find()
    {
        "_id": {objectId implicitly created by mongoDB during insertion},
        "fullPath": "{full path of the file/directory}", // full path such as "/etc/test/text.txt", note that for root directory, its full path will be *EMPTY* string ""
        "isDirectory": true/false,
        "partitionCount": {1 ~ N} //number of data partitions
    }

    datanode structure: documents within single collection Database(datanode).Collection(data).find()
    {
        "_id": {objectId implicitly created by mongoDB during insertion},
        "fullPath": "{full path of the file this partition belongs to}",
        "partitionNum": {0 ~ N-1},
        "subPartitionCount": M, // number of BSON collections this partition is actually stored within
        "subPartitionNum": {0 ~ N-1}, // # of sub partition
        "dataBlob": "{JSON serialized data}"
    }
'''

# TODO: Make it transactional when doing multiple operations

class MongoFS(EDFSInterface):
    MAXIMUM_ROW = 5000 # maximum number of rows a BSON sub partition can store

    def __init__(self, connect_str):
        self.conn_str = connect_str
        self.client = pymongo.MongoClient(connect_str, serverSelectionTimeoutMS=5000)
        self.client.server_info() # eagerly raise exception if connection failed, bad practice?

        # if this database does not have the root directory, create one
        rootDir = self.client.namenode.directory.find({"fullPath": ""})
        if len(list(rootDir.clone())) == 0:
            self.client.namenode.directory.insert_one({
                "fullPath": "",
                "isDirectory": True
            })
    
    def __del__(self):
        self.client.close()

    ####################### EDFS interface method implementations ###############################

    # create a new folder {directory_path}, create parent folders along the way if not existing
    # Return True if succesfull, False if failed
    def mkdir(self, directory_path):
        directory_path = "" if directory_path == "/" else directory_path
        folders = directory_path.split("/")[1:]

        directory_list = []
        prefix_path = ""
        for folder in folders:
            prefix_path += "/" + folder
            directory_list.append(prefix_path)
        
        folder_coll = self.client.namenode.directory.find({"fullPath": {"$in": directory_list}})
        for folder in folder_coll:
            if not folder["isDirectory"]:
                # a file with the same path as one of the parent folder exist, cannot create folder
                return False

        # create the folder in namenode
        documents = [{"fullPath": path, "isDirectory": True} for path in directory_list]
        # first delete existing folders to prevent duplication
        res = self.client.namenode.directory.delete_many({
            "fullPath": {"$in": directory_list},
            "isDirectory": {"$eq": True}
        })
        if not res.acknowledged:
            return False
        res = self.client.namenode.directory.insert_many(documents)
        return res.acknowledged
    
    # listing all files within directory {directory_path}
    # Return list of string containing names of files within the directory
    # FIXME: also return if the file is a directory or file?
    def ls(self, directory_path):
        directory_path = "" if directory_path == "/" else directory_path
        
        children = []
        children_coll = self.client.namenode.directory.find({"fullPath": {"$regex": "^" + directory_path + "/[^/]+$"}})
        for child in children_coll:
            full_path = child["fullPath"]
            children.append(full_path[full_path.rfind("/") + 1 : ])
        return children
    
    # display content of file {file_path}
    # Return list of list of string, representing a table's content
    def cat(self, file_path):
        file_coll = self.client.namenode.directory.find({"fullPath": file_path})
        if len(list(file_coll.clone())) != 1:
            # file does not exist
            return []
        
        data = []
        partition_count = file_coll[0]["partitionCount"]
        for i in range(partition_count):
            partition_data, ok = self.readPartitionWithError(file_path, i)
            if not ok:
                return []
            data += partition_data

        return data


    # remove file {file_path}
    # Return True if succesfull, False if failed
    def rm(self, file_path):
        file_path = "" if file_path == "/" else file_path
        
        # delete entries in namenode
        name_res = self.client.namenode.directory.delete_many({
            "$or":[
                {"fullPath": file_path}, # file itself
                {"fullPath": {"$regex": "^" + file_path + "/.*$"}} # all of its children
            ]
        })
        if not name_res.acknowledged:
            return False

        # then delete actual data partitions
        data_res = self.client.datanode.data.delete_many({
            "$or":[
                {"fullPath": file_path}, # file itself
                {"fullPath": {"$regex": "^" + file_path + "/.*$"}} # all of its children
            ]
        })

        return data_res.acknowledged


    # upload a file from local file system's path {file_src}, to remote EDFS's directory {file_path}, with {partition_count} partitions
    # Return True if succesfull, False if failed
    def put(self, file_src, directory_path, partition_count):
        file_name = file_src[file_src.rfind("/") + 1 :]
        directory_path = "" if directory_path == "/" else directory_path
        file_path = directory_path + "/" + file_name

        # check if path is valid
        parent_coll = self.client.namenode.directory.find({"fullPath": directory_path})
        if len(list(parent_coll.clone())) != 1 or not parent_coll[0]["isDirectory"]:
            # parent directory does not exist or is not a directory
            return False
        
        itself_coll = self.client.namenode.directory.find({"fullPath": file_path})
        if len(list(itself_coll.clone())) != 0:
            # a node with same name already exist
            return False

        # clear potential dirty data in datanode before writing data partitions
        self.client.datanode.data.delete_many({"fullPath": file_path})

        # write data to datanode
        file = open(file_src, "r")
        row_count = sum(1 for row in file)

        file = open(file_src, "r")
        avg = math.ceil(row_count / partition_count)

        for partition_num in range(partition_count):
            # for each partition, try to read average number of rows unless there are no enough rows remaining
            should_read = min(avg, row_count)
            row_count -= should_read
            res = self.putPartition(file, should_read, file_path, partition_num)
            if not res:
                # failed to write data, return err without retrying
                return False

        # create entry in namenode
        res = self.client.namenode.directory.insert_one({
            "fullPath": file_path,
            "isDirectory": False,
            "partitionCount": partition_count
        })

        return res.acknowledged


    # return all locations of file {file_path}'s partitions
    # retrun a list containing partition numbers
    # cannot return a single size because a logical partition is further devided into collections to avoid BSON size limit
    def getPartitionLocations(self, file_path):
        file_coll = self.client.namenode.directory.find({"fullPath": file_path})
        if len(list(file_coll.clone())) != 1 or file_coll[0]["isDirectory"]:
            # file does not exist or is a folder
            return []

        return [*range(file_coll[0]["partitionCount"])]


    # return the content of {partition_num}-th partition of the file {file_path}, 0-indexed
    # Return list of rows(row == list of string)
    def readPartition(self, file_path, partition_num):
        res, _ = self.readPartitionWithError(file_path, partition_num)
        return res
    
    # optional? return the entire nested directory structure from root directory
    # TODO: implement this if needed
    def tree(self):
        directory = self.client.namenode.directory.find({"name": ""})
        return


    ###################### Helper functions ####################################

    def putPartition(self, file, should_read, file_path, partition_num):
        data_list = [] # list of data blobs
        for n in range(should_read):
            if n % self.MAXIMUM_ROW == 0:
                data_list.append([])
            data_list[-1].append(file.readline().replace('\n', '').replace('\r', ''))
        sub_partition_count = len(data_list)
        for i in range(sub_partition_count):
            data_blob = json.dumps(data_list[i]) # serialize data into JSON
            res = self.client.datanode.data.insert_one({
                "fullPath": file_path,
                "partitionNum": partition_num,
                "dataBlob": data_blob,
                "subPartitionCount": sub_partition_count,
                "subPartitionNum": i
            })
            if not res.acknowledged:
                # failed to write data, return err without retrying
                return False

        return True

    def readPartitionWithError(self, file_path, partition_num):
        file_coll = self.client.namenode.directory.find({"fullPath": file_path})
        if len(list(file_coll.clone())) != 1:
            # file does not exist
            return [], False
        
        # try to find first sub_partition
        data_coll = self.client.datanode.data.find({
            "fullPath": file_path,
            "partitionNum": partition_num,
            "subPartitionNum": 0
        })
        if len(list(data_coll.clone())) == 0:
            # partition does not exist
            return [], False
        
        # then start reading remaining sub partitions
        sub_partition_count = data_coll[0]["subPartitionCount"]
        data = json.loads(data_coll[0]["dataBlob"])
        for i in range(1, sub_partition_count):
            data_coll = self.client.datanode.data.find({
                "fullPath": file_path,
                "partitionNum": partition_num,
                "subPartitionNum": i
            })
            if len(list(data_coll.clone())) == 0:
                # partition does not exist
                return [], False
            data += json.loads(data_coll[0]["dataBlob"])

        return data, True