from abc import ABC, abstractmethod
from debugpy import connect
import pymongo
import json
import csv
import math

# Replace the uri string with your MongoDB deployment's connection string.
conn_str = "mongodb+srv://x39j1017d:aLJCQ5mMc1kulqQf@cluster0.exky2zv.mongodb.net/?retryWrites=true&w=majority"




class EDFSInterface(ABC):
    # create a new directory named as {directory_name} under {directory_path}
    @abstractmethod
    def mkdir(self, directory_path, directory_name):
        pass
    
    # listing content of directory {directory_path}
    @abstractmethod
    def ls(self, directory_path):
        pass
    
    # display content of file {file_path}
    @abstractmethod
    def cat(self, file_path):
        pass
    
    # remove file {file_path}
    @abstractmethod
    def rm(self, file_path):
        pass
    
    # upload a file from local file system's path {file_src}, to remote EDFS's path {file_path}, with {partition_count} partitions
    @abstractmethod
    def put(self, file_src, file_path, partition_count):
        pass
    
    # return all locations of file {file_path}'s partitions
    @abstractmethod
    def getPartitionLocations(self, file_path):
        pass
    
    # return the content of {partition_num}-th partition of the file {file_path}, 0-indexed
    @abstractmethod
    def readPartition(self, file_path, partition_num):
        pass
    
    # optional? return the entire nested directory structure from root directory
    @abstractmethod
    def tree(self):
        pass


'''
    namenode structure: documents within single collection Database(namenode).Collection(directory).find()
    {
        "_id": {objectId implicitly created by mongoDB during insertion},
        fullPath: "{full path of the file/directory}", // full path such as "/etc/test/text.txt", note that for root directory, its full path will be *EMPTY* string ""
        isDirectory: true/false,
        partitions: [{objectID of partition #0}, {objectID of partition #1} ...], //for file only
    }

    datanode structure: documents within single collection Database(datanode).Collection(data).find()
    {
        "_id": {objectId implicitly created by mongoDB during insertion},
        fullPath: "{full path of the file this partition belongs to}",
        partitionNum: {0 ~ N-1},
        dataBlob: "{JSON serialized data}"
    }
'''

# Make it transactional when doing multiple operations

class MongoFS(EDFSInterface):
    def __init__(self, connect_str):
        self.conn_str = connect_str
        self.client = pymongo.MongoClient(connect_str, serverSelectionTimeoutMS=5000)
        self.client.server_info() # will raise exception if connection failed, bad practice?

        # if this database does not have root directory, create one
        rootDir = self.client.namenode.directory.find({"fullPath": ""})
        if len(list(rootDir.clone())) == 0:
            self.client.namenode.directory.insert_one({
            "fullPath": "",
            "isDirectory": True
        })
    
    def __del__(self):
        self.client.close()

    # EDFS implementations
    # create a new directory named as {directory_name} under {directory_path}
    # Return True if succesfull, False if failed
    def mkdir(self, directory_path, directory_name):
        directory_path = "" if directory_path == "/" else directory_path

        parent_coll = self.client.namenode.directory.find({"fullPath": directory_path})
        if len(list(parent_coll.clone())) != 1 or not parent_coll[0]["isDirectory"]:
            # parent directory does not exist or is not a directory
            return False
        
        itself_coll = self.client.namenode.directory.find({"fullPath": directory_path + "/" + directory_name})
        if len(list(itself_coll.clone())) != 0:
            # a node with same name already exist
            return False

        res = self.client.namenode.directory.insert_one({
            "fullPath": directory_path + "/" + directory_name,
            "isDirectory": True
        })
        return res.acknowledged
    
    # listing content of directory {directory_path}
    # Return list of string containing names of files within the directory
    def ls(self, directory_path):
        directory_path = "" if directory_path == "/" else directory_path
        
        children = []
        children_coll = self.client.namenode.directory.find({"fullPath": {"$regex": "^" + directory_path + "/[^/]+$"}})
        for child in children_coll:
            full_path = child["fullPath"]
            children.append(full_path[full_path.rfind("/") + 1 : ])
        return children
    
    # display content of file {file_path}
    def cat(self, file_path):
        file_coll = self.client.namenode.directory.find({"fullPath": file_path})
        if len(list(file_coll.clone())) != 1:
            # file does not exist
            return ""
        
        data = []
        print(file_coll[0]["partitions"])
        partition_num = len(file_coll[0]["partitions"])
        for i in range(partition_num):
            partition_data = self.readPartition(file_path, i)
            data += partition_data

        return data
    
    # remove file {file_path}
    # Return array, [0]: number of deleted files, [1]: number of deleted data partitions
    def rm(self, file_path):
        name_res = self.client.namenode.directory.delete_many({
            "$or":[
                {"fullPath": file_path}, # file itself
                {"fullPath": {"$regex": "^" + file_path + "/.*$"}} # all of its children
            ]
        })

        data_res = self.client.datanode.data.delete_many({
            "$or":[
                {"fullPath": file_path}, # file itself
                {"fullPath": {"$regex": "^" + file_path + "/.*$"}} # all of its children
            ]
        })

        return [name_res.deleted_count, data_res.deleted_count]


    # upload a file from local file system's path {file_src}, to remote EDFS's path {file_path}, with {partition_count} partitions
    # FIXME: currently this function supports csv files only
    def put(self, file_src, file_path, partition_count):
        parent_directory = file_path[ : file_path.rfind("/")]

        # check if path is valid
        parent_coll = self.client.namenode.directory.find({"fullPath": parent_directory})
        if len(list(parent_coll.clone())) != 1 or not parent_coll[0]["isDirectory"]:
            # parent directory does not exist or is not a directory
            return False
        
        itself_coll = self.client.namenode.directory.find({"fullPath": file_path})
        if len(list(itself_coll.clone())) != 0:
            # a node with same name already exist
            # FIXME: overwrite that file or return error?
            return False

        # clear potential dirty data in datanode due to previous failed writes
        self.client.datanode.data.delete_many({"fullPath": file_path})

        # write data to datanode
        partitions = [] # stores objectID of partitions
        file = open(file_src, "r")
        reader = csv.reader(file)
        row_count = sum(1 for row in reader)

        file = open(file_src, "r")
        reader = csv.reader(file)
        avg = math.ceil(row_count / partition_count)

        for i in range(partition_count):
            data = [] # list of rows, each row should be a list of strings representing each entry
            # for each partition, try to read average number of rows unless there are no enough rows remaining
            should_read = min(avg, row_count)
            row_count -= should_read

            for n in range(should_read):
                data.append(next(reader))

            data_blob = json.dumps(data) # serialize data into JSON

            res = self.client.datanode.data.insert_one({
                "fullPath": file_path,
                "partitionNum": i,
                "dataBlob": data_blob
            })
            if not res.acknowledged:
                # failed to write data, return err without retrying
                return False

            partitions.append(str(res.inserted_id))

        # create entry in namenode
        self.client.namenode.directory.insert_one({
            "fullPath": file_path,
            "isDirectory": False,
            "partitions": partitions
        })

        return True


    # return all locations of file {file_path}'s partitions
    def getPartitionLocations(self, file_path):
        file_coll = self.client.namenode.directory.find({"fullPath": file_path})
        if len(list(file_coll.clone())) != 1:
            # file does not exist
            return False

        return file_coll[0]["partitions"]
    
    # return the content of {partition_num}-th partition of the file {file_path}, 0-indexed
    # Return list of rows(row == list of string)
    def readPartition(self, file_path, partition_num):
        file_coll = self.client.namenode.directory.find({"fullPath": file_path})
        if len(list(file_coll.clone())) != 1:
            # file does not exist
            return False
        
        data_coll = self.client.datanode.data.find({
            "fullPath": file_path,
            "partitionNum": partition_num
        })
        if len(list(data_coll.clone())) != 1:
            # partition does not exist
            return False
        
        data = json.loads(data_coll[0]["dataBlob"])
        return data
    
    # optional? return the entire nested directory structure from root directory
    # TODO: implement this if needed
    def tree(self):
        directory = self.client.namenode.directory.find({"name": ""})
        return


def main():
    # set a 5-second connection timeout
    client = MongoFS(conn_str)
    client.mkdir("/", "test")
    client.put("./data/data.csv", "/test/data.csv", 5)

    data = client.cat("/test/data.csv")
    print(len(data))
    print(data[0])
    print(data[-1])

    for i in range(len(data)):
        if len(data[i]) != len(data[1]):
            print(str(i) + " is NOT VALID!")
    
    # client.put("./data/Crime_Data_from_2010_to_2019.csv", "/test/data1", 5)


if __name__ == "__main__":
    main()