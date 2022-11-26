from abc import ABC, abstractmethod

class EDFSInterface(ABC):
    # create a new folder {directory_path}, create parent folders along the way if not existing
    @abstractmethod
    def mkdir(self, directory_path):
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
    
    # upload a file from local file system's path {file_src}, to remote EDFS's directory {file_path}, with {partition_count} partitions
    @abstractmethod
    def put(self, file_src, directory_path, partition_count):
        pass
    
    # return all locations of file {file_path}'s partitions
    @abstractmethod
    def getPartitionLocations(self, file_path):
        pass
    
    # return the content of {partition_num}-th partition of the file {file_path}, 1-indexed
    @abstractmethod
    def readPartition(self, file_path, partition_num):
        pass
    
    # optional? return the entire nested directory structure from root directory
    @abstractmethod
    def tree(self):
        pass