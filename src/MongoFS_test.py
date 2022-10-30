from MongoFS import MongoFS


# Replace the uri string with your MongoDB deployment's connection string.
conn_str = "mongodb+srv://x39j1017d:aLJCQ5mMc1kulqQf@cluster0.exky2zv.mongodb.net/?retryWrites=true&w=majority"

def main():
    # set a 5-second connection timeout
    client = MongoFS(conn_str)
    #print(client.rm("/test/john")) # True
    #print(client.mkdir("/test/john"))  # True
    #print(client.put("./data/small_test.csv", "/test/john", 5))  #True
    # print(client.cat("/test/john/small_test.csv")[0])  # last line of data
    # print(client.getPartitionLocations("/test/john/small_test.csv")) # return partition numbers, e.g. [0, 1, 2, 3, 4]
    # print(client.readPartition("/test/john/small_test.csv", 0)[0]) # first line
    print(client.ls("/test/john"))

if __name__ == "__main__":
    main()