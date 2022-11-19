from MongoFS import MongoFS
import ordertools as SqlFS
import firebase_commands as FbFS

# configs
mongo_conn_str = "mongodb+srv://x39j1017d:aLJCQ5mMc1kulqQf@cluster0.exky2zv.mongodb.net/?retryWrites=true&w=majority"

database=""
mongoClient = MongoFS(mongo_conn_str)
def getPartition(file,db):
    # get file's number of partitions from database db
    # return the number
    try:
        res=[]
        if db == 'mongo':
            res = mongoClient.getPartitionLocations(file)
        elif db == 'mysql':
            res = SqlFS.getPartitionLocations(file)
        elif db == 'firebase':
            res = FbFS.getPartitionLocations(file)
        return len(res)
    except Exception as e:
        print("Wrong Database")


def mapPartition(p,head,args,cal):
    # p: the filename of partition p
    # select rows from partition p according to args
    # if cal is not false, do the analyse
    print()

def combine(arr,cal):
    # combine the results of partitions' calculations
    res=""
    return res

def getHeader(file, db):
    print()

def manage(args, table, db, cal=False):
    # args: {} all fields and their values
    # table: String location of the table eg. /crime/Crime_data.csv
    # db: String
    # cal: String eg. min, max, count
    database = db
    # get the number of partitions
    partitions=getPartition(table,database)
    # get header of the csv file
    header=getHeader(table,db)
    results=[]
    for partition in range(partitions):
        if db == 'mongo':
            p = mongoClient.readPartition(table,partition+1)
        elif db == 'mysql':
            p = SqlFS.readPartition(table,partition+1)
        else:
            p = FbFS.readPartition(table,partition+1)
        result=mapPartition(p,header,args,cal)
        results.append(result)
    res=combine(results,cal)
    return res

