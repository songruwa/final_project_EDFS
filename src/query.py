
database=""

def getPartition(file,db):
    # get file's number of partitions from database db
    # return the number
    p=0
    return p

def mapPartition(p,args,cal):
    # p: the filename of partition p
    # select rows from partition p according to args
    # if cal is not false, do the analyse
    print()

def combine(arr,cal):
    # combine the results of partitions' calculations
    res=""
    return res

def manage(args, table, db, cal=False):
    # args: {} all fields and their values
    # table: String
    # db: String
    # cal: String eg. min, max, count
    database = db
    # get the number of partitions
    partitions=getPartition(table,database)
    results=[]
    for partition in partitions:
        p=table+partition # build the filename of partition
        result=mapPartition(p,args,cal)
        results.append(result)
    res=combine(results,cal)
    return res

