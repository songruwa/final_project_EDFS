from MongoFS import MongoFS
import ordertools as SqlFS
import firebase_commands as FbFS
import csv


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

def getHeader(file, db):
    '''
        getHeader reads the first partition of {file} in {db}, then use the first line
        to get attribute information. Returns a map<string, int> that maps attribute name to index in row (0-indexed)
        e.g. an csv table with schema ['id', 'name'], and value ['33', 'John Smith']. This functions will return a map:
        {'id': 0, 'name': 1}
    '''
    res = []
    try:
        if db == 'mongo':
            res = mongoClient.readPartition(file, 1)
        elif db == 'mysql':
            res = SqlFS.readPartition(file, 1)
        elif db == 'firebase':
            res = FbFS.readPartition(file, 1)
    except:
        print("ERROR: getHeader() read first partition of " + file + "  from " + db)

    attributeNameToIndex = {}
    reader = csv.reader([res[0]], delimiter=',')
    firstLine = reader.__next__()
    for i in range(len(firstLine)):
        attributeNameToIndex[firstLine[i]] = i
    return attributeNameToIndex


def mapPartition(p, header, args, cal):
    '''
        mapPartition() works as the mapper function.
        It processes the content of a partition and generate output

        :param p: partition content, list of strings with each string representing a row in the document
        :param header: csv header info, map<string, int>. It is used to get the index for an attribute in a row. e.g. header.get('second_attribute') == 1
        :param args: a map with key being the attribute name, and val being the target value. Only those tuples that has exact same attribute value will be keeped
        :param cal: a string indicating extra processing method we want to apply, MIN/MAX/COUNT  FIXME: how?
    '''
    ans = []
    reader = csv.reader(p, delimiter=',') # assume all files need to be processed are csv files
    for tuple in reader:
        isMatch = True
        for attribute, targetValue in args.items():
            if attribute not in header:
                print("WARN: invalid attribute " + attribute)
                continue

            index = header.get(attribute)
            if index >= len(tuple) or tuple[index] != targetValue:
                isMatch = False
                continue
        if isMatch:
            ans.append(tuple)

    if cal == 'COUNT':
        return len(ans)

    # if cal is not defined, just return all matching tuples
    return ans

def combine(mapPartitionResults, cal):
    # combine the results of partitions' calculations
    '''
        combine() works as the reducer function.
        It process all the results from the mapPartition() function

        :param mapPartitionResults: list of results, each element is a return value from mapPartition()
        :param cal: a string indicating extra processing method we want to apply, MIN/MAX/COUNT  FIXME: how?
    '''

    if cal == 'COUNT':
        ans = 0
        for count in mapPartitionResults:
            ans += int(count)
        return ans

    # if cal is not defined, just combine all tuples
    ans = []
    for mapResult in mapPartitionResults:
        ans += mapResult
    return ans

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


def main():
    args = {}
    args['Sex Code'] = 'M'
    args['Arrest Type Code'] = 'I'
    res = manage(args, '/crime/arrest.csv', 'mongo', 'COUNT')
    
    print(res)
    #for row in res:
        #print(row)

    mongoClient.close()
    return 0

if __name__ == "__main__":
    main()
