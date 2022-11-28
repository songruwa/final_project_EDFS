from datetime import datetime as dt
import csv
from MongoFS import MongoFS
import ordertools as SqlFS
import firebase_commands as FbFS


# configs
mongo_conn_str = "mongodb+srv://x39j1017d:aLJCQ5mMc1kulqQf@cluster0.exky2zv.mongodb.net/?retryWrites=true&w=majority"

database=""
mongoClient = MongoFS(mongo_conn_str)


def getPartitionCount(file, db):
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

def getHeader(firstPartition):
    '''
        getHeader reads the first line of the first partition to get attribute information.
        return a string list[], each element represent one attribute
    '''
    reader = csv.reader([firstPartition[0]], delimiter=',')
    return reader.__next__()


def getNumOrDate(inputStr):
    '''
        getNumOrDate parse the input string and return one of following value:
        if inputStr does not has "/", return as float
        if inputStr has "/", treat as date in format "MM/DD/YYYY" and return datetime object
        if either case raises exception, simply return itself
    '''
    try:
        if inputStr.find("/") < 0:
            return float(inputStr)

        return dt.strptime(inputStr, "%m/%d/%Y")
    except:
        return inputStr


def mapPartition(p, header, argsEq, argsGte, argsLte, cal):
    '''
        mapPartition() works as the mapper function.
        It processes the content of a partition and generate output

        :param p: partition content, list of strings with each string representing a row in the document
        :param header: csv header info, map<string, int>. It is used to get the index for an attribute in a row. e.g. header.get('second_attribute') == 1
        :param argsEq: a map with key being the attribute name, and val being the target value. Only those tuples that has exact same attribute value will be keeped
        :param argsGte: a map with key being the attribute name, and val being the target value. Only those tuples that has greater or equal attribute value will be keeped
        :param argsLte: a map with key being the attribute name, and val being the target value. Only those tuples that has smaller or equal attribute value will be keeped
        :param cal: a string indicating extra processing method we want to apply, currently only support 'COUNT'
    '''
    ans = 0 if cal == 'COUNT' else []

    reader = csv.reader(p, delimiter=',') # assume all files need to be processed are csv files
    for tuple in reader:
        isMatch = True

        for attribute, targetValue in argsEq.items():
            if attribute not in header:
                print("WARN: invalid attribute " + attribute)
                continue

            index = header.index(attribute)
            if index >= len(tuple) or tuple[index] != targetValue:
                isMatch = False
                break
        if not isMatch:
            continue

        for attribute, lteValue in argsLte.items():
            if attribute not in header:
                print("WARN: invalid attribute " + attribute)
                continue

            index = header.index(attribute)
            if index >= len(tuple):
                isMatch = False
                break

            target = getNumOrDate(lteValue)
            actual =  getNumOrDate(tuple[index])
            if type(target) != type(actual) or actual > target:
                isMatch = False
                break

        if not isMatch:
            continue

        for attribute, gteValue in argsGte.items():
            if attribute not in header:
                print("WARN: invalid attribute " + attribute)
                continue

            index = header.index(attribute)
            if index >= len(tuple):
                isMatch = False
                break

            target = getNumOrDate(gteValue)
            actual =  getNumOrDate(tuple[index])
            if type(target) != type(actual) or actual < target:
                isMatch = False
                break

        if isMatch:
            if cal == 'COUNT':
                ans += 1
            else:
                ans.append(tuple)

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


def manage(table, database, argsEq, argsGte, argsLte, cal=None):
    # table: String represeting location of the table in EDFS eg. /crime/Crime_data.csv
    # database: String representing database, one of ["mongo", "firebase", "mysql"]
    # argsEq, argsGte, argsLte: a map with key being the attribute name, and val being the target value. Used for "equal", "greater or equal" and "less or equal" opearation
    # cal: String eg. min, max, count
    # get the number of partitions
    partitionCount = getPartitionCount(table, database)
    
    # get header of the csv file
    header = []
    results = []
    for partitionNum in range(1, partitionCount + 1):
        if database == 'mongo':
            p = mongoClient.readPartition(table, partitionNum)
        elif database == 'mysql':
            p = SqlFS.readPartition(table, partitionNum)
        else:
            p = FbFS.readPartition(table, partitionNum)

        # if this is the first partition, use its first line as header, and only use remaing rows as data
        if partitionNum == 1:
            header = getHeader(p)
            p = p[1:]

        result = mapPartition(p, header, argsEq, argsGte, argsLte, cal)
        results.append(result)

    res = combine(results, cal)
    return res, header


def main():
    argsEq = {}
    argsGte = {}
    argsLte = {}

    argsEq['Sex Code'] = 'M'
    argsEq['Arrest Type Code'] = 'I'
    argsGte['Arrest Date'] = '01/01/2022'
    argsLte['Arrest Date'] = '01/31/2022'

    res, _ = manage('/crime/arrest.csv', 'mongo', argsEq, argsGte, argsLte)

    print(res)

    mongoClient.close()
    return 0

if __name__ == "__main__":
    main()
