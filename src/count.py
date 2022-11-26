from pyspark import SparkContext,SparkConf
from datetime import datetime as dt

def analyse(filename,argsEq, argsGte, argsLte, cal=None):
    
    conf = SparkConf().setAppName('WordCount').setMaster('local[*]')
    #conf = SparkConf().setAppName('WordCount').setMaster("spark://ec2-13-57-254-114.us-west-1.compute.amazonaws.com:7077")
    conf.set("spark.shuffle.service.enabled", "false").set("spark.dynamicAllocation.enabled", "false")
    conf.set('spark.driver.memory','2g')
    conf.set('spark.executor.memory', '2g')
    conf.set("spark.cores.max", '4')
    sc = SparkContext(conf=conf)

    rdd_init = sc.textFile(filename)
    header=rdd_init.first()
    header=header.split(',')
    # print("header")
    # print(header)
    rdd_map=rdd_init.map(lambda x:x.split(','))
    # print("map 1")
    # print(rdd_map.collect())
    # [['0', '0', '11/16/2021', 'N Hollywood', 'M', 'I'], ['1', '1', '9/1/2021', '77th Street', 'M', 'M']]
    rdd_map = rdd_map.map(lambda row: map_fun(header, row,argsEq, argsGte, argsLte, cal))
    # print("map 2")
    # print(rdd_map.collect())
    # [1, 1]
    rdd_map=rdd_map.filter(lambda row:row is not None)
    # print("filter")
    # print(rdd_map.collect())
    rdd_res= rdd_map.reduce(lambda a,b:a+b)
    print(rdd_res)
    # 2
    sc.stop()
    return rdd_res, header


def map_fun(header,row,argsEq, argsGte, argsLte, cal):
    isMatch = True
    for attribute, targetValue in argsEq.items():
        if attribute not in header:
            print("WARN: invalid attribute " + attribute)
            continue

        index = header.index(attribute)
        if index >= len(row) or row[index] != targetValue:
            isMatch = False
            break
    if not isMatch:
        return map_return(isMatch,cal,header,row)
    for attribute, lteValue in argsLte.items():
        if attribute not in header:
            print("WARN: invalid attribute " + attribute)
            continue

        index = header.index(attribute)
        if index >= len(row) or getNumOrDate(row[index]) > getNumOrDate(lteValue):
            isMatch = False
            break
    if not isMatch:
        return map_return(isMatch,cal,header,row)

    for attribute, gteValue in argsGte.items():
        if attribute not in header:
            print("WARN: invalid attribute " + attribute)
            continue

        index = header.index(attribute)
        if index >= len(row) or getNumOrDate(row[index]) < getNumOrDate(gteValue):
            isMatch = False
            break

    return map_return(isMatch,cal,header,row)

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

def map_return(match,cal,header,row):
    if cal=='COUNT':
        if match: return 1
        else: return 0
    else:
        if match:
            res={}
            for i in range(1,len(header)):
               res[header[i]]=row[i]
            return [res]
        else:
            return


if __name__ == '__main__':

    res,totaltime=analyse('arrest.csv',{ "Gender": "M",  "Arrest Type Code": "I"},{ "Date": "11/15/2021" },{ "Date": "11/17/2021" })
    #request body:{"table": "/crime/arrest.csv", "database": "mongo", "argsEq": { "Gender": "M",  "Arrest Type Code": "I"}, "argsLte": { "Arrest Date": "01/05/2022" }, "argsGte": { "Arrest Date": "01/01/2022" },"cal": "COUNT"}
    print(res)
    # [{'Date': '11/16/2021', 'Location': 'N Hollywood', 'Gender': 'M', 'Arrest Type Code': 'I'}, {'Date': '11/17/2021', 'Location': 'Devonshire', 'Gender': 'M', 'Arrest Type Code': 'I'},...]
    print(totaltime)
