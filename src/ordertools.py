import pymysql
import sqlalchemy
import pandas as pd
import collections
import itertools

pymysql.install_as_MySQLdb()

address = "mysql+mysqldb://admin:dsci-551@database-2.cxay4obryyr9.us-west-1.rds.amazonaws.com:3306/dsci551project"
# address = "mysql+mysqldb://root:123456@localhost/dsci551project"
my_conn = sqlalchemy.create_engine(address)


def __findpath(order):
    # order Example: /john/a
    """
    function: recieve a path, return the relative information about the target file
    return:
        sign: Bool value, represent whether the function find the path successful
        c_id: the id of current file
        orderlist[i:]: the function also return the order name slice behind current file name
    example:
        the file system exist: /john/b
        run function __findpath('/john/b')--> it will return True, the id of /john/b, []
        run function __findpath('/john/a')--> it will return False,the id of /john,['b']
        run function __findpath('//') --> it will return True, the id of root,['john','b']
        the file system is empty(it is not the correct status):
        run function __findpath('/john/b')--> False ,"", ['', 'user', 'b']
        the file only contain root file:
        run function __findpath('/john/b')--> False ,id of root, ['user', 'b']
    """
    if order == "//" or order == "/":
        orderlist = [""]
    else:
        orderlist = order.split('/')  # ["","john","a"]
    print("orderlist:--> ", orderlist)
    c_id = ""  # c_id is current file id

    i = 0
    for i in range(len(orderlist)):
        filename = orderlist[i]
        if i == 0:  # check root if exist
            t_list = pd.read_sql('SELECT * FROM `meta data` WHERE filename = ""', my_conn)
        else:
            # list all information that filename = orderlist[i]
            t_list = pd.read_sql(f"SELECT t.id FROM (SELECT * FROM `meta data` WHERE filename = '{filename}') AS t, "
                                 f"`directory` AS d WHERE d.`children_id` = t.id AND `parent_id` = {c_id}", my_conn)
        if t_list.empty:
            print(False, c_id, orderlist[i:])
            return False, c_id, orderlist[i:]
        else:
            c_id = t_list['id'][0]
    print(True, c_id, orderlist[i:])
    return True, c_id, orderlist[i:]


def __create_file_path(orderlist, start_id):
    """
        function: recieve a orderlist and start_id, edit(create) the table of directory and meta data
        process:
            if pid is "", create the root(it is not the correct case)
            else edit the directory and meta data tables step by step
        tips:
            mtime is an useless variable now
    """
    mtime = 0  # mtime is stable now, it should be changed in the future version
    p_id = start_id
    for name in orderlist:
        # create from empty table
        if not p_id:
            my_conn.execute(
                f"INSERT INTO `meta data` (`filename`, `isFile`, `mtime`) VALUES ('{name}', '0', '{mtime}')")
            result = pd.read_sql("SELECT LAST_INSERT_ID()", my_conn)
            p_id = result["LAST_INSERT_ID()"][0]
        else:
            my_conn.execute(
                f"INSERT INTO `meta data` (`filename`, `isFile`, `mtime`) VALUES ('{name}', '0', '{mtime}')")
            result = pd.read_sql("SELECT LAST_INSERT_ID()", my_conn)
            c_id = result["LAST_INSERT_ID()"][0]
            my_conn.execute(
                f"INSERT INTO `directory` (`parent_id`, `children_id`) VALUES ('{p_id}', '{c_id}')")
            p_id = c_id
    return


def __delete_file_path(start_id):
    """
            function: recieve a start_id, delete all the id and file under the start id in the tree structure(include start id)
            process:
                if pid is "", create the root(it is not the correct case)
                else edit the directory and meta data tables by bfs algorithm
            tips:
    """
    # use bfs to delete the file
    queue = collections.deque()
    queue.append(start_id)
    id_list = []
    while queue:
        t = queue.popleft()
        signlist = pd.read_sql(f"SELECT filename,isFile FROM `meta data` WHERE id = {t}", my_conn)
        sign = signlist["isFile"][0]
        if signlist["filename"][0] != "":
            id_list.append(t)
        if sign:
            my_conn.execute(f"DROP TABLE `fileid_{t}`")
        children = list(pd.read_sql(f"SELECT children_id AS id FROM DIRECTORY WHERE parent_id = {t}", my_conn)["id"])
        queue += children
    my_conn.execute(
        f"DELETE FROM `directory` WHERE children_id IN ({str(id_list)[1:-1]})"
    )
    print("the directory table drop success")
    my_conn.execute(
        f"DELETE FROM `meta data` WHERE id IN ({str(id_list)[1:-1]})"
    )
    print("the meta data table drop success")
    return


def __create_file(c_id, dataframe, p):
    """
        function: recieve a c_id, pandas dataframe and partitions number
        process:
                create a file table with c_id, in the system, we use hash partitions
        tips:
    """
    name = f"fileid_{c_id}"
    dataframe.to_sql(name=name, con=my_conn)
    my_conn.execute(f"ALTER TABLE `{name}` PARTITION BY HASH(`index`) PARTITIONS {p};")


def mkdir(order):
    # order is the file path of the whole command
    # Example: mkdir /john/a --> order: "/john/a"

    tsign, c_id, iteration = __findpath(order)  # c_id is current file id
    if not tsign:
        __create_file_path(iteration, c_id)
        return True
    print("the path error or have exist!")
    print(tsign, c_id, iteration)
    return False


def rm(order):
    # order is the file path of the whole command
    # Example: rm /john/a --> order: "/john/a"
    tsign, c_id, iteration = __findpath(order)
    if not tsign:
        print("the path invalid")
        return False
    __delete_file_path(c_id)
    return True


def put(filename, order, k):
    # e.g., put(cars.csv, /user/john, k = # partitions)

    try:
        mtime = 0
        tsign, p_id, iteration = __findpath(order)
        my_conn.execute(
            f"INSERT INTO `meta data` (`filename`, `isFile`, `mtime`) VALUES ('{filename}', '1', '{mtime}')")
        result = pd.read_sql("SELECT LAST_INSERT_ID()", my_conn)
        c_id = result["LAST_INSERT_ID()"][0]
        my_conn.execute(
            f"INSERT INTO `directory` (`parent_id`, `children_id`) VALUES ('{p_id}', '{c_id}')")
        dataframe = pd.read_csv(filename)
        __create_file(c_id, dataframe, k)
        return True
    except:
        return False


def getPartitionLocations(order):
    tsign, c_id, iteration = __findpath(order)
    if not tsign:
        print("can not find file")
        return
    res = pd.read_sql(
        f"SELECT partition_name FROM `information_schema`.`PARTITIONS` AS p WHERE p.table_name = 'fileid_{c_id}'",
        my_conn)
    print(res)
    return res.values.tolist()


def readPartition(order, partition):
    tsign, c_id, iteration = __findpath(order)
    if not tsign:
        print("can not find file")
        return
    sql = f"SELECT * from fileid_{c_id} PARTITION(p{partition})"
    res = pd.read_sql(sql, my_conn)
    print(res)

    return res.values.tolist()


# # order example: /dsci551project
# return all tables in dsci551project
def ls(order):
    # eg: ls /dsci551project
    # it will return all child file name of dsci551project
    tsign, c_id, iteration = __findpath(order)
    if not tsign:
        print("the path invalid")
        return False
    # now has folder id; find folder id in direcdtory, then find all file whose parent_id equal to folder id
    # first find child_id in `meta data`
    query = 'SELECT children_id FROM directory WHERE parent_id = {id}'.format(id=c_id)
    df = pd.read_sql(query,my_conn)

    # second, find file name based on child_id in `meta data`
    res = []
    for i in df['children_id'].tolist():
        n_query = 'SELECT filename FROM `meta data` WHERE id = {id}'.format(id=i)
        daf = pd.read_sql(n_query,my_conn)
        res.append(daf['filename'].tolist())
    res = list(itertools.chain.from_iterable(res))

    return res

# example:  '/dsci551project/LAPD_call/Area_Occ'
# return all data entries in Area_Occ column
# other instance: '/dsci551project/LAPD_call'
# return all table data entries in LAPD_call
def cat(order):
    tsign, c_id, iteration = __findpath(order)
    if not tsign:
        print("the path invalid")
        return False
    # first, even though it's a valid path, determine whether it's a file
    # if not a file, return 'the path only have directory'
    nn_query = 'SELECT isFile FROM `meta data` WHERE id = {id}'.format(id=c_id)
    df = pd.read_sql(nn_query, my_conn)
    if 0 in df['isFile'].tolist():
        return 'Not valid; the path only have directory'
    elif 1 in df['isFile'].tolist():
        # why table name in this way?
        # bc when in database imported table's name is in this style: 'fileid_fileID'
        nn_query = 'SELECT * FROM {table}'.format(table='fileid_'+str(c_id))
        df = pd.read_sql(nn_query, my_conn)
        return df
