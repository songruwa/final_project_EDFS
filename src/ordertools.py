import pymysql
import sqlalchemy
import pandas as pd
import collections

pymysql.install_as_MySQLdb()

my_conn = sqlalchemy.create_engine("mysql+mysqldb://root:123456@localhost/dsci551project")


def __findpath(order):
    # order Example: /john/a
    if order == "//" or "/":
        orderlist = [""]
    else:
        orderlist = order.split('/')  # ["","john","a"]
    c_id = "" # c_id is current file id
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
            return False, c_id, orderlist[i:]
        else:
            c_id = t_list['id'][0]
    return True, c_id, orderlist[i:]


def __create_file_path(orderlist, start_id):
    mtime = 0 # mtime is stable now, it should be changed in the future version
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
    # use bfs to delete the file
    queue = collections.deque()
    queue.append(start_id)
    id_list = []
    while queue:
        t = queue.popleft()
        id_list.append(t)
        signlist = pd.read_sql(f"SELECT filename,isFile FROM `meta data` WHERE id = {t}", my_conn)
        sign = signlist["isFile"][0]
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
    name = f"fileid_{c_id}"
    dataframe.to_sql(name=name, con=my_conn)
    my_conn.execute(f"ALTER TABLE `{name}` PARTITION BY HASH(`index`) PARTITIONS {p};")


def mkdir(order):
    # order is the file path of the whole command
    # Example: mkdir /john/a --> order: "/john/a"
    tsign, c_id, iteration = __findpath(order) # c_id is current file id
    if not tsign:
        __create_file_path(iteration, c_id)
        return
    print("the path error or have exist!")


def rm(order):
    # order is the file path of the whole command
    # Example: rm /john/a --> order: "/john/a"
    tsign, c_id, iteration = __findpath(order)
    if not tsign:
        print("the path invalid")
        return
    __delete_file_path(c_id)
    return


def put(filename, order, k):
    # e.g., put(cars.csv, /user/john, k = # partitions)
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


def getPartitionLocations(order):
    tsign, c_id, iteration = __findpath(order)
    if not tsign:
        print("can not find file")
        return
    res = pd.read_sql(
        f"SELECT partition_name FROM `information_schema`.`PARTITIONS` AS p WHERE p.table_name = 'fileid_{c_id}'",
        my_conn)
    print(res)
    return


def readPartition(order, partition):
    tsign, c_id, iteration = __findpath(order)
    if not tsign:
        print("can not find file")
        return
    sql = f"SELECT * from fileid_{c_id} PARTITION(p{partition})"
    res = pd.read_sql(sql, my_conn)
    print(res)
    return
