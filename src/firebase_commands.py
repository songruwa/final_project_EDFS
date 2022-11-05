import json
import os
import requests
import ssl
import urllib.request
import firebase_admin
from firebase_admin import storage, credentials

firebaseConfig={
  "type": "service_account",
  "project_id": "edfs-7268e",
  "private_key_id": "1d2e42f8aad3fb80f4354b40e020429ea57e9384",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDQO0b2C3IIqMmv\nmQE2IUcba5BZ/07a3ZTh7Qski3+ANC/XP2dkKTwLtbN5XkJeExQBPcDiBQvdMezZ\niK+MKFjj0jA1FNHhcMZbMCtGWuKAPlnMaY9f+YpShQwQaHp58YBRwE/GfMW4H1lU\nb7VaENYso+diZWkRo7FUixkqllHQjz4GFNgXcfYg85G1C1dmpg7T48Yya2FU07xg\nXfJ+DeQMeOJJP69ZxrrumcV6BuyAYBZhsV70xGrXGFCeUe9C9Xy4enCle+G8WWJo\n7BkpmRnIcJr8QWWTGsBaLS16nWi8nA0osh60Y4bSpLKgaUtEobWc9agFKfv978dZ\njATpvcbhAgMBAAECggEABTtbyCzALW7rtDjvbcHHBjpyVwM0NAmCNhguTsqFjK8c\nlyQ3pi2z+2u8rStZnxHQgsKKJfKUk/biVEWNA1UR68BCx+IdERKR4UoOkzogW8Oo\nfGgpGxVRxFvhOMBziXg48FChsqEr/jCt2fAoMDopmhu7Nt7JFasCIT3mZPDVVs/N\n0st/2BABTyWpTYSiT6IVeXmtGl7xLBJF72G8BkM+xWRkvSxztqKyAFfMP+5UDTsU\nNTkc50Hc3PDlL1E+c+scwuS9yNrVDfbAXbWppT7NpZ8cbkesQtYQd8P2ewIkqnGG\n733tC+Vxh2EncvkT50XWce9AzYs6uoBcaZzvCwpQkwKBgQD+Ixq08GUIqjgxfghX\nSx6Oc64ywOd7hWlF1qLmWdUZSyjY6XORDBlxWVbRWfzkOFR1Mz75WxpJx8xBdMlO\nROUzIIh2E0DPjDVbFLFsQGYg0RXZH6dZhsHatgU+wlYzHIArhF0NL9Uw93xrfHqX\nfODXGsqIOosIgxNJ0lp2xVHvDwKBgQDRwgecQ2/HbZMA0o1uMI9QrzHvqQAMX+68\nHgV+yNo3EyLblVo/+YaHTanC06Pcah0+TuUnk/rMd/hnFUGQRmicx1zfRC15+B6G\ni3Z4cwkIj6He84m6EXKnA4gCnSvMlm9HMkKw9Th3UdrtgNTIvBuKe1/vr0t6omyu\nJFwuZKLrDwKBgD3sjdhi7ytyVqjwndWvby2k0GB7kqwNcP5cc2sPnpZAMx7Pm6JP\nQW/WJgpzE+UEOMgqCYE3CqvJrGKSs26H6RVRKw8iV9t6vZGI9EZ9VqVObTVvuhZd\neKTzT0ngjqJ7olt4MDhDXH5G/6EtPq3k9uBTHeCd6zzSu4N0ZyijofhPAoGAJt/g\n7TMKWlmSbwqWr59MUFXH2XTmz2RQKIkf15l958siILQTX9vs9NKN7c+vhMAd31hb\n2/Pu/UoWvXQRDJ4f8T55ld9a1koHzkO2lygqum10QI++LL/jEdTzthhO78HJqdZE\nyesIpgSDoJTJ2tISAJ5Q72j7giTsI3IVq3t3Pj0CgYEAqvUMESoKiS6WQ3cxGI67\njquCFWhIXRozo+Ti+GhiDHlPJUBZ6n9Z+hBdpQUWKjNrIAJn207AnoK15U9+O4lD\nTnVIXl8iFgwzAVip0/KdSrA2/px5fz3f0Rgk6UHKpnV2rILmp0BFdLWPEZWE9DIG\nmEXhWCIRrb/sDJtaGa0Jolk=\n-----END PRIVATE KEY-----\n",
  "client_email": "firebase-adminsdk-c1204@edfs-7268e.iam.gserviceaccount.com",
  "client_id": "105665766978137470648",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-c1204%40edfs-7268e.iam.gserviceaccount.com"
}

cred = credentials.Certificate(firebaseConfig)
firebase_admin.initialize_app(cred, {"databaseURL": "https://edfs-7268e-default-rtdb.firebaseio.com","storageBucket": "edfs-7268e.appspot.com"})

base_url="https://edfs-7268e-default-rtdb.firebaseio.com"

ssl._create_default_https_context = ssl._create_unverified_context


def mkdir(dir):
    # create a directory in file system, e.g., mkdir /user/john
    # dir: String
    index=dir.rfind('/')
    url=base_url+dir[:index]+".json"
    # {"john":"dir"}
    content={dir[index+1:]:"dir"}
    data=json.dumps(content)
    res = requests.patch(url, data)
    #print(type(res.status_code))
    if res.status_code == 200:
        return True
    else:
        return False



def ls(dir):
    # listing content of a given directory, e.g., ls /user
    # dir: String
    # return: list  e.g. ['jason', 'john']
    url=base_url+dir+".json"
    response= requests.get(url)
    data=response.text
    data_json=json.loads(data)
    keys=list(data_json.keys())
    #print(keys)
    return keys


def cat(file):
    # display content of a file, e.g., cat /user/john/testfile.txt
    bucket = storage.bucket()
    blob = bucket.blob(file[1:])
    path = "https://firebasestorage.googleapis.com/v0" + blob.path + "?alt=media"
    f = urllib.request.urlopen(path).read()
    # print(f)
    res=str(f,encoding='utf-8').replace('\r','')
    res=res.strip()
    res=res.split('\n')
    print(res) # ['This is a test file', 'edfs is a file system', 'test txt working', 'usc ucla', 'this is a testing file']
    print(type(res)) # <class 'list'>
    # print(f)  # b'this is a test file'
    # print(type(f))  # <class 'bytes'>
    return res


def rm(file):
    # remove a file from the file system, e.g., rm /user/john/hello.txt
    flag = False
    if '.' in file:
        flag = True
    bucket = storage.bucket()
    try:
        if flag:
            blob=bucket.blob(file[1:])
            blob.delete()
            file=file.split('.')[0]
            file2=file[1:]+"_"
            blobs = bucket.list_blobs(prefix=file2)
            for bl in blobs:
                bl.delete()
        else:
            blobs = bucket.list_blobs(prefix=file[1:])
            for blob in blobs:
                blob.delete()
    except Exception as e:
        pass
    url = base_url + file.split(".")[0] + ".json"
    res = requests.delete(url)
    if res.status_code == 200:
        return True
    else:
        return False


def put(filename,location,k):
    # uploading a file to file system, e.g., put(cars.csv, /user/john, k = # partitions) will
    # upload a file cars.csv to the directory /user/john in EDFS.
    # filename: String, the uploading file's name
    # location : String, the aim location of uploading the file, e.g. "/user/john"

    path = location+"/"+filename  # /user/john/cars.csv
    bucket = storage.bucket()
    blob = bucket.blob(path[1:])
    blob.upload_from_filename(filename)
    sourceFileData = open(filename, 'r', encoding='utf-8')
    ListOfLine = sourceFileData.read().splitlines()  # split content in lines and save into a list
    n = len(ListOfLine)
    p = n // k + 1
    for i in range(k):
        tmp=filename.split('.')
        subpath=location[1:]+"/"+tmp[0]+"_"+str(i+1)+"."+tmp[1]
        subname=tmp[0]+"_"+str(i+1)+"."+tmp[1]
        subdata=open(subname, "w", encoding='utf-8')
        if (i == k - 1):
            for line in ListOfLine[i * p:]:
                subdata.write(line + '\n')
        else:
            for line in ListOfLine[i * p:(i + 1) * p]:
                subdata.write(line + '\n')
        subdata.close()
        blob=bucket.blob(subpath)
        blob.upload_from_filename(subname)
        os.remove(subname)
    # {"cars":{"path": "csv", "partitions":3}}
    url = base_url + location + ".json"
    file = filename.split(".")
    content = {file[0]: {"type": file[1], "partitions": k}}
    data = json.dumps(content)
    res = requests.patch(url, data)
    # delete the original file or not depending on whether we want to save it on our server permanently
    # os.remove(filename)
    # print(res.status_code)
    if res.status_code == 200:
        return True
    else:
        return False


def getPartitionLocations(file):
    # this method will return the locations of partitions of the file.
    # file: String eg. /user/john/test.txt
    # return eg: ["gs://edfs-7268e.appspot.com/user/john/testfile1_1.txt"]
    tmp=file.split(".")
    url=base_url+tmp[0]+"/partitions.json"
    response=requests.get(url)
    partitions=int(response.text)
    base_path="gs://edfs-7268e.appspot.com"
    paths=[]
    for partition in range(partitions):
        path=base_path+tmp[0]+"_"+str(partition+1)+"."+tmp[1]
        paths.append(path)
    print(paths)
    return paths


def readPartition(file, partition):
    # this method will return the content of partition # of the specified file.
    # The portioned data will be needed in the second task for parallel processing
    # file: String
    # partition : int
    # eg. readPartition("/user/john/test.txt", 2)
    tmp=file.split(".")
    filename=tmp[0]+"_"+str(partition)+"."+tmp[1]
    bucket = storage.bucket()
    blob = bucket.blob(filename[1:])
    path = "https://firebasestorage.googleapis.com/v0" + blob.path + "?alt=media"
    f = urllib.request.urlopen(path).read()
    res = str(f, encoding='utf-8').replace('\r', '')
    res = res.strip()
    res = res.split('\n')
    print(res)
    print(type(res))
    return res


if __name__ == '__main__':
    # res=mkdir("/user/Lucy")
    # print(res)
    #result=ls("/user")
    # rm("/user/john/testcsv.csv")
    # res=put("testcsv.csv","/user/john",3)
    # print(res)
    cat("/user/john/testcsv.csv")
    # readPartition("/user/john/testcsv.csv", 2)
    #getPartitionLocations("/user/john/test.txt")



