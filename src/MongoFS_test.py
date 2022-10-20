from MongoFS import MongoFS


# Replace the uri string with your MongoDB deployment's connection string.
conn_str = "mongodb+srv://x39j1017d:aLJCQ5mMc1kulqQf@cluster0.exky2zv.mongodb.net/?retryWrites=true&w=majority"

def main():
    # set a 5-second connection timeout
    client = MongoFS(conn_str)
    client.put("./data/small_test.csv", "/test/small_test.csv", 5)

    data = client.cat("/test/data.csv")
    for i in range(1, len(data)):
        if(len(data[i]) != len(data[0])):
            print(str(i) + "  is invalid!")
    
    print(len(data))
    print(data[0])
    print(data[-1])


if __name__ == "__main__":
    main()