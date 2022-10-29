import pandas as pd

import ordertools as ot
file = "../data/arrest_30.csv"
# ot.mkdir("/user/a")
# ot.mkdir("/user/b")
# ot.put("file", "/user/b", 5)
# ot.getPartitionLocations("/user/b/arrest_30.csv")
# ot.readPartition("/user/b/arrest_30.csv", 4)
df = pd.read_csv("../data/arrest_30.csv")
print(df.info)