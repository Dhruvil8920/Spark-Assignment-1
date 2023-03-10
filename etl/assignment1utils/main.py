from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark-Assignment-1").getOrCreate()
path1 = "E:/CLG/py/Spark-assignment-1/datasets/user.csv"
path2 = "E:/CLG/py/Spark-assignment-1/datasets/transaction.csv"

# Reading CSV files
def ReadUserCsv(path1):
    df1 = spark.read.option("header", True).option("mode", "FAILFAST").csv(path1)
    return df1

def ReadTransactionCsv(path2):
    df2 = spark.read.option("header", True).option("mode", "FAILFAST").csv(path2)
    return df2

# Join two table
def JoinClm():
    df1 = ReadUserCsv(path1)
    df2 = ReadTransactionCsv(path2)
    df3 = df1.join(df2, df1["user_id"] == df2["userid"])
    return df3

# Count of unique locations where each product is sold
def UniqueLocation():
    df3 = JoinClm()
    df4 = df3.select("location ").distinct().count()
    coun = f"The count of total no. of unique city where each product bought is: {df4}"
    return coun

# Products bought by each user
def ProductByEachUser():
    df3 = JoinClm()
    df5 = df3.select("user_id", "product_description").orderBy("user_id")
    return df5

# Total spending done by each user on each product
def TotalSpending():
    df3 = JoinClm()
    df6 = df3.select("user_id", "product_description", "price").orderBy("user_id")
    return df6
