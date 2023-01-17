from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark-Assignment-1").getOrCreate()
path1 = "E:/CLG/py/Spark-assignment-1/datasets/user.csv"
path2 = "E:/CLG/py/Spark-assignment-1/datasets/transaction.csv"

# Reading CSV files
def ReadUserCsv(path1):
    df1 = spark.read.option("header", True).csv(path1)
    return df1

def ReadTransactionCsv(path2):
    df2 = spark.read.option("header", True).csv(path2)
    return df2

# Join two table
def Join():
    df1 = ReadUserCsv(path1)
    df2 = ReadTransactionCsv(path2)
    df3 = df1.join(df2, df1["user_id"] == df2["userid"])
    return df3

# Count of unique locations where each product is sold
def UniqueLocation():
    df3 = Join()
    df4 = df3.select("location ").distinct().count()
    return df4

# Products bought by each user
def ProductByEachUser():
    df3 = Join()
    df5 = df3.select("user_id", "product_description").orderBy("user_id")
    return df5

# Total spending done by each user on each product
def TotalSpending():
    df3 = Join()
    df6 = df3.select("user_id", "product_description", "price").orderBy("user_id")
    return df6
