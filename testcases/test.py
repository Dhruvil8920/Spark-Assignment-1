from pyspark.sql.types import StringType, StructField, LongType, IntegerType, StructType
from Main.main import *
import unittest

spark = SparkSession.builder.getOrCreate()
class TestMyFunc(unittest.TestCase):

# ANS : 1 Total count of unique location
    def TestUniqueLocation(self):
        self.assertTrue(UniqueLocation(), 3)

# ANS : 2 Product By Each User
    def TestProductByEachUser(self):
        def compareDF(spark):
            schema = StructType([StructField("user_id", IntegerType(), True), \
                                 StructField("product_description", StringType(), True)])
            data = [(101, "speaker"), \
                    (101, "fridge"), \
                    (101, "mouse"), \
                    (102, "keyboard"), \
                    (102, "chair"), \
                    (103, "tv"), \
                    (105, "laptop"), \
                    (105, "sofa"), \
                    (105, "bed"), \
                    (105, "phone")]
            df = spark.createDataFrame(data=data, schema=schema)
            return df

        self.assertTrue(ProductByEachUser(), compareDF(spark))

# ANS: 3 Total Spending
    def TestTotalSpending(self):
        def checkDF(spark):
            schema = StructType([StructField("user_id", IntegerType(), True), \
                                 StructField("product_description", StringType(), True), \
                                 StructField("price", LongType(), False)])

            data = [(101, "speaker", 500), \
                    (101, "fridge", 35000), \
                    (101, "mouse", 700), \
                    (102, "keyboard", 900), \
                    (102, "chair", 1000), \
                    (103, "tv", 34000), \
                    (105, "laptop", 60000), \
                    (105, "sofa", 55000), \
                    (105, "bed", 100), \
                    (105, "phone", 20000)]
            df = spark.createDataFrame(data=data, schema=schema)
            return df

        self.assertTrue(TotalSpending(), checkDF(spark))

if __name__ == "__main__":
    unittest.main()
class TestMyFunc(unittest.TestCase):

# ANS : 1 Total count of unique location
    def testUniqueLocation(self):
        self.assertTrue(UniqueLocation(), 3)

# ANS : 2 Product By Each User
    def testProductByEachUser(self):
        def compareDF(spark):
            schema = StructType([StructField("user_id", IntegerType(), True), \
                                 StructField("product_description", StringType(), True)])
            data = [(101, "speaker"), \
                    (101, "fridge"), \
                    (101, "mouse"), \
                    (102, "keyboard"), \
                    (102, "chair"), \
                    (103, "tv"), \
                    (105, "laptop"), \
                    (105, "sofa"), \
                    (105, "bed"), \
                    (105, "phone")]
            df = spark.createDataFrame(data=data, schema=schema)
            return df

        self.assertTrue(ProductByEachUser(), compareDF(spark))

# ANS: 3 Total Spending
    def testTotalSpending(self):
        def checkDF(spark):
            schema = StructType([StructField("user_id", IntegerType(), True), \
                                 StructField("product_description", StringType(), True), \
                                 StructField("price", LongType(), False)])

            data = [(101, "speaker", 500), \
                    (101, "fridge", 35000), \
                    (101, "mouse", 700), \
                    (102, "keyboard", 900), \
                    (102, "chair", 1000), \
                    (103, "tv", 34000), \
                    (105, "laptop", 60000), \
                    (105, "sofa", 55000), \
                    (105, "bed", 100), \
                    (105, "phone", 20000)]
            df = spark.createDataFrame(data=data, schema=schema)
            return df

        self.assertTrue(TotalSpending(), checkDF(spark))

if __name__ == "__main__":
    unittest.main()
