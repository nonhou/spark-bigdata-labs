"""实验二（1）~（4）：读取 JSON、去重、筛选、按年龄分组统计"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkSQL-Basic") \
    .master("local[*]") \
    .getOrCreate()

# 读取 JSON 生成 DataFrame
df = spark.read.json("employee.json")
df.show()

# 去重
df = df.dropDuplicates()
df.show()

# 查询全部记录
df.select("age", "id", "name").show()

# 筛选年龄大于 30 的记录
df.filter(df["age"] > 30).show()

# 按年龄分组统计人数
df.groupBy("age").count().show()

spark.stop()
