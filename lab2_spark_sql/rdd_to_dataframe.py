"""实验二（5）：RDD 转 DataFrame，注册临时视图并执行 SQL"""
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import Row

if __name__ == "__main__":
    sc = SparkContext("local", "Simple App")
    spark = SparkSession.builder \
        .appName("Convert RDD to DataFrame") \
        .master("local[*]") \
        .getOrCreate()

    # 读取文本文件，按逗号切分后映射为 Row
    peopleRDD = sc.textFile("file:///home/hadoop/employee.txt")
    rowRDD = peopleRDD.map(lambda line: line.split(",")) \
                      .map(lambda attributes: Row(int(attributes[0]),
                                                  attributes[1],
                                                  int(attributes[2])))
    # 注册为临时视图
    rowRDD.createOrReplaceTempView("employee")

    # 用 SQL 查询
    personsDF = spark.sql("select * from employee")
    personsDF.rdd.map(lambda t: "id:" + str(t[0]) + ",Name:" + t[1] + ",age:" + str(t[2])).foreach(print)

    spark.stop()
