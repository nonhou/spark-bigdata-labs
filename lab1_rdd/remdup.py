"""实验一（2）：编写独立应用程序实现数据去重"""
from pyspark import SparkContext

sc = SparkContext('local', 'remdup')

# 加载两个文件 A 和 B
lines1 = sc.textFile("file:///home/hadoop/PycharmProjects/pythonProject/A.txt")
lines2 = sc.textFile("file:///home/hadoop/PycharmProjects/pythonProject/B.txt")

# 合并两个文件的内容
lines = lines1.union(lines2)

# 去重操作
distinct_lines = lines.distinct()

# 排序操作
res = distinct_lines.sortBy(lambda x: x)

# 将结果写入 result 文件；repartition(1) 让结果合并到一个文件中，
# 不加的话会按分区写入多个文件
res.repartition(1).saveAsTextFile("file:///usr/local/spark/mycode/remdup/result")

sc.stop()
