"""实验一（3）：编写独立应用程序实现求平均值问题

数据集格式：每行形如 `姓名 成绩`，按学生姓名求所有课程的平均分。
原始数据分为三个文件（Algorithm成绩.txt / Database成绩.txt / Python成绩.txt）。
"""
from pyspark import SparkContext

# 初始化 SparkContext
sc = SparkContext('local', 'avgscore')

# 加载三个文件
lines1 = sc.textFile("file:///home/hadoop/PycharmProjects/pythonProject/Algorithm成绩.txt")
lines2 = sc.textFile("file:///home/hadoop/PycharmProjects/pythonProject/Database成绩.txt")
lines3 = sc.textFile("file:///home/hadoop/PycharmProjects/pythonProject/Python成绩.txt")

# 合并三个文件的内容
lines = lines1.union(lines2).union(lines3)

# 为每行数据新增一列 1，方便后续统计每个学生选修的课程数目
# data 的数据格式为 ('小明', (92, 1))
data = lines.map(lambda x: x.split(" ")).map(lambda x: (x[0], (int(x[1]), 1)))

# 根据 key（学生姓名）合计每门课程的成绩，以及选修的课程数目
# res 的数据格式为 ('小明', (269, 3))
res = data.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# 总成绩除以选修课程数，计算每个学生的平均分，round 保留两位小数
result = res.map(lambda x: (x[0], round(x[1][0] / x[1][1], 2)))

# 写入结果文件；repartition(1) 让结果合并到一个文件，不加会按分区写入多个文件
result.repartition(1).saveAsTextFile("file:///home/hadoop/spark/mycode/avgscore/result")

sc.stop()
