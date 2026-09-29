"""实验一（1）：pyspark 交互式编程练习
数据集：chapter5-data1.txt，每行格式为 姓名,课程名,成绩
"""
from pyspark import SparkContext

sc = SparkContext('local', 'rdd-practice')
lines = sc.textFile("file:///home/hadoop/chapter5-data1.txt")

# --- 该系总共有多少学生 ---
print(lines.map(lambda x: x.split(",")[0]).distinct().count())

# --- 该系共开设了多少门课程 ---
print(lines.map(lambda x: x.split(",")[1]).distinct().count())

# --- Tom 同学的总成绩平均分 ---
res = lines.map(lambda x: x.split(",")).filter(lambda x: x[0] == "Tom")
score = res.map(lambda x: int(x[2]))
num = res.count()
sum_score = score.reduce(lambda x, y: x + y)
print(sum_score / num)

# --- 求每名同学选修的课程门数 ---
res = lines.map(lambda x: x.split(",")).map(lambda x: (x[0], 1))
res.reduceByKey(lambda x, y: x + y).foreach(print)

# --- 该系 DataBase 课程共有多少人选修 ---
print(lines.map(lambda x: x.split(",")).filter(lambda x: x[1] == "DataBase").count())

# --- 各门课程的平均分 ---
res = lines.map(lambda x: x.split(",")).map(lambda x: (x[1], (int(x[2]), 1)))
temp = res.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
avg = temp.map(lambda x: (x[0], round(x[1][0] / x[1][1], 2)))
avg.foreach(print)

# --- 使用累加器计算共有多少人选了 DataBase 这门课 ---
base_count = sc.accumulator(0)


def count_base(line):
    global base_count
    if "DataBase" in line:
        base_count.add(1)


lines.foreach(count_base)
print(base_count.value)

sc.stop()
