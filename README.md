# 大数据分析与应用实验（Spark RDD / Spark SQL）

《大数据分析与应用》课程实验报告与代码。在 Ubuntu 环境下用 PySpark 完成 RDD 编程与 Spark SQL 两组实验。

## 环境要求

| 组件 | 版本 |
| --- | --- |
| 操作系统 | Ubuntu 16.04 以上 |
| Spark | 3.4.0 以上 |
| Python | 3.8.18 以上 |
| pyspark | 3.4.0 |

```bash
pip install -r requirements.txt
```

---

## 实验一：RDD 编程初级实践

### 数据集

某大学计算机系成绩数据 `chapter5-data1.txt`，每行格式为 `姓名,课程名,成绩`，例如：

```
Tom,DataBase,80
Tom,Algorithm,50
```

### 任务与实现

**（1）pyspark 交互式编程**

| 任务 | 用到的算子 |
| --- | --- |
| 统计该系学生总数 | `map` + `distinct` + `count` |
| 统计共开设多少门课程 | `map` + `distinct` + `count` |
| 求 Tom 同学的总成绩平均分 | `filter` + `map` + `count` + `reduce` |
| 求每名同学选修的课程门数 | `map` + `reduceByKey` |
| 统计 DataBase 课程选修人数 | `filter` + `count` |
| 求各门课程的平均分 | `map` + `reduceByKey`（分数与计数打包后相除） |
| 用累加器统计选 DataBase 的人数 | `SparkContext.accumulator` |

各门课程平均分的实现：

```python
res = lines.map(lambda x: x.split(",")).map(lambda x: (x[1], (int(x[2]), 1)))
temp = res.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
avg = temp.map(lambda x: (x[0], round(x[1][0] / x[1][1], 2)))
avg.foreach(print)
```

输出示例：

```
('Software', 50.91)
('DataBase', 50.54)
('Algorithm', 48.83)
('CLanguage', 50.61)
('OperatingSystem', 54.94)
('Python', 57.82)
('ComputerNetwork', 51.9)
('DataStructure', 47.57)
```

求 Tom 的平均分：

```python
res = lines.map(lambda x: x.split(",")).filter(lambda x: x[0] == "Tom")
score = res.map(lambda x: int(x[2]))
num = res.count()
sum_score = score.reduce(lambda x, y: x + y)
avg = sum_score / num
```

**（2）编写独立应用程序实现数据去重**

合并 `A.txt` 与 `B.txt` 两个文件，去重后按字典序排序输出：

```python
from pyspark import SparkContext

sc = SparkContext('local', 'remdup')
lines1 = sc.textFile("file:///home/hadoop/PycharmProjects/pythonProject/A.txt")
lines2 = sc.textFile("file:///home/hadoop/PycharmProjects/pythonProject/B.txt")

lines = lines1.union(lines2)
distinct_lines = lines.distinct()
res = distinct_lines.sortBy(lambda x: x)

# repartition(1) 让结果合并到单个文件，否则会写出多个分片
res.repartition(1).saveAsTextFile("file:///usr/local/spark/mycode/remdup/result")
```

**（3）编写独立应用程序实现求平均值问题**

三个成绩文件（`Algorithm成绩.txt`、`Database成绩.txt`、`Python成绩.txt`）合并后，按学生姓名求所有课程的平均分。每行格式为 `姓名 成绩`。

```python
# 为每行数据新增一列 1，方便统计每个学生选修的课程数目
# data 的数据格式为 ('小明', (92, 1))
data = lines.map(lambda x: x.split(" ")).map(lambda x: (x[0], (int(x[1]), 1)))

# 按学生姓名合计总成绩与课程数：('小明', (269, 3))
res = data.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# 总成绩 / 课程数 = 平均分
result = res.map(lambda x: (x[0], round(x[1][0] / x[1][1], 2)))
```

输出：

```
('小红', 83.67)
('小新', 88.33)
('小明', 89.67)
('小丽', 88.67)
```

---

## 实验二：Spark SQL 编程初级实践

### 数据集

JSON 格式员工数据，存在重复记录与缺失字段：

```json
{ "id":1 , "name":"Ella" , "age":36 }
{ "id":2, "name":"Bob","age":29 }
{ "id":3 , "name":"Jack","age":29 }
{ "id":4 , "name":"Jim","age":28 }
{ "id":4 , "name":"Jim","age":28 }
{ "id":5 , "name":"Damon" }
{ "id":5 , "name":"Damon" }
```

### 任务与实现

**（1）读取 JSON 并创建 DataFrame**

```python
spark = SparkSession.builder.appName("SparkSQL").master("local[*]").getOrCreate()
df = spark.read.json("employee.json")
df.show()
```

**（2）去重**

`dropDuplicates()` 去除重复记录，去重后剩 5 条。

**（3）查询全部记录与筛选**

```python
df.select("age", "id", "name").show()
df.filter(df["age"] > 30).show()
```

**（4）按年龄分组统计人数**

```python
df.groupBy("age").count().show()
```

输出：

```
+----+-----+
| age|count|
+----+-----+
|  29|    2|
|null|    2|
|  28|    2|
|  36|    1|
+----+-----+
```

**（5）RDD 转 DataFrame 并注册临时视图执行 SQL**

```python
from pyspark.sql.types import Row

peopleRDD = sc.textFile("file:///home/hadoop/employee.txt")
rowRDD = peopleRDD.map(lambda line: line.split(",")) \
                  .map(lambda attributes: Row(int(attributes[0]), attributes[1], int(attributes[2])))
rowRDD.createOrReplaceTempView("employee")

personsDF = spark.sql("select * from employee")
personsDF.rdd.map(lambda t: "id:" + str(t[0]) + ",Name:" + t[1] + ",age:" + str(t[2])).foreach(print)
```

---

## 目录结构

```
spark-bigdata-labs/
├── README.md
├── requirements.txt
├── .gitignore
├── lab1_rdd/
│   ├── interactive_practice.py     # pyspark 交互式练习题
│   ├── remdup.py                   # 数据去重
│   └── average.py                  # 求平均值
├── lab2_spark_sql/
│   ├── dataframe_basic.py          # 读取 JSON、去重、筛选、分组统计
│   └── rdd_to_dataframe.py         # RDD 转 DataFrame + 临时视图 + SQL
└── data/
    ├── Algorithm成绩.txt            # 求平均值实验输入（算法课成绩）
    ├── Database成绩.txt             # 求平均值实验输入（数据库课成绩）
    ├── Python成绩.txt               # 求平均值实验输入（Python 课成绩）
    ├── A.txt                        # 数据去重实验输入
    ├── B.txt                        # 数据去重实验输入
    ├── employee.json               # Spark SQL 实验输入
    └── employee.txt                # RDD 转 DataFrame 实验输入
```

> 实验一的成绩数据 `chapter5-data1.txt` 未包含在本仓库中，需要时请自备同格式数据。

## 说明

- 课程实验项目，按实验指导书给定的实验内容独立完成。
- 报告中的文件路径为实验环境（Ubuntu）下的绝对路径，本地运行需自行调整。
- `data/` 目录中的 `A.txt`、`B.txt`、`employee.txt` 与三个成绩文件为**演示数据**：按实验报告中的输出结果构造，用于本地跑通流程。`employee.json` 取自实验指导书给出的数据集。原始数据请以实验环境为准。
