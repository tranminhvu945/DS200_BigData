import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from pyspark import SparkConf, SparkContext
import datetime

conf = SparkConf().setAppName("Bai1").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

movies_rdd = sc.textFile("../Input/movies.txt")
ratings_1_rdd = sc.textFile("../Input/ratings_1.txt")
ratings_2_rdd = sc.textFile("../Input/ratings_2.txt")
users_rdd = sc.textFile("../Input/users.txt")
occupation_rdd = sc.textFile("../Input/occupation.txt")

ratings_rdd = ratings_1_rdd.union(ratings_2_rdd)

# Bước 1: Hàm chuyển Timestamp thành Năm
def get_year_from_timestamp(ts_str):
    # Timestamp trong dữ liệu chuẩn Unix (giây)
    return datetime.datetime.fromtimestamp(int(ts_str)).year

# Bước 2: Đọc dữ liệu ratings và map -> (Year, (Rating, 1))
# parts[2] là Rating, parts[3] là Timestamp
year_ratings = ratings_rdd.map(lambda line: line.split(",")) \
                          .map(lambda parts: (get_year_from_timestamp(parts[3]), (float(parts[2]), 1)))

# Bước 3: ReduceByKey để tính tổng và trung bình
year_stats = year_ratings.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
                         .mapValues(lambda x: (x[0] / x[1], x[1])) \
                         .sortByKey() # Sắp xếp theo năm

# --- LƯU KẾT QUẢ ---
output_dir = "../Output/Bai6"
os.makedirs(output_dir, exist_ok=True)

output_6_rdd = year_stats.map(lambda x: f"Năm: {x[0]} | Điểm TB: {x[1][0]:.2f} | Tổng số lượt: {x[1][1]}")
output_6_rdd.saveAsTextFile(f"{output_dir}/bai6_output")

all_year_data = output_6_rdd.collect()

with open(f"{output_dir}/year_avg.txt", "w", encoding="utf-8") as f:
    for line in all_year_data:
        f.write(line + "\n")

sc.stop()