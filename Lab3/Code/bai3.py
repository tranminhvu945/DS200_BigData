import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Bai1").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

movies_rdd = sc.textFile("../Input/movies.txt")
ratings_1_rdd = sc.textFile("../Input/ratings_1.txt")
ratings_2_rdd = sc.textFile("../Input/ratings_2.txt")
users_rdd = sc.textFile("../Input/users.txt")
occupation_rdd = sc.textFile("../Input/occupation.txt")

ratings_rdd = ratings_1_rdd.union(ratings_2_rdd)

def parse_movie(line):
    parts = line.split(",")
    return (parts[0], parts[1])  # (MovieID, Title)

movies_map = movies_rdd.map(parse_movie)

# Bước 1: Tạo map (UserID -> Gender)
# users.txt format: UserID, Gender, Age, Occupation, Zip-code
users_gender = users_rdd.map(lambda line: line.split(",")) \
                        .map(lambda parts: (parts[0], parts[1]))

# Bước 2: Map ratings_rdd thành (UserID, (MovieID, Rating))
ratings_by_user = ratings_rdd.map(lambda line: line.split(",")) \
                             .map(lambda parts: (parts[0], (parts[1], float(parts[2]))))

# Join với users_gender để thêm thông tin giới tính
# Kết quả: (UserID, (Gender, (MovieID, Rating)))
user_gender_rating = users_gender.join(ratings_by_user)

# Bước 3: Đổi key thành (MovieID, Gender) và value thành (Rating, 1)
movie_gender_pairs = user_gender_rating.map(
    lambda x: ((x[1][1][0], x[1][0]), (x[1][1][1], 1))
)

# ReduceByKey và tính trung bình
movie_gender_avg = movie_gender_pairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
                                     .mapValues(lambda x: x[0] / x[1])

# (Tùy chọn) Join với tên phim cho dễ nhìn
movie_gender_final = movie_gender_avg.map(lambda x: (x[0][0], (x[0][1], x[1]))) \
                                     .join(movies_map) \
                                     .map(lambda x: (x[1][1], x[1][0][0], x[1][0][1])) # (Title, Gender, AvgRating)

# --- LƯU KẾT QUẢ ---
output_dir = "../Output/Bai3"
os.makedirs(output_dir, exist_ok=True)

output_3_rdd = movie_gender_final.map(lambda x: f"Phim: {x[0]} | Giới tính: {x[1]} | Điểm TB: {x[2]:.2f}")
output_3_rdd.saveAsTextFile(f"{output_dir}/bai3_output")

all_movie_gender_data = output_3_rdd.collect()

with open(f"{output_dir}/movie_gender_avg.txt", "w", encoding="utf-8") as f:
    f.write("Điểm trung bình phim theo giới tính\n")
    f.write("-" * 50 + "\n")
    for line in all_movie_gender_data:
        f.write(line + "\n")

sc.stop()