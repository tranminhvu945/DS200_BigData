import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Bai2").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

movies_rdd = sc.textFile("../Input/movies.txt")
ratings_1_rdd = sc.textFile("../Input/ratings_1.txt")
ratings_2_rdd = sc.textFile("../Input/ratings_2.txt")
users_rdd = sc.textFile("../Input/users.txt")
occupation_rdd = sc.textFile("../Input/occupation.txt")

ratings_rdd = ratings_1_rdd.union(ratings_2_rdd)

# Bước 1: Tạo map (MovieID -> Title)
def parse_movie(line):
    parts = line.split(",")
    return (parts[0], parts[1])  # (MovieID, Title)

movies_map = movies_rdd.map(parse_movie)

# Bước 2: Tạo map MovieID -> (Rating, 1) từ ratings_rdd
def map_ratings_to_pair(line: str):
    parts = line.split(",")
    return (parts[1], (float(parts[2]), 1))

ratings_mapped = ratings_rdd.map(map_ratings_to_pair)

# Bước 3: ReduceByKey để tính tổng điểm và tổng số lượt đánh giá
# Kết quả: (MovieID, (SumRating, Count))
ratings_reduced = ratings_mapped.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Bước 4: Tính điểm trung bình cho toàn bộ phim
# Kết quả: (MovieID, (AvgRating, Count))
ratings_avg_all = ratings_reduced.mapValues(lambda x: (x[0] / x[1], x[1]))

# Bước 5: Tìm phim có điểm trung bình cao nhất 
ratings_avg_5 = ratings_avg_all.filter(lambda x: x[1][1] >= 5)
movie_stats = movies_map.join(ratings_avg_5) \
                        .map(lambda x: (x[1][0], x[1][1][0], x[1][1][1])) # (Title, AvgRating, Count)

top_movie = movie_stats.sortBy(lambda x: x[1], ascending=False).first()

# --- LƯU KẾT QUẢ ---
output_dir = "../Output/Bai1"
os.makedirs(output_dir, exist_ok=True)

output_1 = movies_map.join(ratings_avg_all) \
                        .map(lambda x: f"MovieID: {x[0]}, Title: {x[1][0]}, Avg: {x[1][1][0]:.2f}, Count: {x[1][1][1]}")

output_1.saveAsTextFile(f"{output_dir}/bai1_output")

all_movies_data = output_1.collect()
with open(f"{output_dir}/all_movies.txt", "w", encoding="utf-8") as f:
    for line in all_movies_data:
        f.write(line + "\n")

top_movie_text = f"Phim có điểm TB cao nhất (>= 5 đánh giá): {top_movie[0]}, Điểm TB: {top_movie[1]:.2f}, Số lượt: {top_movie[2]}"

with open(f"{output_dir}/top_1_movie.txt", "w", encoding="utf-8") as file:
    file.write(top_movie_text)

sc.stop()