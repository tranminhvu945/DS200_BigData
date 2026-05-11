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

# Bước 1: Hàm phân loại nhóm tuổi và tạo map (UserID -> Age Group)
def get_age_group(age_str):
    age = int(age_str)
    if age < 18: return "Under 18"
    elif 18 <= age <= 35: return "18-35"
    elif 36 <= age <= 50: return "36-50"
    else: return "50+"

users_age = users_rdd.map(lambda line: line.split(",")) \
                     .map(lambda parts: (parts[0], get_age_group(parts[2])))

# Bước 2: Join với ratings_by_user
# Kết quả: (UserID, (AgeGroup, (MovieID, Rating)))
ratings_by_user = ratings_rdd.map(lambda line: line.split(",")) \
                             .map(lambda parts: (parts[0], (parts[1], float(parts[2]))))

user_age_rating = users_age.join(ratings_by_user)

# Bước 3: Đổi key thành (MovieID, AgeGroup) -> (Rating, 1) và tính trung bình
movie_age_pairs = user_age_rating.map(
    lambda x: ((x[1][1][0], x[1][0]), (x[1][1][1], 1))
)

movie_age_avg = movie_age_pairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
                               .mapValues(lambda x: x[0] / x[1])

# Đưa tên phim vào để dễ đọc
movie_age_final = movie_age_avg.map(lambda x: (x[0][0], (x[0][1], x[1]))) \
                               .join(movies_map) \
                               .map(lambda x: (x[1][1], x[1][0][0], x[1][0][1])) # (Title, AgeGroup, AvgRating)

# --- LƯU KẾT QUẢ ---
output_dir = "../Output/Bai4"
os.makedirs(output_dir, exist_ok=True)

output_4_rdd = movie_age_final.map(lambda x: f"Phim: {x[0]} | Nhóm tuổi: {x[1]:8} | Điểm TB: {x[2]:.2f}")
output_4_rdd.saveAsTextFile(f"{output_dir}/bai4_output")

all_movie_age_data = output_4_rdd.collect()

with open(f"{output_dir}/movie_age_avg.txt", "w", encoding="utf-8") as f:
    f.write("-" * 60 + "\n")
    for line in all_movie_age_data:
        f.write(line + "\n")

sc.stop()