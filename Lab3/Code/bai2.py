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

# Bước 1: Tạo map (MovieID -> List of Genres)
def extract_genres(line):
    parts = line.split(",")
    movie_id = parts[0]
    genres = parts[2].split("|") if len(parts) > 2 else []
    return (movie_id, genres)

movies_genres = movies_rdd.map(extract_genres)

# Bước 2: Map từ MovieID -> Rating và Join với thể loại
rating_only = ratings_rdd.map(lambda line: line.split(",")) \
                         .map(lambda parts: (parts[1], float(parts[2])))

# Join: (MovieID, (List of Genres, Rating))
movie_genre_rating = movies_genres.join(rating_only)

# FlatMap để tách từng thể loại ra thành key: (Genre, (Rating, 1))
genre_ratings = movie_genre_rating.flatMap(
    lambda x: [(genre, (x[1][1], 1)) for genre in x[1][0]]
)

# Bước 3: Tính trung bình điểm cho từng thể loại
genre_avg = genre_ratings.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
                         .mapValues(lambda x: x[0] / x[1]) \
                         .sortBy(lambda x: x[1], ascending=False)

# --- LƯU KẾT QUẢ ---
output_dir = "../Output/Bai2"
os.makedirs(output_dir, exist_ok=True)

output_2_rdd = genre_avg.map(lambda x: f"Thể loại: {x[0]:15} | Avg Rating: {x[1]:.2f}")
output_2_rdd.saveAsTextFile(f"{output_dir}/bai2_output")

all_genres_data = output_2_rdd.collect()

with open(f"{output_dir}/genre_avg.txt", "w", encoding="utf-8") as f:
    f.write("Thể loại, Điểm trung bình\n")
    f.write("-" * 30 + "\n")
    
    for line in all_genres_data:
        f.write(line + "\n")
        
sc.stop()

