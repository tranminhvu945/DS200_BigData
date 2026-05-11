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

# Bước 1: Tạo map (OccupationID -> OccupationName) từ file occupation.txt
# Sau đó thu thập về driver (collectAsMap) để broadcast vì danh sách này nhỏ
occ_map_dict = occupation_rdd.map(lambda line: line.split(",")) \
                             .map(lambda parts: (parts[0], parts[1])) \
                             .collectAsMap()

# Truyền broadcast variable để tối ưu hiệu suất map
broadcast_occ = sc.broadcast(occ_map_dict)

# Bước 2: Map từ users.txt (UserID -> OccupationName)
# parts[3] là Occupation ID
users_occ = users_rdd.map(lambda line: line.split(",")) \
                     .map(lambda parts: (parts[0], broadcast_occ.value.get(parts[3], "Unknown")))

# Bước 3: Join với ratings_by_user (UserID, (MovieID, Rating))
ratings_by_user = ratings_rdd.map(lambda line: line.split(",")) \
                             .map(lambda parts: (parts[0], (parts[1], float(parts[2]))))

user_occ_rating = users_occ.join(ratings_by_user)

# Bước 4: Map thành cặp (OccupationName, (Rating, 1)) và Reduce
occ_ratings = user_occ_rating.map(lambda x: (x[1][0], (x[1][1][1], 1)))

occ_stats = occ_ratings.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
                       .mapValues(lambda x: (x[0] / x[1], x[1])) \
                       .sortBy(lambda x: x[1][0], ascending=False)

# --- LƯU KẾT QUẢ ---
output_dir = "../Output/Bai5"
os.makedirs(output_dir, exist_ok=True)

output_5_rdd = occ_stats.map(lambda x: f"Nghề nghiệp: {x[0]:15} | Điểm TB: {x[1][0]:.2f} | Tổng lượt: {x[1][1]}")
output_5_rdd.saveAsTextFile(f"{output_dir}/bai5_output")

all_occ_data = output_5_rdd.collect()

with open(f"{output_dir}/occupation_avg.txt", "w", encoding="utf-8") as f:
    for line in all_occ_data:
        f.write(line + "\n")

sc.stop()