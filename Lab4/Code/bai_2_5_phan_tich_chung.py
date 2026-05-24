import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from pyspark.sql import functions as F
from utils import create_spark, load_tables, parse_args, show_and_save

def bai_2_tong_so_luong(tables):
    orders = tables["orders"]
    customers = tables["customers"]
    order_items = tables["order_items"]

    return orders.sparkSession.createDataFrame(
        [
            ("Tong_so_don_hang", orders.select("Order_ID").distinct().count()),
            ("Tong_so_khach_hang", customers.select("Customer_Trx_ID").distinct().count()),
            ("Tong_so_nguoi_ban", order_items.select("Seller_ID").distinct().count()),
        ],
        ["Chi_Tieu", "Gia_Tri"]
    )

def bai_3_don_hang_theo_quoc_gia(tables):
    orders = tables["orders"]
    customers = tables["customers"]

    return (
        orders.join(customers, on="Customer_Trx_ID", how="inner")
        .groupBy("Customer_Country", "Customer_Country_Code")
        .agg(F.countDistinct("Order_ID").alias("So_Luong_Don_Hang"))
        .orderBy(F.desc("So_Luong_Don_Hang"), "Customer_Country")
    )

def bai_4_don_hang_theo_nam_thang(tables):
    orders = tables["orders"]

    return (
        orders.withColumn("Purchase_Time", F.to_timestamp("Order_Purchase_Timestamp", "yyyy-MM-dd HH:mm"))
        .withColumn("Nam", F.year("Purchase_Time"))
        .withColumn("Thang", F.month("Purchase_Time"))
        .groupBy("Nam", "Thang")
        .agg(F.countDistinct("Order_ID").alias("So_Luong_Don_Hang"))
        .orderBy(F.asc("Nam"), F.desc("Thang"))
    )

def bai_5_thong_ke_review(tables):
    reviews = tables["reviews"]

    clean_reviews = (
        reviews.withColumn("Review_Score_Clean", F.col("Review_Score").cast("int"))
        .filter(F.col("Review_Score_Clean").isNotNull())
        .filter((F.col("Review_Score_Clean") >= 1) & (F.col("Review_Score_Clean") <= 5))
    )

    avg_review = clean_reviews.agg(
        F.round(F.avg("Review_Score_Clean"), 2).alias("Diem_Danh_Gia_Trung_Binh"),
        F.count("Review_Score_Clean").alias("So_Luong_Danh_Gia_Hop_Le")
    )

    count_by_score = (
        clean_reviews.groupBy("Review_Score_Clean")
        .agg(F.count("Review_ID").alias("So_Luong_Danh_Gia"))
        .orderBy("Review_Score_Clean")
    )

    return avg_review, count_by_score

def main():
    args = parse_args("Bài 2-5 - Các phân tích thống kê chung")
    spark = create_spark("Bai_2_5_Phan_Tich_Chung")
    tables = load_tables(spark, args.data_dir)

    result_2 = bai_2_tong_so_luong(tables)
    show_and_save(result_2, "Bài 2 - Tổng số đơn hàng, khách hàng và người bán", args.output_dir, "bai_2_tong_so_luong", args.show)

    result_3 = bai_3_don_hang_theo_quoc_gia(tables)
    show_and_save(result_3, "Bài 3 - Số lượng đơn hàng theo quốc gia", args.output_dir, "bai_3_don_hang_theo_quoc_gia", args.show)

    result_4 = bai_4_don_hang_theo_nam_thang(tables)
    show_and_save(result_4, "Bài 4 - Số lượng đơn hàng theo năm, tháng", args.output_dir, "bai_4_don_hang_theo_nam_thang", args.show)

    avg_review, count_by_score = bai_5_thong_ke_review(tables)
    show_and_save(avg_review, "Bài 5a - Điểm đánh giá trung bình", args.output_dir, "bai_5a_diem_danh_gia_trung_binh", args.show)
    show_and_save(count_by_score, "Bài 5b - Số lượng đánh giá theo từng mức điểm", args.output_dir, "bai_5b_so_luong_danh_gia_theo_muc", args.show)

    spark.stop()

if __name__ == "__main__":
    main()
