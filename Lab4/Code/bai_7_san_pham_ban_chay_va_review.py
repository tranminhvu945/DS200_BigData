import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from pyspark.sql import functions as F
from utils import create_spark, load_tables, parse_args, show_and_save

def san_pham_ban_chay_va_review(tables):
    order_items = tables["order_items"]
    products = tables["products"]
    reviews = tables["reviews"]

    clean_reviews = (
        reviews.withColumn("Review_Score_Clean", F.col("Review_Score").cast("int"))
        .filter(F.col("Review_Score_Clean").between(1, 5))
    )

    sold_qty = (
        order_items.groupBy("Product_ID")
        .agg(
            F.count("Order_Item_ID").alias("So_Luong_Ban_Ra"),
            F.countDistinct("Order_ID").alias("So_Don_Hang_Co_San_Pham"),
            F.round(F.sum(F.col("Price") + F.col("Freight_Value")), 2).alias("Tong_Doanh_Thu")
        )
    )

    review_by_product = (
        order_items.select("Order_ID", "Product_ID").distinct()
        .join(clean_reviews.select("Order_ID", "Review_Score_Clean"), on="Order_ID", how="inner")
        .groupBy("Product_ID")
        .agg(
            F.round(F.avg("Review_Score_Clean"), 2).alias("Diem_Danh_Gia_Trung_Binh"),
            F.count("Review_Score_Clean").alias("So_Luong_Danh_Gia")
        )
    )

    return (
        sold_qty.join(review_by_product, on="Product_ID", how="left")
        .join(products.select("Product_ID", "Product_Category_Name"), on="Product_ID", how="left")
        .orderBy(F.desc("So_Luong_Ban_Ra"), F.desc("Diem_Danh_Gia_Trung_Binh"))
    )

def main():
    args = parse_args("Bài 7 - Sản phẩm bán chạy và điểm đánh giá trung bình")
    spark = create_spark("Bai_7_San_Pham_Ban_Chay_Va_Review")
    tables = load_tables(spark, args.data_dir)

    result = san_pham_ban_chay_va_review(tables)
    show_and_save(result, "Bài 7 - Sản phẩm bán ra cao nhất và điểm đánh giá trung bình", args.output_dir, "bai_7_san_pham_ban_chay_va_review", args.show)

    spark.stop()

if __name__ == "__main__":
    main()
