import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from pyspark.sql import functions as F
from utils import create_spark, load_tables, parse_args, show_and_save

def hieu_suat_giao_hang(tables):
    orders = tables["orders"]
    order_items = tables["order_items"]

    detail = (
        orders.join(order_items, on="Order_ID", how="inner")
        .withColumn("Ngay_Giao_Thuc_Te", F.to_timestamp("Order_Delivered_Carrier_Date", "yyyy-MM-dd HH:mm"))
        .withColumn("Ngay_Giao_Du_Kien", F.to_timestamp("Shipping_Limit_Date", "yyyy-MM-dd HH:mm"))
        .filter(F.col("Ngay_Giao_Thuc_Te").isNotNull() & F.col("Ngay_Giao_Du_Kien").isNotNull())
        .withColumn(
            "So_Ngay_Chenh_Lech",
            F.round((F.unix_timestamp("Ngay_Giao_Thuc_Te") - F.unix_timestamp("Ngay_Giao_Du_Kien")) / 86400, 2)
        )
        .withColumn(
            "Trang_Thai_Giao_Hang",
            F.when(F.col("So_Ngay_Chenh_Lech") <= 0, "Dung_hoac_som_han").otherwise("Tre_han")
        )
    )

    summary = (
        detail.groupBy("Trang_Thai_Giao_Hang")
        .agg(
            F.count("Order_ID").alias("So_Dong_Don_Hang"),
            F.round(F.avg("So_Ngay_Chenh_Lech"), 2).alias("Chenhlech_Trung_Binh_Ngay"),
            F.round(F.min("So_Ngay_Chenh_Lech"), 2).alias("Som_Nhat_Ngay"),
            F.round(F.max("So_Ngay_Chenh_Lech"), 2).alias("Tre_Nhat_Ngay")
        )
        .orderBy("Trang_Thai_Giao_Hang")
    )

    return detail, summary

def main():
    args = parse_args("Bài 8 - Đánh giá hiệu suất giao hàng")
    spark = create_spark("Bai_8_Hieu_Suat_Giao_Hang")
    tables = load_tables(spark, args.data_dir)

    detail, summary = hieu_suat_giao_hang(tables)
    show_and_save(detail.select("Order_ID", "Order_Item_ID", "Seller_ID", "So_Ngay_Chenh_Lech", "Trang_Thai_Giao_Hang"),
                  "Bài 8a - Chi tiết chênh lệch ngày giao hàng", args.output_dir, "bai_8a_chi_tiet_hieu_suat_giao_hang", args.show)
    show_and_save(summary, "Bài 8b - Tổng hợp hiệu suất giao hàng", args.output_dir, "bai_8b_tong_hop_hieu_suat_giao_hang", args.show)

    spark.stop()

if __name__ == "__main__":
    main()
