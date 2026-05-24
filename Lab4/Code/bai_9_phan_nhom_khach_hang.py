import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from pyspark.sql import functions as F
from utils import create_spark, load_tables, parse_args, show_and_save

def phan_nhom_khach_hang(tables):
    customers = tables["customers"]
    orders = tables["orders"]
    order_items = tables["order_items"]

    order_value = (
        order_items.withColumn("Gia_Tri_Dong_Hang", F.col("Price") + F.col("Freight_Value"))
        .groupBy("Order_ID")
        .agg(F.round(F.sum("Gia_Tri_Dong_Hang"), 2).alias("Gia_Tri_Don_Hang"))
    )

    customer_stats = (
        orders.join(order_value, on="Order_ID", how="left")
        .join(customers, on="Customer_Trx_ID", how="left")
        .groupBy("Customer_Trx_ID", "Customer_Country", "Customer_City")
        .agg(
            F.countDistinct("Order_ID").alias("So_Luong_Don_Hang"),
            F.round(F.avg("Gia_Tri_Don_Hang"), 2).alias("Gia_Tri_Trung_Binh_Don_Hang"),
            F.round(F.sum("Gia_Tri_Don_Hang"), 2).alias("Tong_Gia_Tri_Mua_Hang"),
            F.min(F.to_timestamp("Order_Purchase_Timestamp", "yyyy-MM-dd HH:mm")).alias("Lan_Mua_Dau"),
            F.max(F.to_timestamp("Order_Purchase_Timestamp", "yyyy-MM-dd HH:mm")).alias("Lan_Mua_Cuoi")
        )
        .withColumn("So_Ngay_Giua_Lan_Dau_Va_Cuoi", F.datediff("Lan_Mua_Cuoi", "Lan_Mua_Dau"))
        .withColumn(
            "Tan_Suat_Mua_Sam",
            F.when(F.col("So_Ngay_Giua_Lan_Dau_Va_Cuoi") > 0,
                   F.round(F.col("So_Luong_Don_Hang") / F.col("So_Ngay_Giua_Lan_Dau_Va_Cuoi"), 4))
            .otherwise(F.col("So_Luong_Don_Hang"))
        )
    )

    return (
        customer_stats.withColumn(
            "Nhom_Khach_Hang",
            F.when((F.col("So_Luong_Don_Hang") >= 3) & (F.col("Gia_Tri_Trung_Binh_Don_Hang") >= 200), "VIP")
            .when(F.col("So_Luong_Don_Hang") >= 2, "Mua_lap_lai")
            .when(F.col("Gia_Tri_Trung_Binh_Don_Hang") >= 200, "Gia_tri_cao")
            .otherwise("Pho_thong")
        )
        .orderBy(F.desc("Tong_Gia_Tri_Mua_Hang"), F.desc("So_Luong_Don_Hang"))
    )

def main():
    args = parse_args("Bài 9 - Phân nhóm khách hàng")
    spark = create_spark("Bai_9_Phan_Nhom_Khach_Hang")
    tables = load_tables(spark, args.data_dir)

    result = phan_nhom_khach_hang(tables)
    show_and_save(result, "Bài 9 - Phân nhóm khách hàng", args.output_dir, "bai_9_phan_nhom_khach_hang", args.show)

    group_summary = (
        result.groupBy("Nhom_Khach_Hang")
        .agg(
            F.count("Customer_Trx_ID").alias("So_Luong_Khach_Hang"),
            F.round(F.avg("So_Luong_Don_Hang"), 2).alias("TB_So_Don"),
            F.round(F.avg("Gia_Tri_Trung_Binh_Don_Hang"), 2).alias("TB_Gia_Tri_Don")
        )
        .orderBy(F.desc("So_Luong_Khach_Hang"))
    )
    show_and_save(group_summary, "Bài 9 - Tổng hợp theo nhóm khách hàng", args.output_dir, "bai_9_tong_hop_nhom_khach_hang", args.show)

    spark.stop()

if __name__ == "__main__":
    main()
