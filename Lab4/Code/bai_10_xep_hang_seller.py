import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from pyspark.sql import Row, functions as F
from utils import create_spark, load_tables, parse_args, show_and_save

def xep_hang_seller(tables):
    order_items = tables["order_items"]

    seller_stats = (
        order_items.withColumn("Doanh_Thu", F.col("Price") + F.col("Freight_Value"))
        .groupBy("Seller_ID")
        .agg(
            F.round(F.sum("Doanh_Thu"), 2).alias("Tong_Doanh_Thu"),
            F.countDistinct("Order_ID").alias("So_Luong_Don_Hang"),
            F.count("Order_Item_ID").alias("So_Luong_San_Pham_Ban"),
            F.round(F.avg("Doanh_Thu"), 2).alias("Doanh_Thu_Trung_Binh_Moi_Dong")
        )
    )

    df_sorted = seller_stats.orderBy(
        F.col("Tong_Doanh_Thu").desc(),
        F.col("So_Luong_Don_Hang").desc(),
        F.col("So_Luong_San_Pham_Ban").desc(),
        F.col("Seller_ID").asc(),
    )

    ranked_rdd = df_sorted.rdd.zipWithIndex().map(
        lambda row_idx: Row(
            Hang_Seller=int(row_idx[1]) + 1,
            Seller_ID=row_idx[0]["Seller_ID"],
            Tong_Doanh_Thu=row_idx[0]["Tong_Doanh_Thu"],
            So_Luong_Don_Hang=row_idx[0]["So_Luong_Don_Hang"],
            So_Luong_San_Pham_Ban=row_idx[0]["So_Luong_San_Pham_Ban"],
            Doanh_Thu_Trung_Binh_Moi_Dong=row_idx[0]["Doanh_Thu_Trung_Binh_Moi_Dong"],
        )
    )

    return seller_stats.sql_ctx.sparkSession.createDataFrame(ranked_rdd)

def main():
    args = parse_args("Bài 10 - Xếp hạng seller")
    spark = create_spark("Bai_10_Xep_Hang_Seller")
    tables = load_tables(spark, args.data_dir)

    result = xep_hang_seller(tables)
    show_and_save(result, "Bài 10 - Xếp hạng seller theo doanh thu và số lượng đơn hàng", args.output_dir, "bai_10_xep_hang_seller", args.show)

    spark.stop()

if __name__ == "__main__":
    main()
