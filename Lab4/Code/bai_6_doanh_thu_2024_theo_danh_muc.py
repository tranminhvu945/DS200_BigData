import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from pyspark.sql import functions as F
from utils import create_spark, load_tables, parse_args, show_and_save

def tinh_doanh_thu_2024_theo_danh_muc(tables):
    orders = tables["orders"]
    order_items = tables["order_items"]
    products = tables["products"]

    return (
        order_items.join(orders, on="Order_ID", how="inner")
        .join(products, on="Product_ID", how="left")
        .withColumn("Purchase_Time", F.to_timestamp("Order_Purchase_Timestamp", "yyyy-MM-dd HH:mm"))
        .filter(F.year("Purchase_Time") == 2024)
        .withColumn("Doanh_Thu", F.col("Price") + F.col("Freight_Value"))
        .groupBy("Product_Category_Name")
        .agg(
            F.round(F.sum("Doanh_Thu"), 2).alias("Tong_Doanh_Thu_2024"),
            F.countDistinct("Order_ID").alias("So_Luong_Don_Hang"),
            F.sum("Order_Item_ID").alias("Tong_So_Dong_San_Pham")
        )
        .orderBy(F.desc("Tong_Doanh_Thu_2024"))
    )

def main():
    args = parse_args("Bài 6 - Doanh thu năm 2024 theo danh mục sản phẩm")
    spark = create_spark("Bai_6_Doanh_Thu_2024_Theo_Danh_Muc")
    tables = load_tables(spark, args.data_dir)

    result = tinh_doanh_thu_2024_theo_danh_muc(tables)
    show_and_save(result, "Bài 6 - Doanh thu năm 2024 theo danh mục sản phẩm", args.output_dir, "bai_6_doanh_thu_2024_theo_danh_muc", args.show)

    spark.stop()

if __name__ == "__main__":
    main()
