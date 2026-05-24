import argparse
import os
from typing import Dict

from pyspark.sql import DataFrame, SparkSession

FILE_NAMES = {
    "customers": "Customer_List.csv",
    "orders": "Orders.csv",
    "order_items": "Order_Items.csv",
    "products": "Products.csv",
    "reviews": "Order_Reviews.csv",
}

def parse_args(description: str):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--data-dir", default="../Input", help="Thư mục chứa các file CSV đầu vào")
    parser.add_argument("--output-dir", default="../Output", help="Thư mục lưu kết quả")
    parser.add_argument("--show", type=int, default=20, help="So dong hien thi tren console")
    return parser.parse_args()

def create_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )

def read_csv(spark: SparkSession, data_dir: str, file_name: str) -> DataFrame:
    path = os.path.join(data_dir, file_name)
    return (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(path)
    )

def load_tables(spark: SparkSession, data_dir: str) -> Dict[str, DataFrame]:
    return {name: read_csv(spark, data_dir, file_name) for name, file_name in FILE_NAMES.items()}

def show_and_save(df, title, output_dir, folder_name, n=20):
    print("\n" + "=" * 100)
    print(title)
    print("=" * 100)
    df.show(n, truncate=False)

    output_path = os.path.join(output_dir, folder_name)
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(output_path)
    )
    print(f"Da luu ket qua vao: {output_path}")