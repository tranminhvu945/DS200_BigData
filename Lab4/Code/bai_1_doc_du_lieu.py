import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from pyspark.sql import Row
from utils import create_spark, load_tables, parse_args, show_and_save

def build_schema_dataframe(spark, table_name, df):
    rows = []
    for index, field in enumerate(df.schema.fields, start=1):
        rows.append(
            Row(
                table_name=table_name,
                column_order=index,
                column_name=field.name,
                data_type=field.dataType.simpleString(),
                nullable=field.nullable,
            )
        )
    return spark.createDataFrame(rows)

def main():
    args = parse_args("Bài 1 - Đọc dữ liệu từ CSV và lưu kiểu dữ liệu của mỗi bảng")
    spark = create_spark("Bai_1_Doc_Du_Lieu")

    tables = load_tables(spark, args.data_dir)

    schema_dfs = []
    for table_name, df in tables.items():
        print("\n" + "=" * 100)
        print(f"BẢNG: {table_name}")
        print("=" * 100)
        df.printSchema()
        df.show(args.show, truncate=False)
        print(f"Số dòng: {df.count()}")
        print(f"Số cột: {len(df.columns)}")

        schema_df = build_schema_dataframe(spark, table_name, df)
        schema_dfs.append(schema_df)

        # Lưu riêng kiểu dữ liệu của từng bảng
        show_and_save(
            schema_df,
            title=f"Bài 1 - Kiểu dữ liệu của bảng {table_name}",
            output_dir=args.output_dir,
            folder_name=f"bai_1_schema_{table_name}",
            n=args.show,
        )
    spark.stop()

if __name__ == "__main__":
    main()
