from pyspark.sql import SparkSession
import os
import boto3
import pyspark.sql.functions as F
from pyspark.sql.functions import format_number as format, regexp_replace, last_value
import json
import datetime



class Utils:
    def __init__(self) -> None:
        self.project_name = "Wattson"
    
    def init_spark(self):      
        spark = SparkSession.builder \
            .appName(self.project_name) \
            .master("local[*]") \
            .config("spark.driver.host", "localhost") \
            .config("spark.memoffHeap.enabled", "true") \
            .config("spark.memory.offHeary.op.size", "10g") \
            .config("spark.hadoop.io.native.lib.available", "false") \
            .config("mapreduce.fileoutputcommitter.algorithm.version", "1") \
            .config("spark.hadoop.hadoop.security.authentication", "simple") \
            .getOrCreate()
    
        return spark

    def get_data_s3_csv(self, bucket_name):
        print(f"\t\tLendo do bucket {bucket_name} do S3...")

        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=bucket_name)

        if 'Contents' in response:
            ultimo_arquivo = sorted(response['Contents'], key=lambda obj: obj['LastModified'], reverse=True)[0]['Key']

            print("\t\tArquivo mais recente encontrado:", ultimo_arquivo)

            path = "./temp/" + ultimo_arquivo.replace(":", "_")
            s3.download_file(bucket_name, ultimo_arquivo, path)

            print("\t\tSucesso!\n")
            return path

        else:
            raise FileNotFoundError("Nenhum arquivo encontrado no bucket.")

    def set_data_s3_file(self, filepath, bucket_name):
        print(f"\t\tInserindo no bucket {bucket_name} do S3...")
        filename = filepath.split("/")[-1]

        s3 = boto3.client('s3')
        res = s3.upload_file(filepath, bucket_name, filename)

        print("\t\tSucesso!\n")

    def highlight_outlier(self, df, coluna):
        qtr_map = df.select( \
                    F.expr(f"percentile_approx({coluna}, 0.25) as Q1"), \
                    F.expr(f"percentile_approx({coluna}, 0.75) as Q3") \
             ) \
             .collect()[0] \
             .asDict()

        df = df.withColumn(f"floor_{coluna}_imputed", F.floor(F.col(coluna)))
        df = df.withColumn(f"outlier_{coluna}_imputed", F.floor(F.col(coluna)))

        df = df.filter( \
                    (F.col(f"floor_{coluna}_imputed") >= qtr_map["Q1"]) \
                    & (F.col(f"floor_{coluna}_imputed") <= qtr_map["Q3"]) \
                )
        
        return df
        
    def format_number(self, df, coluna):
        return df.withColumn(coluna, regexp_replace(format(F.col(coluna), 2), ",", "").cast('float'))

    def format_number_to_float(self, df, coluna):
        return df.withColumn(coluna, F.round(F.col(coluna), 1).cast('float'))

    def drop_column(self, df, coluna):
        return df.drop(coluna)

    def add_column(self, df, coluna, value):
        return df.withColumn(coluna, F.lit(value))

    def enumerate_column(self, df, coluna):
        id_column = df.withColumn(coluna, F.monotonically_increasing_id())
        id_column = id_column.withColumn(coluna, F.col(coluna) + 1)
        formatted_df = id_column.select(coluna,*df.columns)
        return formatted_df

    def rename_values(self, df, coluna, old_value, new_value):
        return df.withColumn(coluna, F.when(F.col(coluna) == old_value, new_value).otherwise(F.col(coluna)))

    def rename_column(self, df, coluna, new_coluna):
        return df.withColumnRenamed(coluna, new_coluna)
    
    def remove_null(self, df):
        return df.dropna()
    
    def remove_wrong_float(self, df, coluna):
        return df.withColumn(coluna, F.col(coluna).cast("float")).filter(F.col(coluna).isNotNull())
    
    def transform_df_to_json(self, df, sensor, prefix):
        # print("before transform: ")
        # df.show()
        dados = df.toPandas().to_dict(orient="records")


        file_name = "temp/" + prefix + "_" + sensor + str(datetime.datetime.now().year) + str(datetime.datetime.now().day) + str(datetime.datetime.now().hour) + str(datetime.datetime.now().minute) \
        + str(datetime.datetime.now().microsecond)+ ".json"

        with open(file_name, "w") as f:
            json.dump(dados, f, indent=4)
        
        return f"./temp/{file_name}"
    
    def transform_df_to_csv(self, df, sensor, prefix):
        print("before transform: ")
        df.show()
        file_name = f"temp/{prefix}_{sensor}{datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')}.csv"

        df.coalesce(1).write.mode("overwrite").option("header", True).csv(file_name) 

        print(file_name)

        return file_name

    def filter_by_sensor(self, df, coluna, sensor):
        return df.filter(F.col(coluna) == sensor)
    
    def order_by_coluna_desc(self, df, coluna):
        return df.orderBy(F.desc(coluna))
    
    def order_by_coluna_asc(self, df, coluna):
        return df.orderBy(F.asc(coluna))

    def get_last_value(self, df, coluna):
        return df.select(last_value(coluna))