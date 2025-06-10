from pyspark.sql import SparkSession
import os
import boto3
import pyspark.sql.functions as F
from pyspark.sql.functions import format_number as format
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
        return df.withColumn(coluna, format(F.col(coluna), 2).cast('float'))
    
    def remove_null(self, df):
        return df.dropna()
    
    def remove_wrong_float(self, df, coluna):
        return df.withColumn(coluna, F.col(coluna).cast("float")).filter(F.col(coluna).isNotNull())
    
    def transform_df_to_json(self, df, sensor, prefix):

        dados = df.toPandas().to_dict(orient="records")
        file_name = prefix + "_" + sensor + str(datetime.datetime.now().year) + str(datetime.datetime.now().day) + str(datetime.datetime.now().hour) + str(datetime.datetime.now().minute) \
        + str(datetime.datetime.now().microsecond)+ ".json"

        with open("temp/" + file_name, "w") as f:
            json.dump(dados, f, indent=4)
        
        return f"./temp/{file_name}"
    
    def filter_by_sensor(self, df, coluna, sensor):
        return df.filter(F.col(coluna) == sensor)
    
    def order_by_coluna_desc(self, df, coluna):
        return df.orderBy(F.desc(coluna))
    
    def order_by_coluna_asc(self, df, coluna):
        return df.orderBy(F.asc(coluna))