from pyspark.sql import SparkSession
import os
import boto3
import pyspark.sql.functions as F
from pyspark.sql.functions import format_number as format



class Utils:
    def __init__(self) -> None:
        self.project_name = "Wattson"
    
    def init_spark(self):      
        spark = SparkSession.builder \
            .appName(self.project_name) \
            .master("local[*]") \
            .config("spark.memory.offHeap.enabled", "true") \
            .config("spark.memory.offHeap.size", "10g") \
            .config("spark.hadoop.hadoop.security.authentication", "simple") \
            .getOrCreate()
    
        return spark

    def get_data_s3_csv(self, nome_arquivo):
        try:
            bucket_name = os.getenv("BUCKET_NAME_RAW")
            s3 = boto3.client('s3')
            path = "/temp/" + nome_arquivo
            s3.download_file(bucket_name, nome_arquivo, path)
            print("Dados coletados")
        except Exception as e:
            print(f"""Erro ao coletar dados da AWS: 
                  BUCKET_NAME: {bucket_name}
                  NOME_ARQUIVO: {nome_arquivo}
                  PATH: {path}
                  error: {e}
                  """)

    def highlight_outlier(self, df, coluna):
        qtr_map = df.select( \
                    F.expr(f"percentile_approx({coluna}, 0.25) as Q1"), \
                    F.expr(f"percentile_approx({coluna}, 0.75) as Q3") \
             ) \
             .collect()[0] \
             .asDict()

        df = df.withColumn(f"floor_{coluna}_imputed", F.floor(F.col("frequency")))
        df = df.withColumn(f"outlier_{coluna}_imputed", F.floor(F.col("frequency")))

        df = df.filter( \
                    (F.col(f"floor_{coluna}_imputed") >= qtr_map["Q1"]) \
                    & (F.col(f"floor_{coluna}_imputed") <= qtr_map["Q3"]) \
                )
        
        return df
        
    def format_number(self, df, coluna):
        return df.select("id", format(coluna, 2).alias("formatted_value"))