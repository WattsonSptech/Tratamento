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
            .config("spark.memory.offHeap.enabled", "true") \
            .config("spark.memory.offHeap.size", "10g") \
            .config("spark.hadoop.hadoop.security.authentication", "simple") \
            .getOrCreate()
    
        return spark

    def get_data_s3_csv(self):
        try:
            bucket_name = os.getenv("BUCKET_NAME_RAW")
            s3 = boto3.client('s3')
            print("Dados coletados")

            response = s3.list_objects_v2(Bucket=bucket_name)
            ultimo_arquivo = ""
            if 'Contents' in response:
                arquivos = sorted(response['Contents'], key=lambda obj: obj['LastModified'], reverse=True)

                ultimo_arquivo = arquivos[0]['Key']
                print("Último arquivo:", ultimo_arquivo)
                path = "temp/" + ultimo_arquivo
                s3.download_file(bucket_name, ultimo_arquivo, path)
                return ultimo_arquivo
            else:
                raise Exception("Nenhum arquivo encontrado no bucket.")
                
        except Exception as e:
            print(f"""Erro ao coletar dados da AWS: 
                  BUCKET_NAME: {bucket_name}
                  PATH: {path}
                  error: {e}
                  """)

    def set_data_s3_file(self, object_name):
        try:
            bucket_name = os.getenv("BUCKET_NAME_TRUSTED")
            s3 = boto3.client('s3')
            print("Dados coletados")

            response = s3.upload_file(object_name, bucket_name, object_name)
            print(response) 
        except Exception as e:
            print(f"""Erro ao inserir dados na AWS: 
                  BUCKET_NAME: {bucket_name}
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
        return df.withColumn(coluna, format(F.col(coluna), 2).cast('float'))
    
    def remove_null(self, df):
        return df.dropna()
    
    def remove_wrong_float(self, df, coluna):
        return df.withColumn(coluna, F.col(coluna).cast("float")).filter(F.col(coluna).isNotNull())
    
    def tranform_df_to_json(self, df, sensor):

        dados = df.toPandas().to_dict(orient="records")
        file_name = "trusted_" + sensor + str(datetime.datetime.now().year) + str(datetime.datetime.now().day) + str(datetime.datetime.now().hour) + str(datetime.datetime.now().minute) \
        + str(datetime.datetime.now().microsecond)+ ".json"

        with open(file_name, "w") as f:
            json.dump(dados, f, indent=4)
        
        return file_name