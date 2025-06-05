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

    def get_data_s3_csv(self, bucket_name, sensor=None):
        try:
            print(os.getenv("BUCKET_NAME_RAW"))
            s3 = boto3.client('s3')
            response = s3.list_objects_v2(Bucket=bucket_name)
            ultimo_arquivo = ""

            if 'Contents' in response:
                arquivos = sorted(response['Contents'], key=lambda obj: obj['LastModified'], reverse=True)

                if sensor != None:

                    arquivo_existe = False

                    for i in range(len(arquivos)):
                        if (sensor in arquivos[i]['Key']):
                            arquivo_existe = True
                            ultimo_arquivo = arquivos[i]['Key']
                            break
                    
                    if not arquivo_existe:
                        raise Exception(f"Nenhum arquivo do sensor {sensor} encontrado no bucket.")
                else:
                    ultimo_arquivo = arquivos[0]['Key']

                print("Último arquivo:", ultimo_arquivo)

                path = "temp/" + ultimo_arquivo
                s3.download_file(bucket_name, ultimo_arquivo, path)

                return path
            
            else:
                raise Exception("Nenhum arquivo encontrado no bucket.")       
        except Exception as e:
            print(f"""Erro ao coletar dados da AWS: 
                  BUCKET_NAME: {bucket_name}
                  error: {e}
                  """)

    def set_data_s3_file(self, object_name, bucket_name):
        try:
            s3 = boto3.client('s3')
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
        
        return file_name
    
    def filter_by_sensor(self, df, coluna, sensor):
        return df.filter(F.col(coluna) == sensor)
    
    def order_by_coluna_desc(self, df, coluna):
        return df.orderBy(F.desc(coluna))
    
    def order_by_coluna_asc(self, df, coluna):
        return df.orderBy(F.asc(coluna))