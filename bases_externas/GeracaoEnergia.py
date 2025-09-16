from interfaces.ITratamento import ITratamentoDados
import pyspark.sql.functions as F
from pyspark.sql.functions import format_number as format
from interfaces.EnumBuckets import EnumBuckets
from utils.DownloadDados import DownloadDados
import os
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from datetime import datetime, timedelta
from pyspark.sql.functions import to_timestamp, col


class GeracaoEnergia(ITratamentoDados):

    def __init__(self):
        super().__init__()
        self.download_dados = DownloadDados()
        self.nome_sensor = "GeracaoEnergia"
        self.tipo_dado = "generation"

    def __tratar_dado__(self) -> None:
        end_date = datetime.today().date()
        start_date = end_date - timedelta(days=1)
        data = self.download_dados.consultarPorQueryBase('teste')

        print(data)
        hourly = data.get("hourly", {})

        print(type(data))
                
        schema = StructType([
            StructField("time", StringType(), True),
            StructField("temperature_2m", DoubleType(), True)
        ])

        df = self.spark.createDataFrame(data)
        df.printSchema()

        df = self.utils.set_null_zero(df=df)
        df = self.utils.uppercase_strings(df=df)

        df.withColumn("numero_consumidores", col("numero_consumidores").cast('int'))


        print("apos tratativa")
        df.show(6)

        object_name = self.utils.transform_df_to_json(df, self.tipo_dado, "trusted")

        self.utils.set_data_s3_file(object_name, EnumBuckets.TRUSTED.value)

        # print(df.count())
