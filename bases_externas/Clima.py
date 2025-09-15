from interfaces.ITratamento import ITratamentoDados
import pyspark.sql.functions as F
from pyspark.sql.functions import format_number as format
from interfaces.EnumBuckets import EnumBuckets
from utils.DownloadDados import DownloadDados
import os
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from datetime import datetime, timedelta
from pyspark.sql.functions import to_timestamp


class Clima(ITratamentoDados):

    def __init__(self):
        super().__init__()
        self.download_dados = DownloadDados()
        self.nome_sensor = "Clima"
        self.tipo_dado = "celsius"

    def __tratar_dado__(self) -> None:
        end_date = datetime.today().date()
        start_date = end_date - timedelta(days=1)
        data = self.download_dados.consultarPorUrl(
            f"https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41"
            f"&hourly=temperature_2m&start_date={start_date}&end_date={end_date}"
        )

        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        temperatures = hourly.get("temperature_2m", [])

        print("times")
        print(times)
        print("temperatures")
        print(temperatures)

        if len(times) != len(temperatures):
            raise ValueError("Tamanho de 'time' e 'temperature_2m' não coincidem.")

        records = [
            {"time": t, "temperature_2m": temp}
            for t, temp in zip(times, temperatures)
        ]
        
        print("Primeiro registro:", records[0])
        print("Total de registros:", len(records)) 
                
        schema = StructType([
            StructField("time", StringType(), True),
            StructField("temperature_2m", DoubleType(), True)
        ])

        df = self.spark.createDataFrame(records, schema=schema)
        # data = df.limit(5).collect()
        # for row in data:
        #     print(row)
        df.printSchema()

        # df = df.withColumn("time", to_timestamp("time"))
        df.show(5, truncate=False)

        print("apos tratativa")
        df = self.utils.remove_wrong_float(df, "temperature_2m")
        df = self.utils.format_number(df, "temperature_2m")
        df = self.utils.order_by_coluna_desc(df, "time")

        object_name = self.utils.transform_df_to_json(df, self.tipo_dado, "trusted")

        self.utils.set_data_s3_file(object_name, EnumBuckets.TRUSTED.value)



        # print(df.count())

    def __gerar_arquivo_client__(self) -> None:
        arquivo_fator = self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "fator")
        arquivo_temperatura = self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value,"ºC")
        arquivo_corrente = self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value,"Ampere")

        df_fator = self.spark.read.option("multiline", "true").json(arquivo_fator)
        df_temperatura = self.spark.read.option("multiline", "true").json(arquivo_temperatura)
        df_corrente = self.spark.read.option("multiline", "true").json(arquivo_corrente)

        df_fator = df_fator.selectExpr("instant", "value as value_fator", "valueType as valueType_fator")
        df_temperatura = df_temperatura.selectExpr("instant", "value as value_temperatura", "valueType as valueType_temperatura")
        df_corrente = df_corrente.selectExpr("instant", "value as value_corrente", "valueType as valueType_corrente")

        df_fator = df_fator.withColumn("instant", F.substring_index(F.col("instant"), ".", 1))
        df_temperatura = df_temperatura.withColumn("instant", F.substring_index(F.col("instant"), ".", 1))
        df_corrente = df_corrente.withColumn("instant", F.substring_index(F.col("instant"), ".", 1))

        # df_fator.show()
        # df_temperatura.show()
        # df_corrente.show()

        df_join = df_fator.join(df_temperatura, ['instant'], how='inner') \
                          .join(df_corrente, ['instant'], how='inner')
        # df_join.show()

        # convertendo dataframe filtrado em um csv
        client_json_file = self.utils.transform_df_to_json(df_join, "correlacoes_fator", "client")
        # client_csv_file = self.utils.transform_df_to_csv(df_join, self.tipo_dado, "client")
        # enviando csv filtrado para o bucket client
        self.utils.set_data_s3_file(client_json_file, EnumBuckets.CLIENT.value)
        # print('arquivo enviado client')