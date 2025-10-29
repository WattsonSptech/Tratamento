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
        self.tipo_dado = "clima"  
        self.horario_ultima = datetime.now()
        self.proximo_horario = datetime.now() + timedelta(hours=1)

    def __tratar_dado__(self) -> None:
        end_date = datetime.today().date()
        start_date = end_date - timedelta(days=1)

        data = self.download_dados.consultarPorUrl(
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude=52.52&longitude=13.41"
            f"&hourly=temperature_2m,rain,wind_speed_10m"
            f"&start_date={start_date}&end_date={end_date}"
        )

        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        temperatures = hourly.get("temperature_2m", [])
        rain = hourly.get("rain", [])
        wind = hourly.get("wind_speed_10m", [])

        print("times:", len(times))
        print("temperatures:", len(temperatures))
        print("rain:", len(rain))
        print("wind_speed_10m:", len(wind))

        if not (len(times) == len(temperatures) == len(rain) == len(wind)):
            raise ValueError("As listas de tempo, temperatura, chuva e vento têm tamanhos diferentes.")

        records = [
            {
                "time": t,
                "temperature_2m": temp,
                "rain": r,
                "wind_speed_10m": w
            }
            for t, temp, r, w in zip(times, temperatures, rain, wind)
        ]

        print("Primeiro registro:", records[0])
        print("Total de registros:", len(records))

        schema = StructType([
            StructField("time", StringType(), True),
            StructField("temperature_2m", DoubleType(), True),
            StructField("rain", DoubleType(), True),
            StructField("wind_speed_10m", DoubleType(), True)
        ])

        df = self.spark.createDataFrame(records, schema=schema)
        df.printSchema()
        df.show(5, truncate=False)

        print("Após tratativa")

        for col in ["temperature_2m", "rain", "wind_speed_10m"]:
            df = self.utils.remove_wrong_float(df, col)
            df = self.utils.format_number(df, col)

        df = self.utils.order_by_coluna_desc(df, "time")

        object_name = self.utils.transform_df_to_json(df, self.tipo_dado, "trusted")

        self.utils.set_data_s3_file(object_name, EnumBuckets.TRUSTED.value)

    def __gerar_arquivo_client__(self) -> None:
        arquivo_fator = self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "fator")
        arquivo_clima = self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "clima")
        arquivo_corrente = self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "Ampere")

        df_fator = self.spark.read.option("multiline", "true").json(arquivo_fator)
        df_clima = self.spark.read.option("multiline", "true").json(arquivo_clima)
        df_corrente = self.spark.read.option("multiline", "true").json(arquivo_corrente)

        df_fator = df_fator.selectExpr("instant", "value as value_fator", "valueType as valueType_fator")
        df_clima = df_clima.selectExpr("instant", "temperature_2m as temp", "rain as rain", "wind_speed_10m as wind")
        df_corrente = df_corrente.selectExpr("instant", "value as value_corrente", "valueType as valueType_corrente")

        for df in [df_fator, df_clima, df_corrente]:
            df = df.withColumn("instant", F.substring_index(F.col("instant"), ".", 1))

        df_join = df_fator.join(df_clima, ['instant'], how='inner') \
                          .join(df_corrente, ['instant'], how='inner')

        client_json_file = self.utils.transform_df_to_json(df_join, "correlacoes_fator", "client")
        self.utils.set_data_s3_file(client_json_file, EnumBuckets.CLIENT.value)
