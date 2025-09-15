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
        self.tipo_dado = "ºC"

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

        df = self.spark.createDataFrame(records)
        df.printSchema()

        df = df.withColumn("time", to_timestamp("time"))

        print("apos tratativa")
        df.show(6)

        # print(df.count())
