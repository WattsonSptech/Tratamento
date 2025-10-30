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
        self.ja_rodou = False
        self.horario_ultima = datetime.now()
        self.proximo_horario = datetime.now() + timedelta(days=1)
        self.horario_ultima = datetime.now()

    def __tratar_dado__(self) -> None:
        print(self.horario_ultima)
        print(self.proximo_horario)
        if(self.ja_rodou == True):
            if(self.utils.horario_ja_passou(self.proximo_horario) == False):
                print("Marcado para rodar: " + self.proximo_horario)
                return
            else: 
                self.horario_ultima = datetime.now()
                self.proximo_horario = datetime.now() + timedelta(days=30)

        data = self.download_dados.consultarPorQueryBase('teste')

        print(data)
        print(type(data))
        

    
        df = self.spark.createDataFrame(data)
        df.printSchema()

        df = self.utils.set_null_zero(df=df)
        df = self.utils.uppercase_strings(df=df)

        df = self.utils.rename_column(df=df, coluna="numero_consumidores", new_coluna="NUMERO_CONSUMIDORES")
        df = self.utils.rename_column(df=df, coluna="tipo_consumo", new_coluna="TIPO_CONSUMO")
        df = self.utils.rename_column(df=df, coluna="consumo", new_coluna="CONSUMO")
        df = self.utils.rename_column(df=df, coluna="sigla_uf", new_coluna="SIGLA_UF")
        df = self.utils.rename_column(df=df, coluna="ano", new_coluna="ANO")
        df = self.utils.rename_column(df=df, coluna="mes", new_coluna="MES")
        
        df.withColumn("numero_consumidores", col("numero_consumidores").cast('int'))
        df.withColumn("consumo", col("consumo").cast('int'))


        print("apos tratativa")
        df.show(6)

        object_name = self.utils.transform_df_to_json(df, self.tipo_dado, "trusted")

        self.utils.set_data_s3_file(object_name, EnumBuckets.TRUSTED.value)

        # print(df.count())
