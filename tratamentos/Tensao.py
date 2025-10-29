from io import StringIO
import os
# from typing import override
import pandas as pd
import requests as req
from datetime import datetime
from interfaces.EnumBuckets import EnumBuckets
from interfaces.ITratamento import ITratamentoDados
from utils.Utils import Utils

class Tensao(ITratamentoDados):
    
    __SENSOR_NAME__ = "Tensao"
    __SENSOR_UNIT__ = "voltz"
    __MIN_LIMIT__ = 22000
    __MAX_LIMIT__ = 24000

    __SOURCE_BUCKET__: str
    __TARGET_BUCKET__: str

    __ZONE_POPULATION_API_URL__ = "https://pastebin.com/raw/Nz1aMUEM"
    
    # @override
    def __init__(self) -> None:
        self.utils = Utils()
        self.__SOURCE_BUCKET__ = os.getenv("BUCKET_NAME_RAW", None)
        self.__TARGET_BUCKET__ = os.getenv("BUCKET_NAME_TRUSTED", None)

        if self.__SOURCE_BUCKET__ is None or self.__TARGET_BUCKET__ is None:
            raise EnvironmentError(".env's dos buckets raw e trusted não definidos")

    # @override
    def __tratar_dado__(self) -> None:
        dados_tratamentos = {}

        dados_base = self.__ler_arq_telemetria__()
        dados_tratamentos = self.__limpezas__(dados_base)

        self.__subir_dados_s3__(dados_tratamentos)

    def __ler_arq_telemetria__(self):
        filepath = self.utils.get_data_s3_csv(self.__SOURCE_BUCKET__, "generation-")
        return pd.read_json(filepath)

    def __limpezas__(self, df: pd.DataFrame):

        df = df.rename(
            columns=
            {
                'valor': 'VALOR', 
                'zona': "ZONA", 
                'timestamp': 'DATA_HORA_GERACAO'
            }
        )

        df["DATA_HORA_GERACAO"] = pd.to_datetime(df["DATA_HORA_GERACAO"])

        df["DATA_GERACAO"] = df["DATA_HORA_GERACAO"].dt.date
        df["ANO_MES_GERACAO"] = df["DATA_HORA_GERACAO"].dt.strftime('%Y-%m')
        df["HORA_MINUTO_GERACAO"] = df["DATA_HORA_GERACAO"].dt.strftime('%H:%M')
        df["VALOR"] = df["VALOR"].round(3)


        df = df.sort_values(["DATA_GERACAO", "HORA_MINUTO_GERACAO"], ascending=[False, False])
        df = df.groupby(["DATA_GERACAO", "HORA_MINUTO_GERACAO", "ANO_MES_GERACAO", "ZONA", "DATA_HORA_GERACAO"])[['VALOR']].mean()

        return df

    def __subir_dados_s3__(self, dados: pd.DataFrame):
        
        ano = datetime.today().year
        mes = datetime.today().month
        dia = datetime.today().day

        filepath = "./temp/{}_TRUSTED_{}{}{}.csv".format(self.__SENSOR_NAME__, ano, mes, dia)
        dados.to_csv(filepath, sep=";")
        self.utils.set_data_s3_file(filepath, self.__TARGET_BUCKET__)