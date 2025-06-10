from io import StringIO
import os
from typing import override
import pandas as pd
import requests as req
from datetime import datetime
from interfaces.EnumBuckets import EnumBuckets
from interfaces.ITratamento import ITratamentoDados
from utils.Utils import Utils

class Tensao(ITratamentoDados):
    
    __SENSOR_NAME__ = "Tensao"
    __SENSOR_UNIT__ = "voltz"
    __MIN_LIMIT__ = -23000
    __MAX_LIMIT__ = 23000

    __SOURCE_BUCKET__: str
    __TARGET_BUCKET__: str

    __ZONE_POPULATION_API_URL__ = "https://pastebin.com/raw/Nz1aMUEM"
    
    @override
    def __init__(self) -> None:
        self.utils = Utils()
        self.__SOURCE_BUCKET__ = os.getenv("BUCKET_NAME_RAW", None)
        self.__TARGET_BUCKET__ = os.getenv("BUCKET_NAME_CLIENT", None)

        if self.__SOURCE_BUCKET__ is None or self.__TARGET_BUCKET__ is None:
            raise EnvironmentError(".env's dos buckets raw e client não definidos")

    @override
    def __tratar_dado__(self) -> None:
        dados_tratamentos = {}

        dados_base = self.__ler_arq_telemetria__()
        pop_por_zona = self.__req_pop_por_zona__()

        dados_tratamentos["estouros-por-zona"] = self.__agg_estouros_por_zona__(dados_base, pop_por_zona)

        self.__subir_dados_s3__(dados_tratamentos)

    def __ler_arq_telemetria__(self):
        filepath = self.utils.get_data_s3_csv(self.__SOURCE_BUCKET__)

        return pd.read_json(filepath, lines=True)
    
    def __req_pop_por_zona__(self) -> pd.DataFrame:
        res = req.get(self.__ZONE_POPULATION_API_URL__)
        pop_by_zone = pd.read_csv(StringIO(res.text))

        pop_by_zone["zone"] = pop_by_zone["zona"].str.upper()
        return pop_by_zone.groupby("zone")["populacao"].sum()
    
    def __agg_estouros_por_zona__(self, tension_telemtry: pd.DataFrame, pop_by_zone: pd.DataFrame):
        limits_by_zone = tension_telemtry.groupby("zone").agg(
            BaixaTensao=("value", lambda x: x[x < self.__MIN_LIMIT__].count()),
            AltaTensao=("value", lambda x: x[x > self.__MAX_LIMIT__].count())
        ).sort_values(["AltaTensao", "BaixaTensao"], ascending=False)

        return limits_by_zone.merge(pop_by_zone, on="zone").rename(columns={"populacao": "PopulacaoTotal"}).sort_values("PopulacaoTotal", ascending=False)

    def __subir_dados_s3__(self, dados: dict[str, pd.DataFrame]):
        for treatment_name, df in dados.items():

            filepath = "./temp/{}_{}_{}.json".format(
                self.__SENSOR_NAME__, treatment_name, datetime.now().isoformat()
            )
            df.to_json(filepath, orient="records")

            self.utils.set_data_s3_file(filepath, self.__TARGET_BUCKET__)