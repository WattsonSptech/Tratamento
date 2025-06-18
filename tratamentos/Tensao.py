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
    __MIN_LIMIT__ = 22000
    __MAX_LIMIT__ = 24000

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
        dados_base = self.__limpezas__(dados_base)

        pop_por_zona = self.__req_pop_por_zona__()

        dados_tratamentos["meses_dados"] = self.__get_meses_dados__(dados_base)
        dados_tratamentos["ocorrencias"] = self.__ocorrencias__(dados_base)
        dados_tratamentos["ocorrencias_por_dia"] = self.__ocorrs_por_dia__(dados_base, pop_por_zona)
        dados_tratamentos["ocorrencias_por_periodo"] = self.__ocorrs_por_periodo__(dados_base)

        self.__subir_dados_s3__(dados_tratamentos)

    def __ler_arq_telemetria__(self):
        filepath = self.utils.get_data_s3_csv(self.__SOURCE_BUCKET__)
        return pd.read_json(filepath, lines=True)

    def __limpezas__(self, df: pd.DataFrame):
        df = df[df["valueType"] == "volts"].reset_index(drop=True).drop(columns=['scenery', 'valueType'])

        if "EventProcessedUtcTime" in df.columns:
            df = df.drop(columns=['EventProcessedUtcTime', 'PartitionId', 'EventEnqueuedUtcTime', 'IoTHub'])

        df["instant"] = pd.to_datetime(df["instant"])

        return df.sort_values("instant", ascending=False)

    def __req_pop_por_zona__(self) -> pd.DataFrame:
        res = req.get(self.__ZONE_POPULATION_API_URL__)
        pop_by_zone = pd.read_csv(StringIO(res.text))

        pop_by_zone["zone"] = pop_by_zone["zona"].str.upper()
        return pop_by_zone.groupby("zone")["populacao"].sum()

    def __get_meses_dados__(self, telemetry: pd.DataFrame) -> pd.DataFrame:
        months = telemetry[["instant"]].copy()
        months["instant"] = months["instant"].dt.to_period("M").astype(str)
        return months.drop_duplicates()

    def __ocorrencias__(self, telemetry: pd.DataFrame) -> pd.DataFrame:
        ocorrencias = telemetry.copy()

        ocorrencias = ocorrencias[
            (ocorrencias["value"] >= self.__MAX_LIMIT__) | (ocorrencias["value"] <= self.__MIN_LIMIT__)
        ]
        ocorrencias["occurrence_type"] = ocorrencias["value"].apply(
            lambda x: "Alta tensão" if x >= self.__MAX_LIMIT__ else "Baixa tensão"
        )
        ocorrencias["instant"] = ocorrencias["instant"].dt.strftime("%d/%m/%Y %H:%M")

        return ocorrencias

    def __ocorrs_por_dia__(self, tension_telemetry: pd.DataFrame, pop_by_zone: pd.DataFrame):
        limits_by_zone = tension_telemetry.copy()

        limits_by_zone["day"] = limits_by_zone["instant"].dt.to_period("D")
        limits_by_zone = limits_by_zone.groupby(["day", "zone"]).agg(
            BaixaTensao=("value", lambda x: x[x < self.__MIN_LIMIT__].count()),
            AltaTensao=("value", lambda x: x[x > self.__MAX_LIMIT__].count())
        ).reset_index()

        juncao = limits_by_zone.merge(pop_by_zone, on="zone").sort_values("day", ascending=False)
        juncao["day"] = juncao["day"].astype(str)

        return juncao

    def __ocorrs_por_periodo__(self, tension_telemetry: pd.DataFrame):
        occrs_by_period = tension_telemetry.copy()

        def determinar_periodo(hora):
            if 0 <= hora < 6:
                return "Madrugada"
            if 6 <= hora < 11:
                return "Manhã"
            if 11 <= hora < 14:
                return "Meio dia"
            if 14 <= hora < 18:
                return "Tarde"
            return "Noite"
        occrs_by_period["day"] = occrs_by_period["instant"].dt.to_period("D").astype(str)
        occrs_by_period["day-period"] = occrs_by_period["instant"].dt.strftime("%H").astype(int).apply(determinar_periodo)

        occrs_by_period = occrs_by_period.groupby(["day", "zone", "day-period"]).agg({
            "value": lambda x: x[(x < self.__MIN_LIMIT__) | (x > self.__MIN_LIMIT__)].count()
        }).reset_index()

        return occrs_by_period

    def __subir_dados_s3__(self, dados: dict[str, pd.DataFrame]):
        for treatment_name, df in dados.items():

            filepath = "./temp/{}_{}.json".format(self.__SENSOR_NAME__, treatment_name)
            df.to_json(filepath, orient="records")

            self.utils.set_data_s3_file(filepath, self.__TARGET_BUCKET__)