from interfaces.EnumBuckets import EnumBuckets
from interfaces.ITratamento import ITratamentoDados
import pandas as pd

class GerarTabelaFato(ITratamentoDados):
    
    def __init__(self):
        super().__init__()

    def __gerar_fato_sensores__(self):
    
        df_trusted_tensao = pd.read_csv(self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "Tensao_TRUSTED_"), sep=";")
        df_trusted_clima = pd.read_json(self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "trusted_clima"), orient='records')
        df_trusted_reclamacoes = pd.read_csv(self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "ReclameAqui_TRUSTED_"), sep=";")
        df_trusted_consumo = pd.read_json(self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "trusted_generation"), orient='records')

        df_fato_sensor = self.__merge_fato_reclamacoes__(df_trusted_tensao, df_trusted_reclamacoes)
        df_fato_sensor = self.__merge_fato_clima__(df_fato_sensor, df_trusted_clima)
        df_fato_consumo = self.__create_fato_consumo__(df_trusted_consumo)

        filepath = "./temp/Fato_Tensao_Clima.csv"   
        factpath = "./temp/Fato_Consumo.csv"

        df_fato_sensor.to_csv(filepath, sep=";")
        df_fato_consumo.to_csv(factpath, sep=";")

    def __merge_fato_reclamacoes__(self, df_trusted_tensao, df_trusted_reclamacoes):
        df_fato_sensor = pd.merge(
            df_trusted_tensao, 
            df_trusted_reclamacoes, 
            left_on=['DATA_GERACAO'],
            right_on=['DATA_RECLAMACAO'],
            how="left"
        )

        df_fato_sensor["TENSAO_SEVERIDADE"] = df_fato_sensor["TENSAO_VALOR"] <= 0

        reclamacoes_por_dia_e_tipo = df_trusted_reclamacoes.groupby(['DATA_RECLAMACAO', 'RECLAMACAO_SENTIMENTO']).size().reset_index(name='TOTAL_NEGATIVOS')

        reclamacoes_negativas_por_dia = reclamacoes_por_dia_e_tipo[reclamacoes_por_dia_e_tipo['RECLAMACAO_SENTIMENTO'] == 'NEGATIVO'][['DATA_RECLAMACAO', 'TOTAL_NEGATIVOS']]
        total_reclamacoes_por_dia = df_trusted_reclamacoes.groupby('DATA_RECLAMACAO').size().reset_index(name='TOTAL_DIA')

        df_fato_sensor = df_fato_sensor.merge(reclamacoes_negativas_por_dia, on="DATA_RECLAMACAO", how='left')
        df_fato_sensor = df_fato_sensor.merge(total_reclamacoes_por_dia, on="DATA_RECLAMACAO", how='left')

        df_fato_sensor['INDICE_APROVACAO'] = (df_fato_sensor['TOTAL_DIA'] - df_fato_sensor['TOTAL_NEGATIVOS']) / df_fato_sensor['TOTAL_DIA']

        df_fato_sensor = df_fato_sensor.drop(
            [
                'Unnamed: 0', 
                'HORA_MINUTO_RECLAMACAO', 
                'RECLAMACAO_STATUS', 
                'RECLAMACAO_CATEGORIA',
                'TIPO_PRODUTO', 
                'TIPO_PROBLEMA', 
                'DATA_HORA_RECLAMACAO',
                'DATA_RECLAMACAO',
                'RECLAMACAO_SENTIMENTO',
                'TOTAL_NEGATIVOS', 
                'TOTAL_DIA'
            ], axis=1
        )

        df_fato_sensor = df_fato_sensor.drop_duplicates(keep="first")

        return df_fato_sensor
    
    def __merge_fato_clima__(self, df_fato, df_trusted_clima):

        df_fato["ANO_MES_DIA_HORA_GERACAO"] = pd.to_datetime(df_fato["DATA_HORA_GERACAO"]).dt.strftime('%Y-%m-%d %H')
        df_trusted_clima["ANO_MES_DIA_HORA_CLIMA"] = pd.to_datetime(df_trusted_clima["DATA_HORA_CLIMA"]).dt.strftime('%Y-%m-%d %H')

        df_fato_sensor = pd.merge(
            df_fato, 
            df_trusted_clima, 
            left_on=['ANO_MES_DIA_HORA_GERACAO'],
            right_on=['ANO_MES_DIA_HORA_CLIMA'],
            how="left"
        )

        df_fato_sensor["CLIMA_SEVERIDADE"] = df_fato_sensor["CLIMA_CHUVA"] >= 50 or df_fato_sensor["CLIMA_VENTO"] >= 50
        df_fato_sensor["CLIMA_EVENTO"] = "N/A" if df_fato_sensor["CLIMA_SEVERIDADE"] else "VENTO" if df_fato_sensor["CLIMA_VENTO"] >= 50 else "CHUVA"

        df_fato_sensor = df_fato_sensor.drop(
            [
                'ANO_MES_DIA_HORA_GERACAO',
                'DATA_HORA_CLIMA',
                'ANO_MES_DIA_HORA_CLIMA'
            ], axis=1
        )

        df_fato_sensor = df_fato_sensor.drop_duplicates(keep="first")

        return df_fato_sensor
    
    def __create_fato_consumo__(self,df_consumo):

        df_client_consumo = df_consumo.copy()
        df_client_consumo = df_client_consumo.drop('SIGLA_UF')
        df_client_consumo["ANO_MES_COLETA"] = df_client_consumo["ANO"] + "-" + df_client_consumo["MES"]
        df_client_consumo = df_client_consumo.drop(
            ['ANO', 'MES']
        )

        df_client_consumo = df_client_consumo.drop_duplicates(keep="first")

        return df_client_consumo

        

