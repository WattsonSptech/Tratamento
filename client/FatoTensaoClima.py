from interfaces.EnumBuckets import EnumBuckets
from interfaces.ITabelasFato import ITabelasFato
import pandas as pd
import os

class FatoTensaoClima(ITabelasFato):
    
    def __init__(self):
        super().__init__()
        self.colunas_fato_sensor = ['DATA_GERACAO','HORA_MINUTO_GERACAO','ANO_MES_GERACAO','ZONA_GERACAO','DATA_HORA_GERACAO','TENSAO_VALOR','TENSAO_SEVERIDADE','CLIMA_TEMPERATURA','CLIMA_CHUVA','CLIMA_VENTO','CLIMA_SEVERIDADE','CLIMA_EVENTO','INDICE_APROVACAO']

    def __gerar_tabela_fato__(self):
        
        df_fato_historico_sensor = None

        if not os.path.isdir("temp/tensao_clima"):
            os.mkdir("temp/tensao_clima")

        try:
            df_fato_historico_sensor = pd.read_csv(self.utils.get_data_s3_csv(EnumBuckets.CLIENT.value, "tensao_clima/Fato_Tensao_Clima"), sep=";")
        except Exception as e:
            print("Erro: tabela fato Fato_Tensao_Clima ainda não existe: ", e)


        df_trusted_tensao = pd.read_csv(self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "Tensao_TRUSTED_"), sep=";")
        df_trusted_clima = pd.read_csv(self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "TRUSTED_clima"), sep=";")
        df_trusted_reclamacoes = pd.read_csv(self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "ReclameAqui_TRUSTED_"), sep=";")
        
        df_fato_sensor = self.__merge_fato_reclamacoes__(df_trusted_tensao, df_trusted_reclamacoes)
        df_fato_sensor = self.__merge_fato_clima__(df_fato_sensor, df_trusted_clima)

        if df_fato_historico_sensor is not None:
            df_fato_sensor = self.utils.concat_pd_dataframes(
                df_fato_historico_sensor, 
                df_fato_sensor, 
                ["DATA_HORA_GERACAO"], 
            )

        df_fato_sensor = self.utils.select_columns_pd(df_fato_sensor, self.colunas_fato_sensor)
        df_fato_sensor = df_fato_sensor.drop_duplicates(keep="first")
        
        filepath = "./temp/Fato_Tensao_Clima.csv" 
        self.__salvar_flat_na_s3__(df_fato_sensor, filepath)
        
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

        df_fato_sensor["CLIMA_SEVERIDADE"] = (df_fato_sensor["CLIMA_CHUVA"] >= 25) | (df_fato_sensor["CLIMA_VENTO"] >= 25)
        df_fato_sensor["CLIMA_EVENTO"] = "SEM EVENTO"
        df_fato_sensor.loc[(df_fato_sensor["CLIMA_SEVERIDADE"] == True) & (df_fato_sensor["CLIMA_VENTO"] >= 25), "CLIMA_EVENTO"] = "VENTO"
        df_fato_sensor.loc[(df_fato_sensor["CLIMA_SEVERIDADE"] == True) & (df_fato_sensor["CLIMA_CHUVA"] >= 25), "CLIMA_EVENTO"] = "CHUVA"

        df_fato_sensor = df_fato_sensor.drop(
            [
                'ANO_MES_DIA_HORA_GERACAO',
                'DATA_HORA_CLIMA',
                'ANO_MES_DIA_HORA_CLIMA'
            ], axis=1
        )

        df_fato_sensor = df_fato_sensor.drop_duplicates(keep="first")

        return df_fato_sensor
    
    def __salvar_flat_na_s3__(self, df, path):
        df.to_csv(path, sep=";")
        self.utils.set_data_s3_file(path, EnumBuckets.CLIENT.value, "tensao_clima/")