from interfaces.EnumBuckets import EnumBuckets
from interfaces.ITratamento import ITratamentoDados
import pandas as pd

class GerarTabelaFato(ITratamentoDados):
    
    def __init__(self):
        super().__init__()

    def __gerar_fato_sensores__(self):
        
        df_fato_historico_sensor = None
        df_fato_historico_consumo = None
        df_fato_historico_reclamacao = None

        try:
            df_fato_historico_sensor = pd.read_csv(self.utils.get_data_s3_csv(EnumBuckets.CLIENT.value, "Fato_Tensao_Clima"), sep=";")
        except Exception as e:
            print("Erro: tabela fato Fato_Tensao_Clima ainda não existe: ", e)

        try:
            df_fato_historico_consumo = pd.read_csv(self.utils.get_data_s3_csv(EnumBuckets.CLIENT.value, "Fato_Consumo"), sep=";")
        except Exception as e:
            print("Erro: tabela fato Fato_Consumo ainda não existe: ", e)

        try:
            df_fato_historico_reclamacao = pd.read_csv(self.utils.get_data_s3_csv(EnumBuckets.CLIENT.value, "Fato_Reclamacao"), sep=";")
        except Exception as e:
            print("Erro: tabela fato Fato_Reclamacao ainda não existe: ", e)

        df_trusted_tensao = pd.read_csv(self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "Tensao_TRUSTED_"), sep=";")
        df_trusted_clima = pd.read_csv(self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "TRUSTED_clima"), sep=";")
        df_trusted_reclamacoes = pd.read_csv(self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "ReclameAqui_TRUSTED_"), sep=";")
        df_trusted_consumo = pd.read_csv(self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "trusted_generation"), sep=";")
        
        df_fato_sensor = self.__merge_fato_reclamacoes__(df_trusted_tensao, df_trusted_reclamacoes)
        df_fato_sensor = self.__merge_fato_clima__(df_fato_sensor, df_trusted_clima)

        if df_fato_historico_sensor is not None:
            df_fato_sensor = self.__agregar_dados_historicos__(df_fato_historico_sensor, df_fato_sensor, ["DATA_HORA_GERACAO"])
        
        df_fato_consumo = self.__create_fato_consumo__(df_trusted_consumo)

        if df_fato_historico_consumo is not None:
            df_fato_consumo = self.__agregar_dados_historicos__(df_fato_historico_consumo, df_fato_consumo, ["ANO_MES_COLETA"])
        
        df_fato_reclamacao = pd.read_csv(self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "ReclameAqui_TRUSTED_"), sep=";")
        
        if df_fato_historico_reclamacao is not None:
            df_fato_reclamacao = self.__agregar_dados_historicos__(df_fato_historico_reclamacao, df_fato_reclamacao, ["DATA_HORA_RECLAMACAO"])
        
        filepath = "./temp/Fato_Tensao_Clima.csv" 
        df_fato_sensor.to_csv(filepath, sep=";")
        self.utils.set_data_s3_file(filepath, EnumBuckets.CLIENT.value)
        
        factpath = "./temp/Fato_Consumo.csv"
        df_fato_consumo.to_csv(factpath, sep=";")
        self.utils.set_data_s3_file(factpath, EnumBuckets.CLIENT.value)

        reclamacaopath = "./temp/Fato_Reclamacao.csv" 
        df_fato_reclamacao.to_csv(reclamacaopath, sep=";")
        self.utils.set_data_s3_file(reclamacaopath, EnumBuckets.CLIENT.value)

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

        df_fato_sensor["CLIMA_SEVERIDADE"] = (df_fato_sensor["CLIMA_CHUVA"] >= 50) | (df_fato_sensor["CLIMA_VENTO"] >= 50)
        df_fato_sensor["CLIMA_EVENTO"] = "N/A"
        df_fato_sensor.loc[(df_fato_sensor["CLIMA_SEVERIDADE"] == True) & (df_fato_sensor["CLIMA_VENTO"] >= 50), "CLIMA_EVENTO"] = "VENTO"
        df_fato_sensor.loc[(df_fato_sensor["CLIMA_SEVERIDADE"] == True) & (df_fato_sensor["CLIMA_CHUVA"] >= 50), "CLIMA_EVENTO"] = "CHUVA"

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
        df_client_consumo = df_client_consumo.drop('SIGLA_UF', axis=1)
        df_client_consumo["ANO_MES_COLETA"] = (
            df_client_consumo["ANO"].astype(str) + "-" + df_client_consumo["MES"].astype(str).str.zfill(2)
        )
        df_client_consumo = df_client_consumo.drop(
            ['ANO', 'MES'], axis=1
        )

        df_client_consumo = df_client_consumo.drop_duplicates(keep="first")

        return df_client_consumo

    def __agregar_dados_historicos__(self, df_fato_historico, df_fato, sort_column):

        df_fato_historico = df_fato_historico.drop(   
            [
                'Unnamed: 0',
            ], axis=1
        )

        df_final = pd.concat([df_fato_historico, df_fato], ignore_index=True)

        df_final = df_final.drop_duplicates(keep="first")
        df_final = df_final.sort_values(by=sort_column, ascending=False)
        return df_final

