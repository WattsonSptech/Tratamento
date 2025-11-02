from interfaces.EnumBuckets import EnumBuckets
from interfaces.ITabelasFato import ITabelasFato
import pandas as pd

class FatoConsumo(ITabelasFato):
    
    def __init__(self):
        super().__init__()
        self.colunas_fato_consumo = ['ANO_MES_COLETA','NUMERO_CONSUMIDORES','NUMERO_CONSUMO','TIPO_CONSUMO']

    def __gerar_tabela_fato__(self):

        df_fato_historico_consumo = None

        try:
            df_fato_historico_consumo = pd.read_csv(self.utils.get_data_s3_csv(EnumBuckets.CLIENT.value, "consumo/Fato_Consumo"), sep=";")
        except Exception as e:
            print("Erro: tabela fato Fato_Consumo ainda não existe: ", e)

        df_consumo = pd.read_csv(self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "trusted_generation"), sep=";")

        df_client_consumo = df_consumo.copy()
        df_client_consumo = df_client_consumo.drop('SIGLA_UF', axis=1)
        df_client_consumo["ANO_MES_COLETA"] = (
            df_client_consumo["ANO"].astype(str) + "-" + df_client_consumo["MES"].astype(str).str.zfill(2)
        )
        
        df_client_consumo = df_client_consumo.drop(
            ['ANO', 'MES'], axis=1
        )

        df_client_consumo = df_client_consumo.drop_duplicates(keep="first")

        if df_fato_historico_consumo is not None:
            df_fato_consumo = self.utils.concat_pd_dataframes(
                df_fato_historico_consumo, 
                df_fato_consumo, 
                ["ANO_MES_COLETA"],
            )
        
        df_fato_consumo = self.utils.select_columns_pd(df_fato_consumo, self.colunas_fato_consumo)
        df_fato_consumo = df_fato_consumo.drop_duplicates(keep="first")
        
        filepath = "./temp/Fato_Consumo.csv"
        self.__salvar_flat_na_s3__(df_fato_consumo, filepath)  
    
    def __salvar_flat_na_s3__(self, df, path):
        df.to_csv(path, sep=";")
        self.utils.set_data_s3_file(path, EnumBuckets.CLIENT.value, "consumo/")