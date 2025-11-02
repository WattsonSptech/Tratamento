from interfaces.EnumBuckets import EnumBuckets
from interfaces.ITabelasFato import ITabelasFato
import pandas as pd

class FatoReclamacao(ITabelasFato):
    
    def __init__(self):
        super().__init__()
        self.colunas_fato_reclamacao = ['DATA_RECLAMACAO','HORA_MINUTO_RECLAMACAO','RECLAMACAO_STATUS','RECLAMACAO_CATEGORIA','TIPO_PRODUTO','TIPO_PROBLEMA','RECLAMACAO_SENTIMENTO','DATA_HORA_RECLAMACAO']

    def __gerar_tabela_fato__(self):
    
        df_fato_historico_reclamacao = None

        try:
            df_fato_historico_reclamacao = pd.read_csv(self.utils.get_data_s3_csv(EnumBuckets.CLIENT.value, "reclamacao_cliente/Fato_Reclamacao"), sep=";")
        except Exception as e:
            print("Erro: tabela fato Fato_Reclamacao ainda não existe: ", e)    
        
        df_fato_reclamacao = pd.read_csv(self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "ReclameAqui_TRUSTED_"), sep=";")
        
        if df_fato_historico_reclamacao is not None:
            df_fato_reclamacao = self.utils.concat_pd_dataframes(
                df_fato_historico_reclamacao, 
                df_fato_reclamacao, 
                ["DATA_HORA_RECLAMACAO"],
            )

        df_fato_reclamacao = self.utils.select_columns_pd(df_fato_reclamacao, self.colunas_fato_reclamacao)
        df_fato_reclamacao = df_fato_reclamacao.drop_duplicates(keep="first")

        filepath = "./temp/Fato_Reclamacao.csv"
        self.__salvar_flat_na_s3__(df_fato_historico_reclamacao, filepath)  
    
    def __salvar_flat_na_s3__(self, df: pd.DataFrame, path: str):
        df.to_csv(path, sep=";")
        self.utils.set_data_s3_file(path, EnumBuckets.CLIENT.value, "reclamacao_cliente/")