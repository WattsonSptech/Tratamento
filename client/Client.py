from interfaces.ITratamento import ITratamentoDados
import pyspark.sql.functions as F
from pyspark.sql.functions import format_number as format
from interfaces.EnumBuckets import EnumBuckets



class Client(ITratamentoDados):

    def __init__(self):
        super().__init__()
        self.nome_sensor = "Client"

    
    # envia para o trusted / client
    # pfv adicionem seus sensores aqui
    def __tratar_dado__(self):
        arquivo_tensao = self.utils.get_data_s3_csv(EnumBuckets.CLIENT.value, "Tensao_ocorrencias.json") # Porcentagem
        arquivo_frequencia = self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "Hz")
        arquivo_temperatura = self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "ºC")
        if arquivo_frequencia == None:
            arquivo_temperatura = self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "\u00b0C")


        df_tensao = self.spark.read.option("multiline", "true").json(arquivo_tensao)
        df_tensao = df_tensao.selectExpr("instant", "value as value_tensao")

        df_frequencia = self.spark.read.option("multiline", "true").json(arquivo_frequencia)
        # df_frequencia.show()
        df_frequencia = df_frequencia.selectExpr("instant", "value as value_frequencia", "valueType as valueType_frequencia", "tensao_senoidal")

        df_temperatura = self.spark.read.option("multiline", "true").json(arquivo_temperatura)
        df_temperatura = df_temperatura.selectExpr("instant", "value as value_temperatura", "valueType as valueType_temperatura")

        df_join = df_tensao.join(df_frequencia, how="inner")
        df_join = df_join.join(df_temperatura, ['instant'], how="inner")

        # df_join.show()

        df_join = df_join.drop('zone')
        df_join = df_join.drop('scenery')
        # df_join.show()

        client_json_file = self.utils.transform_df_to_json(df_join, "client", "client")
        self.utils.set_data_s3_file(client_json_file, EnumBuckets.CLIENT.value)