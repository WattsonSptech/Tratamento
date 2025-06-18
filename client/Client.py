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
        arquivo_harmonicas = self.utils.get_data_s3_csv(bucket_name=EnumBuckets.TRUSTED.value, sensor="Porcentagem")
        arquivo_frequencia = self.utils.get_data_s3_csv(bucket_name=EnumBuckets.TRUSTED.value, sensor="Hz")
        arquivo_temperatura = self.utils.get_data_s3_csv(bucket_name=EnumBuckets.TRUSTED.value, sensor="ºC")

        df_harmonicas = self.spark.read.option("multiline", "true").json(arquivo_harmonicas)                                  
        df_harmonicas = df_harmonicas.selectExpr("instant", "value as value_harmonicas", "valueType as valueType_harmonicas")

        df_frequencia = self.spark.read.option("multiline", "true").json(arquivo_frequencia)
        df_frequencia = df_frequencia.selectExpr("instant", "value as value_frequencia", "valueType as valueType_frequencia")

        df_temperatura = self.spark.read.option("multiline", "true").json(arquivo_temperatura)
        df_temperatura = df_temperatura.selectExpr("instant", "value as value_temperatura", "valueType as valueType_temperatura")

        df_join = df_harmonicas.join(df_frequencia, ['instant'], how="inner")
        df_join = df_join.join(df_temperatura, ['instant'], how="inner")

        # df_join.show()

        df_join = df_join.drop('zone')
        df_join = df_join.drop('scenery')
        # df_join.show()

        client_json_file = self.utils.transform_df_to_csv(df_join, "client", "client")
        self.utils.set_data_s3_file(object_name=client_json_file, bucket_name=EnumBuckets.CLIENT.value)