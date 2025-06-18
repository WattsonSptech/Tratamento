from interfaces.ITratamento import ITratamentoDados
import pyspark.sql.functions as F
from pyspark.sql.functions import format_number as format
from interfaces.EnumBuckets import EnumBuckets
import os

class FatorPotencia(ITratamentoDados):

    def __init__(self):
        super().__init__()
        self.nome_sensor = "SDM630"
        self.tipo_dado = "fator"

    def __tratar_dado__(self) -> None:
        nome_arquivo = self.utils.get_data_s3_csv(EnumBuckets.RAW.value)

        # criando dataframe com o arquivo lido do bucket RAW
        df = self.spark.read.option("multiline", "true").json(nome_arquivo)
        df.printSchema()
        
        # removendo valores nulos
        df = self.utils.remove_null(df)

        # pegando dados apenas do sensor de harmônicas
        df = self.utils.filter_by_sensor(df, "valueType", "fator")

        # removendo valores que não são floats
        df = self.utils.remove_wrong_float(df, "value")

        # formatando numeros
        df = self.utils.format_number(df, "value")
        df.printSchema()

        # ordenando por data, da maior para a menor
        df = self.utils.order_by_coluna_desc(df, "instant")
        df.show()
        
        # convertendo dataframe filtrado em um json
        trusted_json_file = self.utils.transform_df_to_json(df, self.tipo_dado, "trusted")

        # enviando json filtrado para o bucket trusted
        self.utils.set_data_s3_file(trusted_json_file, EnumBuckets.TRUSTED.value)

        self.__gerar_arquivo_client__()

    def __gerar_arquivo_client__(self) -> None:
        arquivo_fator = self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "fator")
        arquivo_temperatura = self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value,"ºC")
        arquivo_corrente = self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value,"Ampere")

        df_fator = self.spark.read.option("multiline", "true").json(arquivo_fator)
        df_temperatura = self.spark.read.option("multiline", "true").json(arquivo_temperatura)
        df_corrente = self.spark.read.option("multiline", "true").json(arquivo_corrente)

        df_fator = df_fator.selectExpr("instant", "value as value_fator", "valueType as valueType_fator")
        df_temperatura = df_temperatura.selectExpr("instant", "value as value_temperatura", "valueType as valueType_temperatura")
        df_corrente = df_corrente.selectExpr("instant", "value as value_corrente", "type as valueType_corrente")

        df_fator = df_fator.withColumn("instant", F.substring_index(F.col("instant"), ".", 1))
        df_temperatura = df_temperatura.withColumn("instant", F.substring_index(F.col("instant"), ".", 1))
        df_corrente = df_corrente.withColumn("instant", F.substring_index(F.col("instant"), ".", 1))

        df_fator.show()
        df_temperatura.show()
        df_corrente.show()

        df_join = df_fator.join(df_temperatura, ['instant'], how='inner') \
                          .join(df_corrente, ['instant'], how='inner')
        df_join.show()

        # convertendo dataframe filtrado em um csv
        client_json_file = self.utils.transform_df_to_json(df_join, "correlacoes_fator", "client")
        # client_csv_file = self.utils.transform_df_to_csv(df_join, self.tipo_dado, "client")
        # enviando csv filtrado para o bucket client
        self.utils.set_data_s3_file(client_json_file, EnumBuckets.CLIENT.value)
        print('arquivo enviado client')