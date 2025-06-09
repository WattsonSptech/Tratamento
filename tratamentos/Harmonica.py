from interfaces.ITratamento import ITratamentoDados
import pyspark.sql.functions as F
from pyspark.sql.functions import format_number as format
from interfaces.EnumBuckets import EnumBuckets
import os


class Harmonica(ITratamentoDados):

    def __init__(self):
        super().__init__()
        self.nome_sensor = "Harmonica"
        self.tipo_dado = "Porcentagem"
    
    
    def __tratar_dado__(self) -> None:
        nome_arquivo = self.utils.get_data_s3_csv(EnumBuckets.RAW.value)

        # criando dataframe com o arquivo lido do bucket RAW
        df = self.spark.read.option("multiline", "true").json(nome_arquivo)
        df.printSchema()
        df.show()
        
        # removendo valores nulos
        df = self.utils.remove_null(df)
        df.show()
        
        # removendo valores que não são floats
        df = self.utils.remove_wrong_float(df, "value")
        df.show()

        # formatando numeros
        df = self.utils.format_number(df, "value")
        df.printSchema()
        df.show()

        # ordenando por data, da maior para a menor
        df = self.utils.order_by_coluna_desc(df, "instant")
        df.show()

        # convertendo dataframe filtrado em um json
        trusted_json_file = self.utils.transform_df_to_json(df, "all_sensors", "trusted")

        # enviando json filtrado para o bucket trusted
        self.utils.set_data_s3_file(object_name=trusted_json_file, bucket_name=EnumBuckets.TRUSTED.value)

        self.__gerar_arquivo_client__()

    def __gerar_arquivo_client__(self) -> None:
        arquivo_trusted = self.utils.get_data_s3_csv(bucket_name=EnumBuckets.TRUSTED.value, sensor="all_sensors")
        df_trusted = self.spark.read.option("multiline", "true").json(arquivo_trusted)                                  

        df_harmonicas = self.utils.filter_by_sensor(df_trusted, "valueType", "Porcentagem")
        df_harmonicas = df_harmonicas.selectExpr("instant", "value as value_harmonicas", "valueType as valueType_harmonicas")
        df_harmonicas.printSchema()
        df_harmonicas.show()

        df_tensao = self.utils.filter_by_sensor(df_trusted, "valueType", "volts")
        df_tensao = df_tensao.selectExpr("instant", "value as value_tensao", "valueType as valueType_tensao")
        df_tensao.printSchema()
        df_tensao.show()

        df_tensao_harmonicas = df_harmonicas.join(df_tensao, ['instant'], how="inner")
        df_tensao_harmonicas.show()

        df_tensao_harmonicas = df_tensao_harmonicas \
            .drop('zone')                           \
            .drop('scenery')
        df_tensao_harmonicas.show()

        arquivo_client = self.utils.transform_df_to_json(df_tensao_harmonicas, "tensao_x_harmonica", "client")
        self.utils.set_data_s3_file(object_name=arquivo_client, bucket_name=os.getenv("BUCKET_NAME_CLIENT"))
