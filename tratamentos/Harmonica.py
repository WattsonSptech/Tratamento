from interfaces.ITratamento import ITratamentoDados
import pyspark.sql.functions as F
from pyspark.sql.functions import format_number as format
from interfaces.EnumBuckets import EnumBuckets


class Harmonica(ITratamentoDados):

    def __init__(self):
        super().__init__()
        self.nome_sensor = "Harmonica"
        self.tipo_dado = "Porcentagem"
    
    
    def __tratar_dado__(self) -> None:
        nome_arquivo = self.utils.get_data_s3_csv(EnumBuckets.RAW.value)


        # criando dataframe com o arquivo lido do bucket RAW
        df = self.spark.read.option("multiline", "true") \
            .json(nome_arquivo)                          \
            .printSchema()
        df.show()
        
        # removendo valores nulos
        df = self.utils.remove_null(df)
        df.show()

        # pegando dados apenas do sensor de harmônicas
        df = self.utils.filter_by_sensor(df, "valueType", "Porcentagem")
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
        trusted_json_file = self.utils.transform_df_to_json(df, "value")

        # enviando json filtrado para o bucket trusted
        self.utils.set_data_s3_file(object_name=trusted_json_file, bucket=EnumBuckets.TRUSTED)

        self.__gerar_arquivo_client__()

    def __gerar_arquivo_client__(self) -> None:
        arquivo_harmonicas = self.utils.get_data_s3_csv(bucket_name=EnumBuckets.TRUSTED, sensor="Porcentagem")
        arquivo_tensao = self.utils.get_data_s3_csv(bucket_name=EnumBuckets.TRUSTED, sensor="volts")

        path_harmonicas = "temp/" + arquivo_harmonicas
        path_tensao = "temp/" + arquivo_tensao

        df_harmonicas = self.spark.read.option("multiline", "true") \
            .json(path_harmonicas)                                  \
            .printSchema()
        df_harmonicas.show()

        df_tensao = self.spark.read.option("multiline", "true") \
            .json(path_tensao)                                  \
            .printSchema()
        df_tensao.show()