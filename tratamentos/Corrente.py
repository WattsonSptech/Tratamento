from interfaces.ITratamento import ITratamentoDados
import pyspark.sql.functions as F
from pyspark.sql.functions import format_number as format
from interfaces.EnumBuckets import EnumBuckets
import os

class Corrente(ITratamentoDados):
    def __init__(self) -> None:
        super().__init__()
        self.nome_sensor = "Corrente"
        self.tipo_dado = "Ampere"
        self.outlier_min = 1
        self.outlier_max = 8
    
    def __tratar_dado__(self) -> None:
        nome_arquivo = self.utils.get_data_s3_csv(EnumBuckets.RAW.value)

        df = self.spark.read.option("multiline", "true").json(nome_arquivo)
        df.printSchema()
        
        df = self.utils.filter_by_sensor(df, "valueType", "volts")
        df.show()

        df = self.utils.format_number_to_float(df, "value")
        df.show()

        df = self.utils.remove_wrong_float(df, "value")
        df.show()

        df = self.utils.remove_null(df)
        df.show()

        df = self.utils.order_by_coluna_desc(df, "value")
        df.printSchema()
        df.show()

        object_name = self.utils.transform_df_to_json(df, "tensao", "trusted")

        self.utils.set_data_s3_file(object_name, EnumBuckets.TRUSTED.value)

        self.__gerar_arquivo_client__()
    
    def __gerar_arquivo_client__(self) -> None:
        arquivo_corrente = self.utils.get_data_s3_csv(bucket_name=EnumBuckets.TRUSTED.value, sensor="ampere")
        arquivo_tensao = self.utils.get_data_s3_csv(bucket_name=EnumBuckets.TRUSTED.value, sensor="volts")

        df_corrente = self.spark.read.option("multiline", "true").json(arquivo_corrente)
        df_tensao = self.spark.read.option("multiline", "true").json(arquivo_tensao)
        df_corrente.show()
        df_tensao.show()

        df_join = df_corrente.join(df_tensao, ['instant'], how="inner")
        df_join.printSchema()
        df_join.show()

        client_json_file = self.utils.transform_df_to_json(df_join, self.tipo_dado, "client")
        self.utils.set_data_s3_file(object_name=client_json_file, bucket_name=os.getenv("BUCKET_NAME_CLIENT"))

    
