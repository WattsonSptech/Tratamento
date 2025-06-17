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

        df_corrente = self.spark.read.option("multiline", "true").json(nome_arquivo)
        df_tensao = self.spark.read.option("multiline", "true").json(nome_arquivo)
        df_potencia = self.spark.read.option("multiline", "true").json(nome_arquivo)
        df_temperatura = self.spark.read.option("multiline", "true").json(nome_arquivo)

        
        df_corrente = self.utils.filter_by_sensor(df_corrente, "valueType", "amp\u00e9re")
        df_tensao = self.utils.filter_by_sensor(df_tensao, "valueType", "volts")
        df_potencia = self.utils.filter_by_sensor(df_potencia, "valueType", "fator")
        df_temperatura = self.utils.filter_by_sensor(df_temperatura, "valueType", "\u00b0C")

        df_tensao = self.utils.drop_column(df_tensao, "valueType")
        df_tensao = self.utils.drop_column(df_tensao, "zone")
        df_tensao = self.utils.drop_column(df_tensao, "scenery")
        df_tensao = self.utils.drop_column(df_tensao, "instant")
        df_tensao = self.utils.format_number_to_float(df_tensao, "value")
        df_tensao = self.utils.remove_wrong_float(df_tensao, "value")
        df_tensao = self.utils.order_by_coluna_desc(df_tensao, "value")
        df_tensao = self.utils.rename_column(df_tensao, "value", "voltage")
        df_tensao = self.utils.enumerate_column(df_tensao, "id")
    
        df_tensao.show()

        df_potencia = self.utils.drop_column(df_potencia, "valueType")
        df_potencia = self.utils.drop_column(df_potencia, "zone")
        df_potencia = self.utils.drop_column(df_potencia, "scenery")
        df_potencia = self.utils.drop_column(df_potencia, "instant")
        df_potencia = self.utils.format_number_to_float(df_potencia, "value")
        df_potencia = self.utils.remove_wrong_float(df_potencia, "value")
        df_potencia = self.utils.rename_column(df_potencia, "value", "power_factor")
        df_potencia = self.utils.enumerate_column(df_potencia, "id")

        df_potencia.show()

        df_temperatura = self.utils.drop_column(df_temperatura, "valueType")
        df_temperatura = self.utils.drop_column(df_temperatura, "zone")
        df_temperatura = self.utils.drop_column(df_temperatura, "scenery")
        df_temperatura = self.utils.drop_column(df_temperatura, "instant")
        df_temperatura = self.utils.format_number_to_float(df_temperatura, "value")
        df_temperatura = self.utils.remove_wrong_float(df_temperatura, "value")
        df_temperatura = self.utils.rename_column(df_temperatura, "value", "temperature")
        df_temperatura = self.utils.enumerate_column(df_temperatura, "id")

        df_temperatura.show()

        df_corrente = self.utils.remove_null(df_corrente)
        df_corrente = self.utils.remove_wrong_float(df_corrente, "value")
        df_corrente = self.utils.rename_values(df_corrente, "valueType", "amp\u00e9re", "Ampere")
        df_corrente = self.utils.enumerate_column(df_corrente, "id")
        df_corrente = df_corrente.join(df_tensao, "id")
        df_corrente = df_corrente.join(df_potencia, "id")
        df_corrente = df_corrente.join(df_temperatura, "id")
        
        df_corrente.printSchema()
        df_corrente.show()
        
        object_name = self.utils.transform_df_to_json(df_corrente, self.tipo_dado, "trusted")
        self.utils.set_data_s3_file(object_name, EnumBuckets.TRUSTED.value)

        self.__gerar_arquivo_client__()
    
    def __gerar_arquivo_client__(self) -> None:
        arquivo_corrente = self.utils.get_data_s3_csv(bucket_name=EnumBuckets.TRUSTED.value, sensor=self.tipo_dado)
        df = self.spark.read.option("multiline", "true").json(arquivo_corrente)
        df = self.utils.rename_column(df, "value", "secundary_current")
        df = self.utils.format_number_to_float(df, "secundary_current")
        df = self.utils.format_number_to_float(df, "voltage")
        df = self.utils.format_number_to_float(df, "power_factor")
        df = self.utils.format_number_to_float(df, "temperature")
        df = self.utils.drop_column(df, "zone")
        df = self.utils.drop_column(df, "valueType")
        df = self.utils.order_by_coluna_desc(df, "value")
        df = self.__encontrar_corrente_primaria__(df)
        df = self.__encontrar_corrente_curto_circuito__(df)
        df = self.utils.order_by_coluna_asc(df,"id")
        df.printSchema()
        df.show()

        client_json_file = self.utils.transform_df_to_csv(df, self.tipo_dado, "client")
        self.utils.set_data_s3_file(object_name=client_json_file, bucket_name=os.getenv("BUCKET_NAME_CLIENT"))

    def __encontrar_corrente_primaria__(self,df):
        return df.withColumn("primary_current",(F.col("secundary_current") * 160).cast("int"))
    
    def __encontrar_corrente_curto_circuito__(self,df):
        return df.withColumn("short_circuit_current",(F.col("voltage") / 40).cast("int"))

    
