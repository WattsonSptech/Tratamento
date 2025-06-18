import os.path

from interfaces.ITratamento import ITratamentoDados
import pyspark.sql.functions as F
from pyspark.sql.functions import format_number as format
from interfaces.EnumBuckets import EnumBuckets


class Frequencia(ITratamentoDados):

    def __init__(self):
        super().__init__()
        self.nome_sensor = "Frequencia"
        self.tipo_dado = "Hz"
        self.outlier_min = 95
        self.outlier_max = 5
    
    
    def __tratar_dado__(self) -> None:
        nome_arquivo = self.utils.get_data_s3_csv(EnumBuckets.RAW.value)

        df = self.spark.read.json(nome_arquivo)
        # df.printSchema()
        # df.show()
        
        df = self.utils.remove_null(df)
        # df.show()

        dfTensao = self.utils.filter_by_sensor(df, "valueType", "volts")
        dfTensao = dfTensao.drop("instant")

        df = self.utils.filter_by_sensor(df, "valueType", "Hz")

        dfTensao = dfTensao.selectExpr("value as value_tensao", "valueType as valueType_tensao")
        df = df.join(dfTensao)
        df = df.withColumn("tempo", F.monotonically_increasing_id() * 0.01)
        df = df.withColumn("tensao_senoidal", F.col("value_tensao") * F.sin(2 * F.pi() * F.col("value") * F.col("tempo")))
        df = df.drop("valueType_tensao", "value_tensao", "tempo")

        df = self.utils.remove_wrong_float(df, "value")
        # df.show()

        df = self.utils.format_number(df, "value")
        # df.printSchema()
        # df.show()

        df = self.utils.order_by_coluna_desc(df, "instant")

        object_name = self.utils.transform_df_to_json(df, self.tipo_dado, "trusted")

        self.utils.set_data_s3_file(object_name, EnumBuckets.TRUSTED.value)