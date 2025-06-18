from interfaces.ITratamento import ITratamentoDados
import pyspark.sql.functions as F
from pyspark.sql.functions import format_number as format
from interfaces.EnumBuckets import EnumBuckets


class Temperatura(ITratamentoDados):

    def __init__(self):
        super().__init__()
        self.nome_sensor = "Temperatura"
        self.tipo_dado = "ºC"
        self.outlier_min = 95
        self.outlier_max = 5
    
    
    def __tratar_dado__(self) -> None:
        nome_arquivo = self.utils.get_data_s3_csv(EnumBuckets.RAW.value)

        df = self.spark.read.option("multiline", "true").json(nome_arquivo)
        # df.printSchema()
        # df.show()
        
        df = self.utils.remove_null(df)
        # df.show()
        #\u00b0C
        df = self.utils.filter_by_sensor(df, "valueType", "°C")

        df = self.utils.remove_wrong_float(df, "value")
      
        # df.show()

        df = self.utils.format_number(df, "value")
        # df.printSchema()
        # df.show()

        df = self.utils.order_by_coluna_desc(df, "instant")

        object_name = self.utils.transform_df_to_json(df, self.tipo_dado, "trusted")

        self.utils.set_data_s3_file(object_name, EnumBuckets.TRUSTED.value)