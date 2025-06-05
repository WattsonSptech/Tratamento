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
        print(self.nome_sensor + " is working")

        nome_arquivo = self.utils.get_data_s3_csv(EnumBuckets.RAW.value)
        print(nome_arquivo)

        df = self.spark.read.option("multiline", "true").json(nome_arquivo)
        df.printSchema()
        df.show()
        
        df = self.utils.remove_null(df)
        print("removendo nulls")
        df.show()

        df = self.utils.filter_by_sensor(df, "valueType", "Hz")

        df = self.utils.remove_wrong_float(df, "value")
        print("removendo campos que não sao float da coluna frquence")
        df.show()

        df = self.utils.format_number(df, "value")
        print("formatando numeros")
        df.printSchema()
        df.show()

        print("ordenando por data")
        df = self.utils.order_by_coluna_desc(df, "instant")

        print("gerando arquivo")
        object_name = self.utils.transform_df_to_json(df, self.tipo_dado)


        self.utils.set_data_s3_file(object_name, EnumBuckets.TRUSTED.value)