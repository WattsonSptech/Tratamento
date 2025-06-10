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
        print("\t\t" + self.nome_sensor + " is working")

        nome_arquivo = self.utils.get_data_s3_csv(EnumBuckets.RAW.value)
        print("\t\t" + nome_arquivo)

        df = self.spark.read.option("multiline", "true").json(nome_arquivo)
        df.printSchema()
        df.show()
        
        df = self.utils.remove_null(df)
        print("\t\tremovendo nulls")
        df.show()

        df = self.utils.filter_by_sensor(df, "valueType", "Hz")

        df = self.utils.remove_wrong_float(df, "value")
        print("\t\tremovendo campos que não sao float da coluna frquence")
        df.show()

        df = self.utils.format_number(df, "value")
        print("\t\tformatando numeros")
        df.printSchema()
        df.show()

        print("\t\tordenando por data")
        df = self.utils.order_by_coluna_desc(df, "instant")

        print("\t\tgerando arquivo")
        object_name = self.utils.transform_df_to_json(df, self.tipo_dado, "trusted")


        self.utils.set_data_s3_file(object_name, EnumBuckets.TRUSTED.value)