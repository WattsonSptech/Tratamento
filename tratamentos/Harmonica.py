from interfaces.ITratamento import ITratamentoDados
import pyspark.sql.functions as F
from pyspark.sql.functions import format_number as format


class Harmonica(ITratamentoDados):

    def __init__(self):
        super().__init__()
        self.nome_sensor = "Harmonica"
        self.tipo_dado = "Porcentagem"
    
    
    def __tratar_dado__(self) -> None:
        nome_arquivo = self.utils.get_data_s3_csv()
        path = "temp/" + nome_arquivo

        df = self.spark.read.option("multiline", "true") \
            .json(path)                                  \
            .printSchema()                               \
        
        df.show()
        
        df = self.utils.remove_null(df)
        df.show()
        
        df = self.utils.remove_wrong_float(df, "Porcentagem")
        df.show()

        df = self.utils.format_number(df, "Porcentagem")
        df.printSchema()
        df.show()

        object_name = self.utils.transform_df_to_json(df, "Porcentagem")

        self.utils.set_data_s3_file(object_name)