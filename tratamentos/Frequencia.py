from interfaces.ITratamento import ITratamentoDados
import pyspark.sql.functions as F
from pyspark.sql.functions import format_number as format


class Frequencia(ITratamentoDados):

    def __init__(self):
        super().__init__()
        self.nome_sensor = "Frequencia"
        self.tipo_dado = "Hz"
        self.outlier_min = 95
        self.outlier_max = 5
    
    
    def __tratar_dado__(self) -> None:
        print(self.nome_sensor + " is working")
        nome_arquivo = self.utils.get_data_s3_csv()
        path = "temp/" + nome_arquivo
        # df = self.spark.read.csv(path, header=True, inferSchema=True)
        # df = self.spark.read.csv("temp/frequence.csv", header=True, inferSchema=True) 
        df = self.spark.read.option("multiline", "true").json(path)
        df.printSchema()
        df.show()
        # df = self.utils.highlight_outlier(df, "frequencia")
        df = self.utils.remove_null(df) # isso remove null de todos os campos
        print("removendo nulls")
        df.show()
        df = self.utils.remove_wrong_float(df, "frequencia")
        print("removendo campos que não sao float da coluna frquence")
        df.show()
        df = self.utils.format_number(df, "frequencia")
        print("formatando numeros")
        df.printSchema()
        df.show()

        print("gerando arquivo")
        object_name = self.utils.tranform_df_to_json(df, "frequencia")

        self.utils.set_data_s3_file(object_name)
        print("Feito!")

