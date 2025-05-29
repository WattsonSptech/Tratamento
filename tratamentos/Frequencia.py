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
    
    
    def __tratar_dado__(self, nome_arquivo) -> None:
        print(self.nome_sensor + " is working")
        self.utils.get_data_s3_csv(nome_arquivo=nome_arquivo)
        path = "temp/" + nome_arquivo
        # df = self.spark.read.csv(path, header=True, inferSchema=True)
        df = self.spark.read.csv("temp/frequence.csv", header=True, inferSchema=True) #remove after tests
        df.printSchema()
        df.show()
        df = self.utils.highlight_outlier(df, "frequency")
        df = self.utils.format_number(df, "frequency")
        df.printSchema()
        df.show()
