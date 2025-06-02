from interfaces.ITratamento import ITratamentoDados
import pyspark.sql.functions as F
from pyspark.sql.functions import format_number as format


class Harmonica(ITratamentoDados):

    def __init__(self):
        super().__init__()
        self.nome_sensor = "Harmonica"
        self.tipo_dado = "Porcentagem"
    
    
    def __tratar_dado__(self, nome_arquivo) -> None:
        print(self.nome_sensor + " is working")

        # self.utils.get_data_s3_csv(nome_arquivo=nome_arquivo)
        path = "temp/raw/" + nome_arquivo

        df = self.spark.read. \
                json(f"{path}", header=True, inferSchema=True)
        
        df.printSchema()
        df.show()

        df = self.utils.highlight_outlier(df, "harmonic")
        df = self.utils.format_number(df, "harmonic")
        
        df.printSchema()
        df.show()
