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

        # criando dataframe com o arquivo lido do bucket RAW
        df = self.spark.read.option("multiline", "true") \
            .json(path)                                  \
            .printSchema()
        df.show()
        
        # removendo valores nulos
        df = self.utils.remove_null(df)
        df.show()

        # pegando dados apenas do sensor de harmônicas
        df = self.utils.filter_by_sensor(df, "valueType", "Porcentagem")
        df.show()

        # removendo valores que não são floats
        df = self.utils.remove_wrong_float(df, "value")
        df.show()

        # formatando numeros
        df = self.utils.format_number(df, "value")
        df.printSchema()
        df.show()

        # ordenando por data, da maior para a menor
        df = self.utils.order_by_coluna_desc(df, "instant")
        df.show()

        # convertendo dataframe filtrado em um json
        object_name = self.utils.transform_df_to_json(df, "value")

        # enviando json filtrado para o bucket trusted
        self.utils.set_data_s3_file(object_name)