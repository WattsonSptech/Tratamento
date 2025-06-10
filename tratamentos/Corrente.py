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

        df = self.spark.read.option("multiline", "true").json(nome_arquivo)
        df.printSchema()
        df.show()
        
        df = self.utils.remove_null(df)
        df.show()

        df = self.utils.filter_by_sensor(df, "valueType", "ámpere")
        df.show()

        df = self.utils.remove_wrong_float(df, "value")
        df.show()

        df = self.utils.format_number(df, "value")
        df.printSchema()
        df.show()

        df = self.utils.order_by_coluna_desc(df, "instant")
        df.show()

        object_name = self.utils.transform_df_to_json(df, self.tipo_dado, "trusted")

        caminho_diretorio = 'temp/'
        if "temp/" in object_name:
            print("O arquivo ja esta na pasta temp")
        # if os.path.exists(caminho_diretorio):
        #     print('O diretório já existe.')
        # else:  
        #     caminho_arquivo = caminho_diretorio + object_name
        #     os.mkdir(caminho_arquivo)
        #     print('O diretório foi criado com sucesso')

        # self.utils.set_data_s3_file(object_name, EnumBuckets.TRUSTED.value)
