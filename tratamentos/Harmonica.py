from interfaces.ITratamento import ITratamentoDados
import pyspark.sql.functions as F
from interfaces.EnumBuckets import EnumBuckets
import os


class Harmonica(ITratamentoDados):

    def __init__(self):
        super().__init__()
        self.nome_sensor = "Harmonica"
        self.tipo_dado = "Porcentagem"
    
    
    def __tratar_dado__(self) -> None:
        nome_arquivo = self.utils.get_data_s3_csv(EnumBuckets.RAW.value)

        print("Arquivo Raw")
        df = self.spark.read.option("multiline", "true").json(nome_arquivo)
        df.printSchema()
        df.show()

        print("Arquivo sem Nulls")
        df = self.utils.remove_null(df)
        df.printSchema()
        df.show()
        
        print("Arquivo pós-remoção de valores não floats")
        df = self.utils.remove_wrong_float(df, "value")
        df.printSchema()
        df.show()

        print("Arquivo pós-formação dos valores")
        df = self.utils.format_number(df, "value")
        df.printSchema()
        df.show()

        print("Arquivo ordenado baseado na data")
        df = self.utils.order_by_coluna_asc(df, "instant")
        df.printSchema()
        df.show()

        tamanho_arquivo = df.count()
        print("Tamanho do arquivo: ", tamanho_arquivo)

        linhas = df.collect()
        linhas_filtradas = []

        for i in range(tamanho_arquivo):
            if (i <= (tamanho_arquivo * 2) // 5):
                if(linhas[i]['scenery'] == "NORMAL"):
                    linhas_filtradas.append(linhas[i])
            elif (i <= tamanho_arquivo // 5):
                if(linhas[i]['scenery'] == "EXCEPCIONAL"):
                    linhas_filtradas.append(linhas[i])
            elif (i <= (tamanho_arquivo * 3) // 10):
                if(linhas[i]['scenery'] == "TERRIVEL"):
                    linhas_filtradas.append(linhas[i])
            elif (i <= tamanho_arquivo):
                if(linhas[i]['scenery'] == "NORMAL"):
                    linhas_filtradas.append(linhas[i])
                    
        print('Dataframe final')
        df_final = self.spark.createDataFrame(linhas_filtradas)
        df_final.show()
        
        trusted_json_file = self.utils.transform_df_to_json(df_final, "all_sensors", "trusted")

        self.utils.set_data_s3_file(object_name=trusted_json_file, bucket_name=EnumBuckets.TRUSTED.value)
        self.__gerar_arquivo_client__()

    def __gerar_arquivo_client__(self) -> None:
        arquivo_trusted = self.utils.get_data_s3_csv(bucket_name=EnumBuckets.TRUSTED.value, sensor="all_sensors")
        df_trusted = self.spark.read.option("multiline", "true").json(arquivo_trusted)                                  
        
        harmonicas_x_tensao = self.__harmonicas_x_tensao__(df_trusted)
        harmonicas_x_tempo = self.__harmonicas_x_tempo__(df_trusted)
        ultimo_valor_harmonica = self.__ultimo_valor_harmonica__(df_trusted)

        self.utils.set_data_s3_file(object_name=harmonicas_x_tensao, bucket_name=os.getenv("BUCKET_NAME_CLIENT"))
        self.utils.set_data_s3_file(object_name=harmonicas_x_tempo, bucket_name=os.getenv("BUCKET_NAME_CLIENT"))
        self.utils.set_data_s3_file(object_name=ultimo_valor_harmonica, bucket_name=os.getenv("BUCKET_NAME_CLIENT"))

    def __harmonicas_x_tensao__(self, df_trusted):
        print('Harmonicas')
        df_harmonicas = self.utils.filter_by_sensor(df_trusted, "valueType", "Porcentagem")
        df_harmonicas = df_harmonicas.selectExpr("instant", "value as value_harmonicas", "valueType as valueType_harmonicas", "scenery")
        df_harmonicas.printSchema()
        df_harmonicas.show()

        print('Tensão')
        df_tensao = self.utils.filter_by_sensor(df_trusted, "valueType", "volts")
        df_tensao = df_tensao.selectExpr("instant", "value as value_tensao", "valueType as valueType_tensao", "scenery")
        df_tensao.printSchema()
        df_tensao.show()
        
        print('Harmônicas x Tensão')
        df_tensao_harmonicas = df_harmonicas.join(df_tensao, ['instant', 'scenery'], how="inner")
        df_tensao.printSchema()
        df_tensao_harmonicas.show()

        df_tensao_harmonicas.show()

        return self.utils.transform_df_to_json(df_tensao_harmonicas, "tensao_x_harmonica", "client")

    def __harmonicas_x_tempo__(self, df_trusted):
        print('Harmonicas')
        df_harmonicas = self.utils.filter_by_sensor(df_trusted, "valueType", "Porcentagem")
        df_harmonicas = df_harmonicas.selectExpr("instant", "value as value_harmonicas", "valueType as valueType_harmonicas")
        df_harmonicas.printSchema()
        df_harmonicas.show()

        df_harmonicas = df_harmonicas \
            .drop('zone')                           \
            .drop('scenery')
        df_harmonicas.show()

        return self.utils.transform_df_to_json(df_harmonicas, "harmonicas_x_tempo", "client")
    
    def __ultimo_valor_harmonica__(self, df_trusted):
        print('Ultima Harmonica')
        df_harmonicas = self.utils.filter_by_sensor(df_trusted, "valueType", "Porcentagem")
        df_harmonicas = df_harmonicas.selectExpr("instant", "value as value_harmonicas", "valueType as valueType_harmonicas")
        df_harmonicas.show()

        df_harmonicas = self.utils.order_by_coluna_asc(df_harmonicas, "instant")
        df_harmonicas.show()

        df_harmonicas = self.utils.get_last_value(df_harmonicas, "value_harmonicas")
        df_harmonicas.printSchema()
        df_harmonicas.show()

        return self.utils.transform_df_to_json(df_harmonicas, "ultimo_valor_harmonica", "client")