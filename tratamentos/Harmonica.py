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

        df = self.spark.read.json(nome_arquivo)
        # df.printSchema()
        # df.show()

        df = self.utils.remove_null(df)
        df = self.utils.filter_by_sensor(df, "valueType", "Porcentagem")
        # df.printSchema()
        # df.show()

        df = self.utils.remove_wrong_float(df, "value")
        # df.printSchema()
        # df.show()

        df = self.utils.format_number(df, "value")
        # df.printSchema()
        # df.show()

        df = self.utils.order_by_coluna_asc(df, "instant")
        # df.printSchema()
        # df.show()

        tamanho_arquivo = df.count()

        linhas = df.drop("IoTHub").collect()
        linhas_filtradas = []

        for i in range(tamanho_arquivo):
            if (i <= (tamanho_arquivo * 2) // 5):
                if(linhas[i]['scenery'] == "NORMAL"):
                    linhas_filtradas.append(linhas[i])
            elif (i <= (tamanho_arquivo * 3) // 5):
                if(linhas[i]['scenery'] == "EXCEPCIONAL"):
                    linhas_filtradas.append(linhas[i])
            elif (i <= (tamanho_arquivo * 9) // 10):
                if(linhas[i]['scenery'] == "TERRIVEL"):
                    linhas_filtradas.append(linhas[i])
            elif (i <= tamanho_arquivo):
                if(linhas[i]['scenery'] == "NORMAL"):
                    linhas_filtradas.append(linhas[i])

        df_final = self.spark.createDataFrame(linhas_filtradas)
        # df_final.show()

        trusted_json_file = self.utils.transform_df_to_json(df_final, "all_sensors", "trusted")

        self.utils.set_data_s3_file(trusted_json_file, EnumBuckets.TRUSTED.value)
        self.__gerar_arquivo_client__()

    def __gerar_arquivo_client__(self) -> None:
        arquivo_trusted = self.utils.get_data_s3_csv(EnumBuckets.TRUSTED.value, "all_sensors")
        df_trusted = self.spark.read.option("multiline", "true").json(arquivo_trusted)

        correlacoes = [
            self.__harmonicas_x_tensao__(df_trusted),
            self.__harmonicas_x_corrente__(df_trusted),
            self.__harmonicas_x_potencia__(df_trusted),
            self.__harmonicas_x_tempo__(df_trusted),
            self.__ultimo_valor_harmonica_por_regiao__(df_trusted)
        ]

        for objeto in correlacoes:
            self.utils.set_data_s3_file(objeto, EnumBuckets.CLIENT.value)


    def __harmonicas_x_tensao__(self, df_trusted):
        df_harmonicas = self.utils.filter_by_sensor(df_trusted, "valueType", "Porcentagem")
        df_harmonicas = df_harmonicas.selectExpr("instant", "value as value_harmonicas", "valueType as valueType_harmonicas", "scenery")
        # df_harmonicas.printSchema()
        # df_harmonicas.show()
        # df_harmonicas = self.spark.read.json(arquivo_harmonicas)
        # df_harmonicas = df_harmonicas.selectExpr("instant", "value as value_harmonicas", "valueType as valueType_harmonicas")
        # df_harmonicas.printSchema()
        # df_harmonicas.show()

        df_tensao = self.utils.filter_by_sensor(df_trusted, "valueType", "volts")
        df_tensao = df_tensao.selectExpr("instant", "value as value_tensao", "valueType as valueType_tensao", "scenery")
        # df_tensao.printSchema()
        # df_tensao.show()

        df_tensao_harmonicas = df_harmonicas.join(df_tensao, ['instant', 'scenery'], how="inner")
        # df_tensao.printSchema()
        # df_tensao_harmonicas.show()

        return self.utils.transform_df_to_json(df_tensao_harmonicas, "tensao_x_harmonica", "client")
    
    def __harmonicas_x_corrente__(self, df_trusted):
        df_harmonicas = self.utils.filter_by_sensor(df_trusted, "valueType", "Porcentagem")
        df_harmonicas = df_harmonicas.selectExpr("instant", "value as value_harmonicas", "valueType as valueType_harmonicas", "scenery")
        # df_harmonicas.printSchema()
        # df_harmonicas.show()
        #
        df_corrente = self.utils.filter_by_sensor(df_trusted, "valueType", "ampére")
        df_corrente = df_corrente.selectExpr("instant", "value as value_corrente", "valueType as valueType_corrente", "scenery")
        # df_corrente.printSchema()
        # df_corrente.show()

        df_corrente_harmonicas = df_harmonicas.join(df_corrente, ['instant', 'scenery'], how="inner")
        # df_corrente.printSchema()
        # df_corrente_harmonicas.show()

        return self.utils.transform_df_to_json(df_corrente_harmonicas, "corrente_x_harmonica", "client")

    def __harmonicas_x_potencia__(self, df_trusted):
        df_harmonicas = self.utils.filter_by_sensor(df_trusted, "valueType", "Porcentagem")
        df_harmonicas = df_harmonicas.selectExpr("instant", "value as value_harmonicas", "valueType as valueType_harmonicas", "scenery")
        # df_harmonicas.printSchema()
        # df_harmonicas.show()

        df_potencia = self.utils.filter_by_sensor(df_trusted, "valueType", "fator")
        df_potencia = df_potencia.selectExpr("instant", "value as value_potencia", "valueType as valueType_potencia", "scenery")
        # df_potencia.printSchema()
        # df_potencia.show()
        #
        df_potencia_harmonicas = df_harmonicas.join(df_potencia, ['instant', 'scenery'], how="inner")
        # df_potencia_harmonicas.printSchema()
        # df_potencia_harmonicas.show()

        return self.utils.transform_df_to_json(df_potencia_harmonicas, "potencia_x_harmonica", "client")

    def __harmonicas_x_tempo__(self, df_trusted):
        df_harmonicas = self.utils.filter_by_sensor(df_trusted, "valueType", "Porcentagem")
        df_harmonicas = df_harmonicas.selectExpr("instant", "value as value_harmonicas", "valueType as valueType_harmonicas")
        # df_harmonicas.printSchema()
        # df_harmonicas.show()

        df_harmonicas = df_harmonicas \
            .drop('zone')                           \
            .drop('scenery')
        # df_harmonicas.show()

        return self.utils.transform_df_to_csv(df_harmonicas, "harmonicas_x_tempo", "client")
    
    def __ultimo_valor_harmonica_por_regiao__(self, df_trusted):
        df_harmonicas = self.utils.filter_by_sensor(df_trusted, "valueType", "Porcentagem")
        df_harmonicas = df_harmonicas.selectExpr("instant", "value as value_harmonicas", "valueType as valueType_harmonicas", "zone")
        # df_harmonicas.show()

        df_harmonicas = self.utils.order_by_coluna_asc(df_harmonicas, "instant")
        # df_harmonicas.show()

        ZONES = ["NORTE", "SUL", "LESTE", "OESTE", "CENTRO"]
        result = []

        for zone in ZONES:
            temp = self.utils.filter_by_sensor(df_harmonicas, "zone", zone)
            result.append(temp.tail(1)[0])

        df_final = self.spark.createDataFrame(result)
        # df_final.show()

        df_final = df_final.selectExpr("zone", "value_harmonicas")
        # df_final.show()

        return self.utils.transform_df_to_json(df_final, "ultimo_valor_harmonica_por_regiao", "client")
