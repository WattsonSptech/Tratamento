from interfaces.ITratamento import ITratamentoDados
import pandas as pd
from interfaces.EnumBuckets import EnumBuckets
import unicodedata
import re
from datetime import datetime

class ReclameAqui(ITratamentoDados):
    
    def __init__(self):
        super().__init__()
        
    
    def __tratar_dado__(self):
        df = pd.read_csv(self.utils.get_data_s3_csv(EnumBuckets.RAW.value, "ReclameAqui_Raw_"), sep=";")
        df = self.__gerar_colunas_data_e_hora__(df, df['Data-hora'])

        df = df.rename(
            columns={
                'Data-hora': 'DATA_HORA_RECLAMACAO',
                'DATA': 'DATA_RECLAMACAO',
                'HORA': 'HORA_MINUTO_RECLAMACAO',
                'Avaliação': 'AVALIACAO'
            }
        )

        df['DATA'] = pd.to_datetime(df['DATA'])
        df['DATA_HORA_RECLAMACAO'] = pd.to_datetime(df['DATA_HORA_RECLAMACAO'])

        df = df.drop(['Reclamação'], axis=1)

        self.__subir_dados_s3__(df)

    def __remover_acentos__(self, series):
        series_normalizada = []

        for texto in series:
            texto_normalizado = unicodedata.normalize('NFD', texto)
            texto_sem_acentos = re.sub(r'[\u0300-\u036f]', '', texto_normalizado)
            series_normalizada.append(texto_sem_acentos.upper())
        return series_normalizada

    def __gerar_colunas_data_e_hora__(self, df, coluna_data_hora):
        data_hora = data = hora = []

        for item in coluna_data_hora:
            data_separada = str(item).split('T')

            data_reclamacao = data_separada[0]
            hora_reclamacao = data_separada[1][:8]
            data_hora_reclamacao = data_separada[0] + " " + hora_reclamacao

            data.append(data_reclamacao)
            hora.append(hora_reclamacao)
            data_hora.append(data_hora_reclamacao)

        df['Data-hora'] = data_hora
        df['DATA'] = data
        df['HORA'] = hora

        return df
    
    def __converter_para_timestamp__(self, data, horario):
        series_normalizada = []
        
        if len(data) == len(horario):
        
            horas = [hora + ":00.00000" for hora in horario]
            datas = [item for item in data]

            for i in range (len(horas)):
                series_normalizada.append(f"{datas[i]} {horas[i]}")
                
        return series_normalizada

    def __subir_dados_s3__(self, dados: pd.DataFrame):
        
        ano = datetime.today().year
        mes = datetime.today().month
        dia = datetime.today().day

        filepath = "./temp/ReclameAqui_TRUSTED_{}{}{}.csv".format(ano, mes, dia)
        dados.to_csv(filepath, sep=";")
        self.utils.set_data_s3_file(filepath, EnumBuckets.TRUSTED.value)